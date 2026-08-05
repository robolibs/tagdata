#![cfg(feature = "changefeed")]

use std::{
    sync::mpsc::TryRecvError,
    time::{Duration, UNIX_EPOCH},
};

use tagdata::{ChangeOperation, DB, Error, WatchError, WatchFilter};

mod common;

#[test]
fn watches_deliver_ordered_committed_change_sets() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    let watch = db.watch(4)?;

    db.update(|tx| {
        let root = tx.create_bucket("root")?;
        root.put("first", "1")?;
        let nested = root.create_bucket("nested")?;
        nested.put("second", "2")?;
        Ok(())
    })?;

    let set = watch.recv().unwrap();
    assert!(set.transaction_id > 0);
    assert!(!set.truncated);
    assert_eq!(set.changes.len(), 4);
    assert_eq!(set.changes[0].operation, ChangeOperation::BucketCreate);
    assert_eq!(set.changes[1].bucket_path, vec![b"root".to_vec()]);
    assert_eq!(set.changes[1].key, b"first");
    assert_eq!(set.changes[2].operation, ChangeOperation::BucketCreate);
    assert_eq!(
        set.changes[3].bucket_path,
        vec![b"root".to_vec(), b"nested".to_vec()]
    );
    assert_eq!(set.changes[3].key, b"second");
    Ok(())
}

#[test]
fn filtered_watches_keep_transaction_boundaries_and_select_changes() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.update(|tx| {
        tx.create_bucket("users")?;
        tx.create_bucket("jobs")?;
        Ok(())
    })?;
    let watch = db.watch_filtered(
        2,
        WatchFilter::new()
            .collection("users")
            .prefix("active/")
            .operations([ChangeOperation::Put]),
    )?;
    db.update(|tx| {
        tx.get_bucket("users")?.put("active/1", "Ada")?;
        tx.get_bucket("users")?.put("inactive/2", "Grace")?;
        tx.get_bucket("jobs")?.put("active/3", "Linus")?;
        Ok(())
    })?;

    let set = watch.recv().unwrap();
    assert_eq!(set.changes.len(), 1);
    assert_eq!(set.changes[0].bucket_path, vec![b"users".to_vec()]);
    assert_eq!(set.changes[0].key, b"active/1");
    Ok(())
}

#[test]
fn watch_ranges_and_overflow_have_structured_semantics() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.update(|tx| {
        tx.create_bucket("items")?;
        Ok(())
    })?;
    let watch = db.watch_filtered(
        1,
        WatchFilter::from_now().collection("items").range("b", "d"),
    )?;
    db.update(|tx| {
        let items = tx.get_bucket("items")?;
        items.put("a", "outside")?;
        items.put("b", "inside")?;
        Ok(())
    })?;
    db.update(|tx| {
        tx.get_bucket("items")?.put("c", "overflow")?;
        Ok(())
    })?;

    let first = watch.recv_event().unwrap();
    assert_eq!(first.changes.len(), 1);
    assert_eq!(first.changes[0].key, b"b");
    assert_eq!(watch.try_recv_event(), Err(WatchError::Overflow));
    Ok(())
}

#[test]
fn rolled_back_changes_are_not_delivered_and_slow_consumers_disconnect() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.update(|tx| {
        tx.create_bucket("items")?;
        Ok(())
    })?;
    let watch = db.watch(1)?;

    let result = db.update(|tx| {
        tx.get_bucket("items")?.put("rollback", "value")?;
        Err::<(), _>(Error::KeyValueMissing)
    });
    assert_eq!(result, Err(Error::KeyValueMissing));
    assert_eq!(watch.try_recv(), Err(TryRecvError::Empty));

    db.update(|tx| {
        tx.get_bucket("items")?.put("one", "1")?;
        Ok(())
    })?;
    db.update(|tx| {
        tx.get_bucket("items")?.put("two", "2")?;
        Ok(())
    })?;
    assert_eq!(watch.recv().unwrap().changes[0].key, b"one");
    assert_eq!(watch.try_recv(), Err(TryRecvError::Disconnected));
    Ok(())
}

#[test]
fn ttl_visibility_cleanup_and_compaction_are_consistent() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let compact = common::RandomFile::new();
    let db = DB::open(&file)?;
    let before = UNIX_EPOCH + Duration::from_secs(1_000);
    let expiry = before + Duration::from_secs(10);
    let after = expiry + Duration::from_millis(1);

    db.update(|tx| {
        let bucket = tx.create_bucket("sessions")?;
        bucket.put_with_ttl("token", "alive", expiry)?;
        assert_eq!(
            bucket.get_live_at("token", before)?.unwrap().value(),
            b"alive"
        );
        assert!(bucket.get_live_at("token", after)?.is_none());
        Ok(())
    })?;

    db.compact_to(&compact.path)?;
    let copied = DB::open(&compact)?;
    copied.view(|tx| {
        assert!(
            tx.get_bucket("sessions")?
                .get_live_at("token", after)?
                .is_none()
        );
        Ok(())
    })?;

    let watch = db.watch(2)?;
    db.update(|tx| {
        let bucket = tx.get_bucket("sessions")?;
        assert_eq!(bucket.purge_expired(after, 10)?, 1);
        assert!(bucket.get_kv("token").is_none());
        Ok(())
    })?;
    let changes = watch.recv().unwrap();
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].operation, ChangeOperation::Expire);
    assert_eq!(changes.changes[0].key, b"token");
    Ok(())
}

#[test]
fn ttl_can_be_cleared_and_cleanup_is_bounded() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    let expiry = UNIX_EPOCH + Duration::from_secs(10);
    let after = expiry + Duration::from_secs(1);
    db.update(|tx| {
        let bucket = tx.create_bucket("items")?;
        bucket.put_with_ttl("keep", "value", expiry)?;
        bucket.put_with_ttl("first", "value", expiry)?;
        bucket.put_with_ttl("second", "value", expiry)?;
        assert!(bucket.clear_ttl("keep")?);
        assert_eq!(bucket.purge_expired(after, 1)?, 1);
        assert!(bucket.get_live_at("keep", after)?.is_some());
        let remaining = usize::from(bucket.get_kv("first").is_some())
            + usize::from(bucket.get_kv("second").is_some());
        assert_eq!(remaining, 1);
        Ok(())
    })
}

#[test]
fn deadline_index_orders_cleanup_and_database_cleanup_walks_nested_buckets() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    let expired = UNIX_EPOCH + Duration::from_secs(10);
    let future = UNIX_EPOCH + Duration::from_secs(100);
    let now = UNIX_EPOCH + Duration::from_secs(20);
    db.update(|tx| {
        let root = tx.create_bucket("root")?;
        root.put_with_ttl("a-future", "keep", future)?;
        root.put_with_ttl("z-expired", "remove", expired)?;
        let nested = root.create_bucket("nested")?;
        nested.put_with_ttl("expired", "remove", expired)?;
        Ok(())
    })?;

    assert_eq!(db.purge_expired(now, 1)?, 1);
    assert_eq!(db.purge_expired(now, 10)?, 1);
    db.view(|tx| {
        let root = tx.get_bucket("root")?;
        assert!(root.get_live_at("a-future", now)?.is_some());
        assert!(root.get_kv("z-expired").is_none());
        assert!(root.get_bucket("nested")?.get_kv("expired").is_none());
        Ok(())
    })
}

#[test]
fn oversized_change_sets_are_bounded_and_marked_truncated() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.update(|tx| {
        tx.create_bucket("items")?;
        Ok(())
    })?;
    let watch = db.watch(1)?;
    db.update(|tx| {
        let bucket = tx.get_bucket("items")?;
        for key in 0..4200_u64 {
            bucket.put(key.to_be_bytes(), [1])?;
        }
        Ok(())
    })?;
    let set = watch.recv().unwrap();
    assert!(set.truncated);
    assert_eq!(set.changes.len(), 4096);
    Ok(())
}
