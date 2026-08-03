use std::panic::{AssertUnwindSafe, catch_unwind};

use inspace::{DB, Error};

mod common;

#[test]
fn named_and_scoped_transactions_commit_only_success() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;

    db.update(|tx| {
        tx.create_bucket("items")?.put("committed", "yes")?;
        Ok(())
    })?;
    let failure = db.update(|tx| {
        tx.get_bucket("items")?.put("rolled-back", "error")?;
        Err::<(), _>(Error::KeyValueMissing)
    });
    assert_eq!(failure, Err(Error::KeyValueMissing));

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = db.update(|tx| -> Result<(), Error> {
            tx.get_bucket("items")?.put("rolled-back", "panic")?;
            panic!("stop");
        });
    }));
    assert!(panic.is_err());

    db.view(|tx| {
        let bucket = tx.get_bucket("items")?;
        assert_eq!(bucket.get_kv("committed").unwrap().value(), b"yes");
        assert!(bucket.get_kv("rolled-back").is_none());
        Ok(())
    })
}

#[test]
fn try_write_tx_never_waits_for_an_owned_writer_slot() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    let writer = db.write_tx()?;
    assert!(db.try_write_tx()?.is_none());

    drop(writer);
    assert!(db.try_write_tx()?.is_some());
    Ok(())
}

#[test]
fn atomic_bucket_operations_report_success_and_conflicts() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.update(|tx| {
        let bucket = tx.create_bucket("items")?;

        let inserted = bucket.put_if_absent("key", "one")?;
        assert!(inserted.applied);
        assert_eq!(inserted.observed, None);
        assert_eq!(inserted.current.as_deref(), Some(b"one".as_slice()));

        let conflict = bucket.put_if_absent("key", "ignored")?;
        assert!(!conflict.applied);
        assert_eq!(conflict.observed.as_deref(), Some(b"one".as_slice()));
        assert_eq!(conflict.current.as_deref(), Some(b"one".as_slice()));

        let conflict = bucket.compare_exchange("key", Some(b"wrong"), "two")?;
        assert!(!conflict.applied);
        assert_eq!(conflict.current.as_deref(), Some(b"one".as_slice()));

        let exchanged = bucket.compare_exchange("key", Some(b"one"), "two")?;
        assert!(exchanged.applied);
        assert_eq!(exchanged.observed.as_deref(), Some(b"one".as_slice()));
        assert_eq!(exchanged.current.as_deref(), Some(b"two".as_slice()));

        let missing = bucket.compare_exchange("new", None, "created")?;
        assert!(missing.applied);

        let conflict = bucket.delete_if_value("key", b"wrong")?;
        assert!(!conflict.applied);
        assert_eq!(conflict.current.as_deref(), Some(b"two".as_slice()));

        let deleted = bucket.delete_if_value("key", b"two")?;
        assert!(deleted.applied);
        assert_eq!(deleted.observed.as_deref(), Some(b"two".as_slice()));
        assert_eq!(deleted.current, None);

        let missing = bucket.delete_if_value("absent", b"anything")?;
        assert!(!missing.applied);
        assert_eq!(missing.observed, None);
        Ok(())
    })?;

    db.view(|tx| {
        let bucket = tx.get_bucket("items")?;
        assert!(bucket.get_kv("key").is_none());
        assert_eq!(bucket.get_kv("new").unwrap().value(), b"created");
        Ok(())
    })
}

#[test]
fn atomic_operations_reject_read_only_transactions_and_bucket_values() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.update(|tx| {
        let bucket = tx.create_bucket("items")?;
        bucket.create_bucket("nested")?;
        assert_eq!(
            bucket.put_if_absent("nested", "value"),
            Err(Error::IncompatibleValue)
        );
        Ok(())
    })?;

    db.view(|tx| {
        let bucket = tx.get_bucket("items")?;
        assert_eq!(bucket.put_if_absent("key", "value"), Err(Error::ReadOnlyTx));
        Ok(())
    })
}
