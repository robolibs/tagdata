use std::time::{Duration, UNIX_EPOCH};

use tagdata::{DB, Data, Error, MergeConflictPolicy, MergeOptions, MergeReport};

mod common;

#[test]
fn database_merge_recurses_and_resolves_all_entry_shapes() -> Result<(), Error> {
    let source_file = common::RandomFile::new();
    let destination_file = common::RandomFile::new();
    let source = DB::open(&source_file)?;
    let destination = DB::open(&destination_file)?;

    source.update(|tx| {
        let alpha = tx.create_bucket("alpha")?;
        alpha.put("same", "same")?;
        alpha.put("conflict", "source")?;
        alpha.put("fresh", "source")?;
        alpha.put("leaf", "source-leaf")?;
        alpha
            .create_bucket("common")?
            .put("source-only", "source")?;
        alpha.create_bucket("node")?.put("inside", "source")?;
        tx.create_bucket("beta")?.put("key", "value")?;
        Ok(())
    })?;
    destination.update(|tx| {
        let alpha = tx.create_bucket("alpha")?;
        alpha.put("same", "same")?;
        alpha.put("conflict", "destination")?;
        alpha.put("destination-only", "destination")?;
        alpha.put("node", "destination-node")?;
        alpha
            .create_bucket("common")?
            .put("destination-only", "destination")?;
        alpha.create_bucket("leaf")?.put("inside", "destination")?;
        Ok(())
    })?;

    let report = destination.merge_from(&source, MergeOptions::new())?;
    assert_eq!(
        report,
        MergeReport {
            keys_inserted: 5,
            keys_updated: 1,
            keys_unchanged: 1,
            entries_skipped: 0,
            buckets_created: 2,
            buckets_merged: 2,
            type_conflicts_resolved: 2,
        }
    );

    destination.view(|tx| {
        let alpha = tx.get_bucket("alpha")?;
        assert_eq!(alpha.get_kv("conflict").unwrap().value(), b"source");
        assert_eq!(alpha.get_kv("fresh").unwrap().value(), b"source");
        assert_eq!(
            alpha.get_kv("destination-only").unwrap().value(),
            b"destination"
        );
        assert_eq!(alpha.get_kv("leaf").unwrap().value(), b"source-leaf");
        assert_eq!(
            alpha.get_bucket("node")?.get_kv("inside").unwrap().value(),
            b"source"
        );
        let common = alpha.get_bucket("common")?;
        assert_eq!(common.get_kv("source-only").unwrap().value(), b"source");
        assert_eq!(
            common.get_kv("destination-only").unwrap().value(),
            b"destination"
        );
        assert_eq!(
            tx.get_bucket("beta")?.get_kv("key").unwrap().value(),
            b"value"
        );
        Ok(())
    })?;
    destination.verify()
}

#[test]
fn keep_existing_preserves_values_and_entry_types() -> Result<(), Error> {
    let source_file = common::RandomFile::new();
    let destination_file = common::RandomFile::new();
    let source = DB::open(&source_file)?;
    let destination = DB::open(&destination_file)?;

    source.update(|tx| {
        let root = tx.create_bucket("root")?;
        root.put_with_ttl("value", "source", UNIX_EPOCH + Duration::from_secs(100))?;
        root.put_with_ttl("same-ttl", "same", UNIX_EPOCH + Duration::from_secs(100))?;
        root.put("bucket-to-value", "source")?;
        root.create_bucket("value-to-bucket")?
            .put("inside", "source")?;
        let common = root.create_bucket("common")?;
        common.put("conflict", "source")?;
        common.put("new", "source")?;
        Ok(())
    })?;
    destination.update(|tx| {
        let root = tx.create_bucket("root")?;
        root.put_with_ttl(
            "value",
            "destination",
            UNIX_EPOCH + Duration::from_secs(200),
        )?;
        root.put_with_ttl("same-ttl", "same", UNIX_EPOCH + Duration::from_secs(200))?;
        root.put("value-to-bucket", "destination")?;
        root.create_bucket("bucket-to-value")?
            .put("inside", "destination")?;
        root.create_bucket("common")?
            .put("conflict", "destination")?;
        Ok(())
    })?;

    let options = MergeOptions::new().conflict_policy(MergeConflictPolicy::KeepExisting);
    let report = destination.merge_from(&source, options)?;
    assert_eq!(report.keys_inserted, 1);
    assert_eq!(report.entries_skipped, 5);
    assert_eq!(report.buckets_merged, 2);
    assert_eq!(report.type_conflicts_resolved, 0);

    destination.view(|tx| {
        let root = tx.get_bucket("root")?;
        assert_eq!(root.get_kv("value").unwrap().value(), b"destination");
        assert!(
            root.get_live_at("same-ttl", UNIX_EPOCH + Duration::from_secs(150))?
                .is_some()
        );
        assert!(matches!(root.get("bucket-to-value"), Some(Data::Bucket(_))));
        assert!(matches!(
            root.get("value-to-bucket"),
            Some(Data::KeyValue(_))
        ));
        let common = root.get_bucket("common")?;
        assert_eq!(common.get_kv("conflict").unwrap().value(), b"destination");
        assert_eq!(common.get_kv("new").unwrap().value(), b"source");
        Ok(())
    })
}

#[test]
fn error_policy_reports_path_and_rolls_back() -> Result<(), Error> {
    let source_file = common::RandomFile::new();
    let destination_file = common::RandomFile::new();
    let source = DB::open(&source_file)?;
    let destination = DB::open(&destination_file)?;

    source.update(|tx| {
        let root = tx.create_bucket("root")?;
        root.put("a-new", "source")?;
        root.put("b-conflict", "source")?;
        Ok(())
    })?;
    destination.update(|tx| {
        tx.create_bucket("root")?.put("b-conflict", "destination")?;
        Ok(())
    })?;

    let options = MergeOptions::new().conflict_policy(MergeConflictPolicy::Error);
    assert_eq!(
        destination.merge_from(&source, options),
        Err(Error::MergeConflict {
            path: vec![b"root".to_vec(), b"b-conflict".to_vec()]
        })
    );
    destination.view(|tx| {
        let root = tx.get_bucket("root")?;
        assert!(root.get("a-new").is_none());
        assert_eq!(root.get_kv("b-conflict").unwrap().value(), b"destination");
        Ok(())
    })
}

#[test]
fn bucket_merge_uses_the_callers_transaction_and_preserves_sequence() -> Result<(), Error> {
    let source_file = common::RandomFile::new();
    let destination_file = common::RandomFile::new();
    let source = DB::open(&source_file)?;
    let destination = DB::open(&destination_file)?;

    source.update(|tx| {
        let bucket = tx.create_bucket("source")?;
        for key in 0_u64..5 {
            bucket.put(key.to_be_bytes(), key.to_be_bytes())?;
        }
        let _ = bucket.delete(1_u64.to_be_bytes())?;
        let _ = bucket.delete(3_u64.to_be_bytes())?;
        Ok(())
    })?;

    let source_tx = source.read_tx()?;
    let source_bucket = source_tx.get_bucket("source")?;
    let destination_tx = destination.write_tx()?;
    let destination_bucket = destination_tx.create_bucket("destination")?;
    let report = destination_bucket.merge_from(&source_bucket, MergeOptions::new())?;
    assert_eq!(report.keys_inserted, 3);
    assert_eq!(destination_bucket.next_int(), 5);
    destination_tx.commit()?;

    destination.view(|tx| {
        let bucket = tx.get_bucket("destination")?;
        assert_eq!(bucket.next_int(), 5);
        assert_eq!(bucket.kv_pairs().count(), 3);
        Ok(())
    })
}

#[test]
fn bucket_merge_rejects_read_only_destinations() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.update(|tx| {
        tx.create_bucket("source")?.put("key", "value")?;
        tx.create_bucket("destination")?;
        Ok(())
    })?;

    let tx = db.read_tx()?;
    let source = tx.get_bucket("source")?;
    let destination = tx.get_bucket("destination")?;
    assert_eq!(
        destination.merge_from(&source, MergeOptions::new()),
        Err(Error::ReadOnlyTx)
    );
    Ok(())
}

#[test]
fn database_merge_with_a_cloned_handle_is_a_no_op() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.update(|tx| {
        tx.create_bucket("root")?.put("key", "value")?;
        Ok(())
    })?;

    let report = db.merge_from(&db.clone(), MergeOptions::new())?;
    assert_eq!(report, MergeReport::default());
    db.verify()
}

#[test]
fn overwrite_merge_copies_and_clears_ttl_with_winning_values() -> Result<(), Error> {
    let source_file = common::RandomFile::new();
    let destination_file = common::RandomFile::new();
    let source = DB::open(&source_file)?;
    let destination = DB::open(&destination_file)?;
    let source_deadline = UNIX_EPOCH + Duration::from_secs(100);
    let destination_deadline = UNIX_EPOCH + Duration::from_secs(200);

    source.update(|tx| {
        let root = tx.create_bucket("root")?;
        root.put_with_ttl("source-ttl", "source", source_deadline)?;
        root.put("source-clears-ttl", "source")?;
        Ok(())
    })?;
    destination.update(|tx| {
        let root = tx.create_bucket("root")?;
        root.put("source-ttl", "destination")?;
        root.put_with_ttl("source-clears-ttl", "destination", destination_deadline)?;
        Ok(())
    })?;

    destination.merge_from(&source, MergeOptions::new())?;
    destination.view(|tx| {
        let root = tx.get_bucket("root")?;
        assert!(
            root.get_live_at("source-ttl", UNIX_EPOCH + Duration::from_secs(50))?
                .is_some()
        );
        assert!(
            root.get_live_at("source-ttl", UNIX_EPOCH + Duration::from_secs(150))?
                .is_none()
        );
        assert!(
            root.get_live_at("source-clears-ttl", UNIX_EPOCH + Duration::from_secs(250))?
                .is_some()
        );
        Ok(())
    })
}

#[cfg(feature = "changefeed")]
#[test]
fn database_merge_does_not_import_source_journal_history() -> Result<(), Error> {
    use tagdata::JournalConfig;

    let source_file = common::RandomFile::new();
    let destination_file = common::RandomFile::new();
    let source = DB::open(&source_file)?;
    let destination = DB::open(&destination_file)?;
    source.enable_journal(JournalConfig::default())?;
    source.update(|tx| {
        tx.create_bucket("root")?.put("first", "source")?;
        Ok(())
    })?;
    source.update(|tx| {
        tx.get_bucket("root")?.put("second", "source")?;
        Ok(())
    })?;
    destination.enable_journal(JournalConfig::default())?;
    let before = destination.replay_journal(0, 100, None)?.transactions.len();

    destination.merge_from(&source, MergeOptions::new())?;
    let replay = destination.replay_journal(0, 100, None)?;
    assert_eq!(replay.transactions.len(), before + 1);
    let merged = replay.transactions.last().unwrap();
    assert!(merged.changes.iter().any(|change| change.key == b"first"));
    assert!(merged.changes.iter().any(|change| change.key == b"second"));
    Ok(())
}
