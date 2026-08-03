use inspace::{ChangeOperation, DB, Error, JournalConfig, WatchFilter};

mod common;

#[test]
fn journal_replays_atomic_boundaries_across_reopen_and_filters() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.enable_journal(JournalConfig {
        max_transactions: 20,
    })?;
    db.update(|tx| {
        let users = tx.create_bucket("users")?;
        users.put("active/1", "Ada")?;
        users.put("inactive/2", "Grace")?;
        Ok(())
    })?;
    let rollback = db.update(|tx| {
        tx.get_bucket("users")?.put("rolled-back", "no")?;
        Err::<(), _>(Error::KeyValueMissing)
    });
    assert_eq!(rollback, Err(Error::KeyValueMissing));
    db.update(|tx| {
        tx.get_bucket("users")?.delete("inactive/2")?;
        Ok(())
    })?;
    drop(db);

    let db = DB::open(&file)?;
    let replay = db.replay_journal(1, 10, None)?;
    assert_eq!(replay.transactions.len(), 2);
    assert_eq!(replay.transactions[0].changes.len(), 3);
    assert_eq!(
        replay.transactions[1].changes[0].operation,
        ChangeOperation::Delete
    );
    assert!(replay.transactions.iter().all(|set| {
        set.changes
            .iter()
            .all(|change| change.key != b"rolled-back")
    }));

    let filter = WatchFilter::new().collection("users").prefix("active/");
    let filtered = db.replay_journal(1, 10, Some(&filter))?;
    assert_eq!(filtered.transactions.len(), 2);
    assert_eq!(filtered.transactions[0].changes.len(), 1);
    assert!(filtered.transactions[1].changes.is_empty());
    Ok(())
}

#[test]
fn journal_retention_checkpoints_and_gap_detection_are_explicit() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.enable_journal(JournalConfig {
        max_transactions: 2,
    })?;
    for value in 0..3_u8 {
        db.update(|tx| {
            let bucket = tx.get_or_create_bucket("items")?;
            bucket.put("key", [value])?;
            Ok(())
        })?;
    }

    let replay = db.replay_journal(1, 10, None)?;
    assert_eq!(replay.transactions.len(), 2);
    assert_eq!(replay.oldest_available, Some(3));
    assert_eq!(replay.newest_available, Some(4));
    assert_eq!(replay.gap.unwrap().oldest_available, 3);

    db.checkpoint_journal("indexer", 4)?;
    assert_eq!(db.journal_checkpoint("indexer")?, Some(4));
    assert!(db.replay_journal(0, 1, None)?.gap.is_some());
    Ok(())
}

#[test]
fn oversized_transactions_replay_with_a_truncation_marker() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.enable_journal(JournalConfig::default())?;
    db.update(|tx| {
        let bucket = tx.create_bucket("items")?;
        for key in 0..4_200_u64 {
            bucket.put(key.to_be_bytes(), [1])?;
        }
        Ok(())
    })?;
    let replay = db.replay_journal(1, 1, None)?;
    assert_eq!(replay.transactions.len(), 1);
    assert!(replay.transactions[0].truncated);
    assert_eq!(replay.transactions[0].changes.len(), 4_096);
    Ok(())
}
