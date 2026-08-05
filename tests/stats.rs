use tagdata::{DB, Error};

mod common;

#[test]
fn stats_report_storage_commits_and_readers() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;

    let empty = db.stats()?;
    assert_eq!(empty.page_size, db.pagesize());
    assert_eq!(empty.allocated_pages, 4);
    assert_eq!(empty.committed_transactions, 0);
    assert_eq!(empty.bytes_written, 0);
    assert_eq!(empty.active_readers, 0);

    let tx = db.tx(true)?;
    tx.create_bucket("data")?.put("key", "value")?;
    tx.commit()?;

    let committed = db.stats()?;
    assert_eq!(committed.current_tx_id, 1);
    assert_eq!(committed.committed_transactions, 1);
    assert!(committed.bytes_written > committed.page_size);
    assert!(committed.file_bytes >= committed.allocated_pages * committed.page_size);

    let tx = db.tx(false)?;
    let reading = db.stats()?;
    assert_eq!(reading.active_readers, 1);
    assert_eq!(reading.oldest_reader_tx_id, Some(reading.current_tx_id));
    drop(tx);

    let idle = db.stats()?;
    assert_eq!(idle.active_readers, 0);
    assert_eq!(idle.oldest_reader_tx_id, None);
    Ok(())
}

#[test]
fn stats_identify_pages_pinned_by_a_reader() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;

    let tx = db.tx(true)?;
    tx.create_bucket("data")?.put("key", vec![1; 4096])?;
    tx.commit()?;

    let reader = db.tx(false)?;
    let writer_db = db.clone();
    std::thread::spawn(move || -> Result<(), Error> {
        let tx = writer_db.tx(true)?;
        tx.get_bucket("data")?.put("key", vec![2; 4096])?;
        tx.commit()
    })
    .join()
    .unwrap()?;

    let pinned = db.stats()?;
    assert_eq!(pinned.active_readers, 1);
    assert!(pinned.pending_pages > 0);
    assert_eq!(pinned.reader_pinned_pages, pinned.pending_pages);

    drop(reader);
    assert_eq!(db.stats()?.reader_pinned_pages, 0);
    Ok(())
}
