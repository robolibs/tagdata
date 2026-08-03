use inspace::{DB, Error, FormatInfo, LATEST_FORMAT_VERSION, OpenOptions};

mod common;

#[test]
fn version_three_persists_retirement_generations_across_handles() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = OpenOptions::new()
        .format_version(LATEST_FORMAT_VERSION)
        .pagesize(4096)
        .num_pages(4)
        .growth_increment(4096)
        .open(&file)?;
    db.update(|tx| {
        tx.create_bucket("items")?
            .put("key", vec![1_u8; 32 * 1024])?;
        Ok(())
    })?;
    assert_eq!(FormatInfo::inspect(&file)?.version, LATEST_FORMAT_VERSION);

    let reader = db.read_tx()?;
    let writer = DB::open(&file)?;
    writer.update(|tx| {
        tx.get_bucket("items")?.put("key", vec![2_u8; 32 * 1024])?;
        Ok(())
    })?;
    assert_eq!(
        reader.get_bucket("items")?.get_kv("key").unwrap().value()[0],
        1
    );
    drop(writer);

    let reopened = DB::open(&file)?;
    let stats = reopened.stats()?;
    assert!(stats.pending_pages > 0);
    assert_eq!(stats.oldest_reader_tx_id, Some(1));
    reopened.verify()?;

    drop(reader);
    reopened.update(|tx| {
        tx.get_bucket("items")?.put("after-reader", "ok")?;
        Ok(())
    })?;
    reopened.verify()
}

#[test]
fn the_default_format_remains_stable_until_v3_benchmarks_are_accepted() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let _db = DB::open(&file)?;
    assert!(FormatInfo::inspect(&file)?.version < LATEST_FORMAT_VERSION);
    Ok(())
}
