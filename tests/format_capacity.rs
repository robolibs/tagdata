use std::{
    fs::OpenOptions as FileOpenOptions,
    io::{Seek, SeekFrom, Write},
};

use inspace::{DB, Error, FormatInfo, OpenOptions};

mod common;

#[test]
fn existing_format_is_detected_without_host_page_size_assumptions() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = OpenOptions::new()
        .pagesize(5000)
        .num_pages(40)
        .open(&file)?;
    db.update(|tx| {
        tx.create_bucket("items")?.put("key", "value")?;
        Ok(())
    })?;
    drop(db);

    let info = FormatInfo::inspect(&file.path)?;
    assert_eq!(info.page_size, 5000);
    assert_eq!(info.version, 2);
    assert!(info.transaction_id > 0);
    assert_eq!(DB::open(&file)?.pagesize(), 5000);
    assert!(matches!(
        OpenOptions::new().pagesize(4096).open(&file),
        Err(Error::InvalidDB(message)) if message.contains("conflicts")
    ));
    Ok(())
}

#[test]
fn bootstrap_uses_the_surviving_metadata_page_and_fails_closed() -> Result<(), Error> {
    let file = common::RandomFile::new();
    drop(OpenOptions::new().pagesize(4096).open(&file)?);
    let mut raw = FileOpenOptions::new().write(true).open(&file.path)?;
    raw.seek(SeekFrom::Start(0))?;
    raw.write_all(&[0; 128])?;
    raw.sync_all()?;

    assert_eq!(FormatInfo::inspect(&file.path)?.page_size, 4096);
    assert_eq!(DB::open(&file)?.pagesize(), 4096);

    raw.seek(SeekFrom::Start(4096))?;
    raw.write_all(&[0; 128])?;
    raw.sync_all()?;
    assert!(matches!(
        FormatInfo::inspect(&file.path),
        Err(Error::InvalidDB(_))
    ));
    Ok(())
}

#[test]
fn capacity_rejection_never_publishes_the_transaction() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = OpenOptions::new()
        .pagesize(1024)
        .num_pages(4)
        .growth_increment(2048)
        .max_file_bytes(4096)
        .open(&file)?;
    let tx = db.write_tx()?;
    tx.create_bucket("large")?
        .put("key", vec![7_u8; 16 * 1024])?;
    assert!(matches!(tx.commit(), Err(Error::CapacityExceeded { .. })));
    assert_eq!(file.path.metadata()?.len(), 4096);
    assert!(db.read_tx()?.get_bucket("large").is_err());
    db.verify()
}

#[test]
fn growth_increment_is_respected_within_the_capacity_limit() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = OpenOptions::new()
        .pagesize(1024)
        .num_pages(4)
        .growth_increment(2048)
        .max_file_bytes(32 * 1024)
        .open(&file)?;
    db.update(|tx| {
        tx.create_bucket("items")?
            .put("key", vec![3_u8; 5 * 1024])?;
        Ok(())
    })?;
    let bytes = file.path.metadata()?.len();
    assert!(bytes <= 32 * 1024);
    assert_eq!((bytes - 4096) % 2048, 0);
    db.verify()
}
