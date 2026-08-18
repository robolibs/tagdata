#![cfg(feature = "operator")]

use std::{
    fs::OpenOptions as FileOpenOptions,
    io::{Read, Seek, SeekFrom, Write},
};

use tagdata::{DB, Error, OpenOptions};

mod common;

const PAGE_SIZE: usize = 4096;

#[test]
fn physical_backup_is_byte_exact_and_openable() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let destination = common::RandomFile::new();
    let db = populated(&source)?;
    db.physical_backup_to(&destination.path)?;

    assert_eq!(
        std::fs::read(&source.path)?,
        std::fs::read(&destination.path)?
    );
    let copy = DB::open(&destination)?;
    copy.verify()?;
    assert_eq!(
        copy.read_tx()?
            .get_bucket("records")?
            .get_kv(7_u64.to_be_bytes())?
            .unwrap()
            .value(),
        vec![7; 80]
    );
    Ok(())
}

#[test]
fn salvage_copies_into_a_new_verified_destination_with_manifest() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let destination = common::RandomFile::new();
    let db = populated(&source)?;
    let manifest = db.salvage_to(&destination.path)?;
    assert_eq!(manifest.copied_buckets, 1);
    assert_eq!(manifest.copied_records, 100);
    assert!(manifest.skipped_pages.is_empty());
    assert!(manifest.skipped_records.is_empty());
    DB::open(&destination)?.verify()
}

#[test]
fn verification_reports_page_location_and_invariant() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let db = populated(&source)?;
    drop(db);
    let bytes = std::fs::read(&source.path)?;
    let leaf_offset = (2..bytes.len() / PAGE_SIZE)
        .rev()
        .map(|page| page * PAGE_SIZE)
        .find(|offset| bytes[*offset + 8] == 2)
        .expect("leaf page");
    flip(&source.path, leaf_offset + 64)?;

    let db = DB::open(&source)?;
    let report = db.verify_report()?;
    assert!(!report.valid);
    let issue = &report.issues[0];
    assert_eq!(
        issue.offset,
        issue.page_id.map(|page| page * PAGE_SIZE as u64)
    );
    assert_eq!(issue.page_kind.as_deref(), Some("leaf"));
    assert!(!issue.invariant.is_empty());
    Ok(())
}

fn populated(file: &common::RandomFile) -> Result<DB, Error> {
    let db = OpenOptions::new().pagesize(PAGE_SIZE as u64).open(file)?;
    db.update(|tx| {
        let records = tx.create_bucket("records")?;
        for key in 0..100_u64 {
            records.put(key.to_be_bytes(), vec![key as u8; 80])?;
        }
        Ok(())
    })?;
    Ok(db)
}

fn flip(path: &std::path::Path, offset: usize) -> Result<(), Error> {
    let mut file = FileOpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut byte = [0];
    file.read_exact(&mut byte)?;
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset as u64))?;
    file.write_all(&byte)?;
    file.sync_all()?;
    Ok(())
}
