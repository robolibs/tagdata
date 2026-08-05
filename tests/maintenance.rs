#![cfg(feature = "maintenance")]

use std::{fs::File, io::Write};

#[cfg(feature = "test-hooks")]
use std::process::Command;

use inspace::{DB, Error, OpenOptions};

mod common;

#[test]
fn backup_preserves_nested_data_and_sequences() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let backup = common::RandomFile::new();
    let db = DB::open(&source)?;
    populate(&db)?;

    db.backup_to(&backup.path)?;
    let copied = DB::open(&backup)?;
    assert_database(&copied)?;
    copied.check()
}

#[test]
fn backup_writer_produces_an_openable_database() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let output = common::RandomFile::new();
    let db = DB::open(&source)?;
    populate(&db)?;

    let mut bytes = Vec::new();
    db.backup_writer(&mut bytes)?;
    let mut file = File::create(&output.path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;

    let copied = DB::open(&output)?;
    assert_database(&copied)?;
    copied.check()
}

#[test]
fn compaction_reduces_preallocated_database_size() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let compact = common::RandomFile::new();
    let db = OpenOptions::new().num_pages(4096).open(&source)?;
    populate(&db)?;
    let source_size = source.path.metadata()?.len();

    db.compact_to(&compact.path)?;
    let compact_size = compact.path.metadata()?.len();
    assert!(compact_size < source_size / 4);
    let copied = DB::open(&compact)?;
    assert_database(&copied)?;
    copied.check()
}

#[test]
fn compaction_can_change_page_size() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let compact = common::RandomFile::new();
    let db = DB::open(&source)?;
    populate(&db)?;

    db.compact_to_with_page_size(&compact.path, 8192)?;
    let copied = OpenOptions::new().pagesize(8192).open(&compact)?;
    assert_eq!(copied.pagesize(), 8192);
    assert_database(&copied)?;
    copied.check()
}

#[test]
fn backup_is_one_complete_snapshot_during_a_commit() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let backup = common::RandomFile::new();
    let db = DB::open(&source)?;
    {
        let tx = db.tx(true)?;
        let bucket = tx.create_bucket("items")?;
        for key in 0..5000_u64 {
            bucket.put(key.to_be_bytes(), "old")?;
        }
        tx.commit()?;
    }

    let writer_db = db.clone();
    let writer = std::thread::spawn(move || -> Result<(), Error> {
        let tx = writer_db.tx(true)?;
        let bucket = tx.get_bucket("items")?;
        for key in 0..5000_u64 {
            bucket.put(key.to_be_bytes(), "new")?;
        }
        tx.commit()
    });
    db.backup_to(&backup.path)?;
    writer.join().unwrap()?;

    let copied = DB::open(&backup)?;
    let tx = copied.tx(false)?;
    let bucket = tx.get_bucket("items")?;
    let first = bucket.get_kv(0_u64.to_be_bytes()).unwrap().value().to_vec();
    for key in 1..5000_u64 {
        assert_eq!(bucket.get_kv(key.to_be_bytes()).unwrap().value(), first);
    }
    copied.check()
}

#[test]
fn backup_rejects_existing_or_source_destinations() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let existing = common::RandomFile::new();
    let db = DB::open(&source)?;
    File::create(&existing.path)?;

    assert!(
        matches!(db.backup_to(&existing.path), Err(Error::Io(ref error)) if error.kind() == std::io::ErrorKind::AlreadyExists)
    );
    assert!(
        matches!(db.backup_to(&source.path), Err(Error::Io(ref error)) if error.kind() == std::io::ErrorKind::InvalidInput)
    );
    Ok(())
}

#[cfg(feature = "test-hooks")]
#[test]
fn interrupted_backup_never_publishes_a_partial_destination() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let destination = common::RandomFile::new();
    let db = DB::open(&source)?;
    populate(&db)?;
    drop(db);

    let status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("backup_abort_child")
        .env("INSPACE_BACKUP_SOURCE", &source.path)
        .env("INSPACE_BACKUP_DESTINATION", &destination.path)
        .env("INSPACE_FAILPOINT", "backup-before-rename")
        .status()?;
    assert!(!status.success());
    assert!(!destination.path.exists());
    DB::open(&source)?.check()?;
    remove_temporary_siblings(&destination.path)?;
    Ok(())
}

#[cfg(feature = "test-hooks")]
#[test]
fn backup_abort_child() -> Result<(), Error> {
    let Ok(source) = std::env::var("INSPACE_BACKUP_SOURCE") else {
        return Ok(());
    };
    let destination = std::env::var("INSPACE_BACKUP_DESTINATION").unwrap();
    DB::open(source)?.backup_to(destination)
}

fn populate(db: &DB) -> Result<(), Error> {
    let tx = db.tx(true)?;
    let root = tx.create_bucket("root")?;
    root.put("keep", "value")?;
    root.put("deleted", "gone")?;
    root.delete("deleted")?;
    let nested = root.create_bucket("nested")?;
    nested.put("number", 42_u64.to_be_bytes())?;
    nested.put("removed", "gone")?;
    nested.delete("removed")?;
    tx.commit()
}

fn assert_database(db: &DB) -> Result<(), Error> {
    let tx = db.tx(false)?;
    let root = tx.get_bucket("root")?;
    assert_eq!(root.get_kv("keep").unwrap().value(), b"value");
    assert!(root.get("deleted").is_none());
    assert_eq!(root.next_int(), 3);
    let nested = root.get_bucket("nested")?;
    assert_eq!(
        nested.get_kv("number").unwrap().value(),
        42_u64.to_be_bytes()
    );
    assert!(nested.get("removed").is_none());
    assert_eq!(nested.next_int(), 2);
    Ok(())
}

#[cfg(feature = "test-hooks")]
fn remove_temporary_siblings(destination: &std::path::Path) -> Result<(), Error> {
    let parent = destination.parent().unwrap();
    let prefix = format!("{}", destination.file_name().unwrap().to_string_lossy());
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        if name
            .to_string_lossy()
            .starts_with(&format!("{prefix}.tmp-"))
        {
            if entry.file_type()?.is_dir() {
                std::fs::remove_dir_all(entry.path())?;
            } else {
                std::fs::remove_file(entry.path())?;
            }
        }
    }
    Ok(())
}
