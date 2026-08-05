use std::{process::Command, time::Duration};

use tagdata::{DB, Error, OpenOptions};

mod common;

#[test]
fn read_only_handle_reads_and_rejects_writes() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;

    let first = OpenOptions::new().read_only().open(&file)?;
    let second = OpenOptions::new().read_only().open(&file)?;

    assert_eq!(first.tx(true).err(), Some(Error::ReadOnlyDB));
    for db in [&first, &second] {
        let tx = db.tx(false)?;
        let bucket = tx.get_bucket("data")?;
        assert_eq!(bucket.get_kv("key").unwrap().value(), b"value");
    }
    Ok(())
}

#[test]
fn read_only_open_does_not_create_a_database() {
    let file = common::RandomFile::new();
    let error = OpenOptions::new().read_only().open(&file).err().unwrap();
    assert!(matches!(error, Error::Io(ref io) if io.kind() == std::io::ErrorKind::NotFound));
    assert!(!file.path.exists());
}

#[cfg(unix)]
#[test]
fn read_only_open_accepts_a_non_writable_file() -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;

    let file = common::RandomFile::new();
    initialize(&file)?;
    let mut permissions = file.path.metadata()?.permissions();
    permissions.set_mode(0o444);
    std::fs::set_permissions(&file.path, permissions)?;

    {
        let db = OpenOptions::new().read_only().open(&file)?;
        assert!(db.tx(false)?.get_bucket("data")?.get("key").is_some());
    }

    let mut permissions = file.path.metadata()?.permissions();
    permissions.set_mode(0o600);
    std::fs::set_permissions(&file.path, permissions)?;
    Ok(())
}

#[test]
fn separate_processes_can_hold_read_only_handles() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;

    let executable = std::env::current_exe()?;
    let mut first = Command::new(&executable)
        .arg("--exact")
        .arg("read_only_child")
        .env("TAGDATA_READ_ONLY_DB", &file.path)
        .spawn()?;
    std::thread::sleep(Duration::from_millis(50));
    let mut second = Command::new(&executable)
        .arg("--exact")
        .arg("read_only_child")
        .env("TAGDATA_READ_ONLY_DB", &file.path)
        .spawn()?;

    assert!(first.wait()?.success());
    assert!(second.wait()?.success());
    Ok(())
}

#[test]
fn read_only_handle_blocks_a_writer_until_release() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;
    let reader = OpenOptions::new().read_only().open(&file)?;

    let mut writer = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("write_open_child")
        .env("TAGDATA_WRITE_DB", &file.path)
        .spawn()?;
    std::thread::sleep(Duration::from_millis(150));
    assert!(writer.try_wait()?.is_none());

    drop(reader);
    assert!(writer.wait()?.success());
    Ok(())
}

#[test]
fn read_only_child() -> Result<(), Error> {
    let Ok(path) = std::env::var("TAGDATA_READ_ONLY_DB") else {
        return Ok(());
    };
    let db = OpenOptions::new().read_only().open(path)?;
    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("data")?;
    assert_eq!(bucket.get_kv("key").unwrap().value(), b"value");
    std::thread::sleep(Duration::from_millis(250));
    Ok(())
}

#[test]
fn write_open_child() -> Result<(), Error> {
    let Ok(path) = std::env::var("TAGDATA_WRITE_DB") else {
        return Ok(());
    };
    let db = DB::open(path)?;
    let tx = db.tx(true)?;
    tx.get_bucket("data")?.put("writer", "finished")?;
    tx.commit()
}

fn initialize(file: &common::RandomFile) -> Result<(), Error> {
    let db = DB::open(file)?;
    let tx = db.tx(true)?;
    tx.create_bucket("data")?.put("key", "value")?;
    tx.commit()
}
