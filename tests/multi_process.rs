use std::{
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use tagdata::{DB, Error};

mod common;

#[test]
fn reader_keeps_snapshot_while_another_process_commits() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;
    let ready = sibling(&file.path, ".reader-ready");
    let release = sibling(&file.path, ".reader-release");

    let mut reader = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("snapshot_reader_child")
        .env("TAGDATA_MP_DB", &file.path)
        .env("TAGDATA_MP_READY", &ready)
        .env("TAGDATA_MP_RELEASE", &release)
        .spawn()?;
    wait_for(&ready)?;

    let db = DB::open(&file)?;
    let stats = db.stats()?;
    assert_eq!(stats.active_readers, 1);
    assert!(stats.oldest_reader_tx_id.is_some());
    let tx = db.tx(true)?;
    let bucket = tx.get_bucket("data")?;
    bucket.put("value", "new")?;
    bucket.put("growth", vec![9; 512 * 1024])?;
    tx.commit()?;
    std::fs::write(&release, b"continue")?;

    assert!(reader.wait()?.success());
    let _ = std::fs::remove_file(ready);
    let _ = std::fs::remove_file(release);
    db.check()
}

#[test]
fn snapshot_reader_child() -> Result<(), Error> {
    let Ok(path) = std::env::var("TAGDATA_MP_DB") else {
        return Ok(());
    };
    let ready = PathBuf::from(std::env::var("TAGDATA_MP_READY").unwrap());
    let release = PathBuf::from(std::env::var("TAGDATA_MP_RELEASE").unwrap());
    let db = DB::open(path)?;

    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("data")?;
    assert_eq!(bucket.get_kv("value").unwrap().value(), b"old");
    assert!(bucket.get("growth").is_none());
    std::fs::write(&ready, b"ready")?;
    wait_for(&release)?;
    assert_eq!(bucket.get_kv("value").unwrap().value(), b"old");
    assert!(bucket.get("growth").is_none());
    drop(bucket);
    drop(tx);

    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("data")?;
    assert_eq!(bucket.get_kv("value").unwrap().value(), b"new");
    assert_eq!(bucket.get_kv("growth").unwrap().value().len(), 512 * 1024);
    Ok(())
}

#[test]
fn competing_process_writers_do_not_lose_updates() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;
    let executable = std::env::current_exe()?;
    let mut writers = Vec::new();
    for index in 0..6 {
        writers.push(
            Command::new(&executable)
                .arg("--exact")
                .arg("writer_child")
                .env("TAGDATA_MP_WRITE_DB", &file.path)
                .env("TAGDATA_MP_WRITE_KEY", format!("writer-{index}"))
                .spawn()?,
        );
    }
    for mut writer in writers {
        assert!(writer.wait()?.success());
    }

    let db = DB::open(&file)?;
    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("data")?;
    for index in 0..6 {
        let key = format!("writer-{index}");
        assert_eq!(bucket.get_kv(key).unwrap().value(), b"committed");
    }
    Ok(())
}

#[test]
fn writer_child() -> Result<(), Error> {
    let Ok(path) = std::env::var("TAGDATA_MP_WRITE_DB") else {
        return Ok(());
    };
    let key = std::env::var("TAGDATA_MP_WRITE_KEY").unwrap();
    let db = DB::open(path)?;
    let tx = db.tx(true)?;
    tx.get_bucket("data")?.put(key, "committed")?;
    tx.commit()
}

#[test]
fn crashed_reader_registration_is_reclaimed() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;
    let ready = sibling(&file.path, ".crash-ready");
    let status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("crashed_reader_child")
        .env("TAGDATA_MP_CRASH_DB", &file.path)
        .env("TAGDATA_MP_CRASH_READY", &ready)
        .status()?;
    assert!(!status.success());

    let db = DB::open(&file)?;
    let tx = db.tx(true)?;
    tx.get_bucket("data")?.put("after-crash", "safe")?;
    tx.commit()?;
    assert_eq!(db.stats()?.active_readers, 0);
    let _ = std::fs::remove_file(ready);
    db.check()
}

#[test]
fn crashed_reader_child() -> Result<(), Error> {
    let Ok(path) = std::env::var("TAGDATA_MP_CRASH_DB") else {
        return Ok(());
    };
    let ready = std::env::var("TAGDATA_MP_CRASH_READY").unwrap();
    let db = DB::open(path)?;
    let _tx = db.tx(false)?;
    std::fs::write(ready, b"registered")?;
    std::process::abort();
}

#[test]
fn separate_handles_keep_independent_writer_locks() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;

    let first = DB::open(&file)?;
    let second = DB::open(&file)?;
    let writer = first.write_tx()?;
    assert!(second.try_write_tx()?.is_none());
    drop(writer);
    assert!(second.try_write_tx()?.is_some());
    Ok(())
}

#[test]
fn closing_another_handle_keeps_reader_registered() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;

    let owner = DB::open(&file)?;
    let other = DB::open(&file)?;
    let reader = owner.read_tx()?;
    drop(other);

    let observer = DB::open(&file)?;
    let stats = observer.stats()?;
    assert_eq!(stats.active_readers, 1);
    assert_eq!(stats.oldest_reader_tx_id, Some(1));
    drop(reader);
    assert_eq!(observer.stats()?.active_readers, 0);
    Ok(())
}

#[test]
fn readers_with_the_same_snapshot_are_counted_exactly() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;
    let db = DB::open(&file)?;

    let readers = (0..64)
        .map(|_| db.read_tx())
        .collect::<Result<Vec<_>, _>>()?;
    let stats = db.stats()?;
    assert_eq!(stats.active_readers, 64);
    assert_eq!(stats.oldest_reader_tx_id, Some(1));
    drop(readers);
    assert_eq!(db.stats()?.active_readers, 0);
    Ok(())
}

#[test]
fn reader_registration_churn_does_not_interrupt_writers() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;
    let readers = (0..4)
        .map(|_| {
            let path = file.path.clone();
            thread::spawn(move || -> Result<(), Error> {
                let db = DB::open(path)?;
                for _ in 0..200 {
                    drop(db.read_tx()?);
                }
                Ok(())
            })
        })
        .collect::<Vec<_>>();

    let db = DB::open(&file)?;
    for index in 0_u64..100 {
        db.update(|tx| {
            tx.get_bucket("data")?.put("churn", index.to_be_bytes())?;
            Ok(())
        })?;
    }
    for reader in readers {
        reader.join().unwrap()?;
    }
    db.verify()
}

#[test]
#[cfg(any(
    target_os = "android",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos"
))]
fn database_coordination_creates_only_the_database_file() -> Result<(), Error> {
    let directory = common::RandomFile::new();
    std::fs::create_dir(&directory.path)?;
    let path = directory.path.join("only.db");

    let db = DB::open(&path)?;
    db.update(|tx| {
        tx.create_bucket("data")?.put("key", "value")?;
        Ok(())
    })?;
    let size = path.metadata()?.len();
    let reader = db.read_tx()?;
    assert_eq!(db.stats()?.active_readers, 1);
    drop(reader);
    assert_eq!(path.metadata()?.len(), size);
    drop(db);

    let entries = std::fs::read_dir(&directory.path)?.collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path(), path);
    std::fs::remove_file(&path)?;
    std::fs::remove_dir(&directory.path)?;
    Ok(())
}

#[test]
#[cfg(any(target_os = "android", target_os = "linux"))]
fn renamed_database_keeps_reader_coordination() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;
    let db = DB::open(&file)?;
    let renamed = sibling(&file.path, ".renamed");
    std::fs::rename(&file.path, &renamed)?;

    let reader = db.read_tx()?;
    let observer = DB::open(&renamed)?;
    assert_eq!(observer.stats()?.active_readers, 1);
    drop(reader);
    assert_eq!(observer.stats()?.active_readers, 0);
    drop(observer);
    drop(db);
    std::fs::remove_file(renamed)?;
    Ok(())
}

#[test]
fn rapid_process_open_and_close_cycles_remain_consistent() -> Result<(), Error> {
    let file = common::RandomFile::new();
    initialize(&file)?;
    let executable = std::env::current_exe()?;
    let mut children = Vec::new();
    for _ in 0..4 {
        children.push(
            Command::new(&executable)
                .arg("--exact")
                .arg("rapid_open_child")
                .env("TAGDATA_MP_RAPID_DB", &file.path)
                .spawn()?,
        );
    }
    for mut child in children {
        assert!(child.wait()?.success());
    }
    DB::open(&file)?.check()
}

#[test]
fn rapid_open_child() -> Result<(), Error> {
    let Ok(path) = std::env::var("TAGDATA_MP_RAPID_DB") else {
        return Ok(());
    };
    for _ in 0..30 {
        let db = DB::open(&path)?;
        let tx = db.tx(false)?;
        assert!(tx.get_bucket("data")?.get("value").is_some());
    }
    Ok(())
}

fn initialize(file: &common::RandomFile) -> Result<(), Error> {
    let db = DB::open(file)?;
    let tx = db.tx(true)?;
    let bucket = tx.create_bucket("data")?;
    bucket.put("value", "old")?;
    tx.commit()
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut result = path.as_os_str().to_owned();
    result.push(suffix);
    PathBuf::from(result)
}

fn wait_for(path: &Path) -> Result<(), Error> {
    let started = Instant::now();
    while !path.exists() {
        if started.elapsed() > Duration::from_secs(10) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("timed out waiting for {}", path.display()),
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}
