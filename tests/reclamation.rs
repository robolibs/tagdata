use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use tagdata::{DB, Error, FORMAT_VERSION, FormatInfo, OpenOptions};

mod common;

#[test]
fn current_format_persists_retirement_generations_across_handles() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = OpenOptions::new()
        .pagesize(4096)
        .num_pages(4)
        .growth_increment(4096)
        .open(&file)?;
    db.update(|tx| {
        tx.create_bucket("items")?
            .put("key", vec![1_u8; 32 * 1024])?;
        Ok(())
    })?;
    assert_eq!(FormatInfo::inspect(&file)?.version, FORMAT_VERSION);

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
fn dead_reader_immediately_releases_pinned_pages() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = OpenOptions::new()
        .pagesize(4096)
        .num_pages(4)
        .growth_increment(4096)
        .open(&file)?;
    db.update(|tx| {
        tx.create_bucket("items")?
            .put("key", vec![1_u8; 64 * 1024])?;
        Ok(())
    })?;
    let ready = sibling(&file.path, ".reader-ready");
    let mut child = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("pinned_reader_child")
        .env("TAGDATA_PINNED_DB", &file.path)
        .env("TAGDATA_PINNED_READY", &ready)
        .spawn()?;
    wait_for(&ready)?;

    db.update(|tx| {
        tx.get_bucket("items")?.put("key", vec![2_u8; 64 * 1024])?;
        Ok(())
    })?;
    assert!(db.stats()?.pending_pages > 0);

    child.kill()?;
    let _ = child.wait()?;
    assert_eq!(db.stats()?.active_readers, 0);
    db.update(|tx| {
        tx.get_bucket("items")?.put("after-crash", "ok")?;
        Ok(())
    })?;
    assert!(db.stats()?.free_pages > 0);
    let _ = std::fs::remove_file(ready);
    db.verify()
}

#[test]
fn pinned_reader_child() -> Result<(), Error> {
    let Ok(path) = std::env::var("TAGDATA_PINNED_DB") else {
        return Ok(());
    };
    let ready = std::env::var("TAGDATA_PINNED_READY").unwrap();
    let db = DB::open(path)?;
    let _reader = db.read_tx()?;
    std::fs::write(ready, b"ready")?;
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
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
