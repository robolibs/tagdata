use std::{
    ffi::OsString,
    fs::{File, OpenOptions as FileOpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{Bucket, DB, Data, OpenOptions, Result, support::failpoints};

static TEMP_NONCE: AtomicU64 = AtomicU64::new(0);

impl DB {
    /// Writes a validated snapshot to a new database file.
    pub fn backup_to<P: AsRef<Path>>(&self, destination: P) -> Result<()> {
        self.copy_to(destination.as_ref(), self.pagesize())
    }

    /// Writes a validated snapshot to a byte stream and flushes it.
    pub fn backup_writer<W: Write>(&self, mut writer: W) -> Result<()> {
        let path = temporary_stream_path();
        let result = (|| {
            self.backup_to(&path)?;
            let mut file = File::open(&path)?;
            std::io::copy(&mut file, &mut writer)?;
            writer.flush()?;
            Ok(())
        })();
        cleanup(&path);
        result
    }

    /// Copies live data to a compact database using the current page size.
    pub fn compact_to<P: AsRef<Path>>(&self, destination: P) -> Result<()> {
        self.copy_to(destination.as_ref(), self.pagesize())
    }

    /// Copies live data to a compact database using a selected page size.
    pub fn compact_to_with_page_size<P: AsRef<Path>>(
        &self,
        destination: P,
        page_size: u64,
    ) -> Result<()> {
        self.copy_to(destination.as_ref(), page_size)
    }

    fn copy_to(&self, destination: &Path, page_size: u64) -> Result<()> {
        if same_path(&self.inner.path, destination)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "source and destination database paths are the same",
            )
            .into());
        }
        if destination.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "backup destination already exists",
            )
            .into());
        }

        let temporary = temporary_sibling(destination);
        let result = (|| {
            copy_snapshot(self, &temporary, page_size)?;
            let file = FileOpenOptions::new()
                .read(true)
                .write(true)
                .open(&temporary)?;
            file.sync_all()?;
            failpoints::hit("backup-before-rename");
            std::fs::rename(&temporary, destination)?;
            sync_parent(destination)?;
            Ok(())
        })();
        cleanup(&temporary);
        result
    }
}

fn copy_snapshot(source: &DB, destination: &Path, page_size: u64) -> Result<()> {
    let source_tx = source.tx(false)?;
    let target = OpenOptions::new().pagesize(page_size).open(destination)?;
    let target_tx = target.tx(true)?;

    for (name, source_bucket) in source_tx.buckets() {
        let target_bucket = target_tx.create_bucket(name.name().to_vec())?;
        copy_bucket(&source_bucket, &target_bucket)?;
    }
    let root_next_int = source_tx.inner.borrow().root.borrow().meta.next_int;
    target_tx.inner.borrow().root.borrow_mut().meta.next_int = root_next_int;
    target_tx.commit()?;
    target.check()?;
    drop(target);
    Ok(())
}

fn copy_bucket(source: &Bucket<'_, '_>, target: &Bucket<'_, '_>) -> Result<()> {
    for data in source.cursor() {
        match data {
            Data::KeyValue(pair) => {
                target.put(pair.key().to_vec(), pair.value().to_vec())?;
            }
            Data::Bucket(name) => {
                let name = name.name().to_vec();
                let source_child = source.get_bucket(name.clone())?;
                let target_child = target.create_bucket(name.clone())?;
                copy_bucket(&source_child, &target_child)?;
            }
        }
    }
    target.inner.borrow_mut().meta.next_int = source.next_int();
    Ok(())
}

fn temporary_sibling(destination: &Path) -> PathBuf {
    let mut path: OsString = destination.as_os_str().to_owned();
    path.push(format!(
        ".tmp-{}-{}",
        std::process::id(),
        TEMP_NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    PathBuf::from(path)
}

fn temporary_stream_path() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tagdata-backup-{}-{timestamp}-{}.db",
        std::process::id(),
        TEMP_NONCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
}

fn same_path(source: &Path, destination: &Path) -> Result<bool> {
    let source = source.canonicalize()?;
    let destination = if destination.is_absolute() {
        destination.to_path_buf()
    } else {
        std::env::current_dir()?.join(destination)
    };
    Ok(source == destination)
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}
