use std::{
    fs::{File, OpenOptions as FileOpenOptions},
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{Bucket, DB, Data, Error, OpenOptions, Result};

static OPERATOR_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedRecord {
    pub bucket_path: Vec<Vec<u8>>,
    pub key: Option<Vec<u8>>,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SalvageManifest {
    pub copied_buckets: u64,
    pub copied_records: u64,
    pub skipped_pages: Vec<u64>,
    pub skipped_records: Vec<SkippedRecord>,
}

impl DB {
    /// Takes a byte-for-byte snapshot while holding the cross-process writer slot.
    pub fn physical_backup_to(&self, destination: impl AsRef<Path>) -> Result<()> {
        let destination = destination.as_ref();
        reject_destination(&self.inner.path, destination)?;
        let temporary = temporary_sibling(destination);
        let result = (|| {
            let writer_guard = self.write_tx()?;
            std::fs::copy(&self.inner.path, &temporary)?;
            FileOpenOptions::new()
                .read(true)
                .write(true)
                .open(&temporary)?
                .sync_all()?;
            let copied = OpenOptions::new()
                .read_only()
                .verify_on_open(true)
                .open(&temporary)?;
            drop(copied);
            drop(writer_guard);
            std::fs::rename(&temporary, destination)?;
            sync_parent(destination)?;
            Ok(())
        })();
        cleanup(&temporary);
        result
    }

    /// Conservatively copies readable records into a new database.
    ///
    /// The source is never modified. Unreadable subtrees are recorded in the
    /// returned manifest and skipped rather than repaired in place.
    pub fn salvage_to(&self, destination: impl AsRef<Path>) -> Result<SalvageManifest> {
        let destination = destination.as_ref();
        reject_destination(&self.inner.path, destination)?;
        let source = self.read_tx()?;
        let target = OpenOptions::new()
            .pagesize(self.pagesize())
            .open(destination)?;
        let target_tx = target.write_tx()?;
        let mut manifest = SalvageManifest::default();
        if let Ok(report) = self.verify_report() {
            manifest
                .skipped_pages
                .extend(report.issues.into_iter().filter_map(|issue| issue.page_id));
        }

        for (name, source_bucket) in source.buckets() {
            let name = name.name().to_vec();
            let target_bucket = target_tx.create_bucket(name.clone())?;
            manifest.copied_buckets += 1;
            salvage_bucket(
                &source_bucket,
                &target_bucket,
                &mut vec![name],
                &mut manifest,
            )?;
        }
        target_tx.commit()?;
        target.verify()?;
        Ok(manifest)
    }
}

fn salvage_bucket(
    source: &Bucket<'_, '_>,
    target: &Bucket<'_, '_>,
    path: &mut Vec<Vec<u8>>,
    manifest: &mut SalvageManifest,
) -> Result<()> {
    let mut cursor = source.cursor();
    loop {
        let item = catch_unwind(AssertUnwindSafe(|| cursor.next()));
        let data = match item {
            Ok(Some(data)) => data,
            Ok(None) => break,
            Err(_) => {
                manifest.skipped_records.push(SkippedRecord {
                    bucket_path: path.clone(),
                    key: None,
                    reason: "panic while traversing damaged bucket".into(),
                });
                break;
            }
        };
        match data {
            Data::KeyValue(pair) => {
                target.put(pair.key().to_vec(), pair.value().to_vec())?;
                manifest.copied_records += 1;
            }
            Data::Bucket(name) => {
                let key = name.name().to_vec();
                match source.get_bucket(key.clone()) {
                    Ok(source_child) => {
                        let target_child = target.create_bucket(key.clone())?;
                        manifest.copied_buckets += 1;
                        path.push(key);
                        salvage_bucket(&source_child, &target_child, path, manifest)?;
                        path.pop();
                    }
                    Err(error) => manifest.skipped_records.push(SkippedRecord {
                        bucket_path: path.clone(),
                        key: Some(key),
                        reason: error.to_string(),
                    }),
                }
            }
        }
    }
    target.inner.borrow_mut().meta.next_int = source.next_int();
    Ok(())
}

fn reject_destination(source: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "destination already exists",
        )));
    }
    let destination = if destination.is_absolute() {
        destination.to_owned()
    } else {
        std::env::current_dir()?.join(destination)
    };
    if source.canonicalize()? == destination {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "source and destination are the same",
        )));
    }
    Ok(())
}

fn temporary_sibling(destination: &Path) -> PathBuf {
    let suffix = format!(
        ".physical-{}-{}",
        std::process::id(),
        OPERATOR_NONCE.fetch_add(1, Ordering::Relaxed)
    );
    PathBuf::from(format!("{}{}", destination.display(), suffix))
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()?;
    Ok(())
}
