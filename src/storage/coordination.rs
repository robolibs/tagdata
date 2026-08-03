use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use fs4::FileExt;

use crate::{Error, Result};

static READER_NONCE: AtomicU64 = AtomicU64::new(0);

pub(crate) struct Coordination {
    gate: Mutex<File>,
    readers: PathBuf,
}

impl Coordination {
    pub(crate) fn open(database: &Path) -> Result<Self> {
        let root = sidecar_path(database);
        let readers = root.join("readers");
        std::fs::create_dir_all(&readers)?;
        let gate = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("gate.lock"))?;
        Ok(Self {
            gate: Mutex::new(gate),
            readers,
        })
    }

    pub(crate) fn shared_gate(&self) -> Result<GateGuard<'_>> {
        let gate = self.gate.lock()?;
        FileExt::lock_shared(&*gate)?;
        Ok(GateGuard { gate })
    }

    pub(crate) fn exclusive_gate(&self) -> Result<GateGuard<'_>> {
        let gate = self.gate.lock()?;
        FileExt::lock_exclusive(&*gate)?;
        Ok(GateGuard { gate })
    }

    pub(crate) fn register(&self, tx_id: u64) -> Result<ReaderRegistration> {
        loop {
            let nonce = READER_NONCE.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let name = format!(
                "reader-{tx_id:020}-{}-{timestamp}-{nonce}",
                std::process::id()
            );
            let path = self.readers.join(name);
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    FileExt::lock_shared(&file)?;
                    return Ok(ReaderRegistration { file, path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    pub(crate) fn readers(&self) -> Result<(u64, Option<u64>)> {
        let mut count = 0;
        let mut oldest: Option<u64> = None;
        for entry in std::fs::read_dir(&self.readers)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            let file = match OpenOptions::new().read(true).write(true).open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => {
                    FileExt::unlock(&file)?;
                    let _ = std::fs::remove_file(path);
                }
                Err(error) if error.kind() == fs4::lock_contended_error().kind() => {
                    let tx_id = parse_tx_id(&entry.file_name()).ok_or_else(|| {
                        Error::InvalidDB(format!(
                            "active reader registration has an invalid name: {}",
                            path.display()
                        ))
                    })?;
                    count += 1;
                    oldest = Some(oldest.map_or(tx_id, |current| current.min(tx_id)));
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok((count, oldest))
    }
}

pub(crate) struct GateGuard<'a> {
    gate: MutexGuard<'a, File>,
}

impl Drop for GateGuard<'_> {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&*self.gate);
    }
}

pub(crate) struct ReaderRegistration {
    file: File,
    path: PathBuf,
}

impl Drop for ReaderRegistration {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(crate) fn sidecar_path(database: &Path) -> PathBuf {
    let mut path: OsString = database.as_os_str().to_owned();
    path.push(".inspace");
    PathBuf::from(path)
}

fn parse_tx_id(name: &std::ffi::OsStr) -> Option<u64> {
    name.to_str()?
        .strip_prefix("reader-")?
        .split('-')
        .next()?
        .parse()
        .ok()
}
