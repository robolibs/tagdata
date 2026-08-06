use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{Error, Result};

const LOCK_BASE: u64 = 1 << 60;
pub(crate) const MAX_DATA_FILE_BYTES: u64 = LOCK_BASE;
const GATE_OFFSET: u64 = LOCK_BASE;
const TOKEN_BASE: u64 = LOCK_BASE + 8;
const MAX_READER_SLOTS: u64 = 65_536;
const PAYLOAD_BASE: u64 = LOCK_BASE + (1 << 20);
const PAYLOAD_STRIDE: u64 = 1 << 33;
const LOW_MASK: u64 = u32::MAX as u64;

static READER_SLOT: AtomicU64 = AtomicU64::new(0);

pub(crate) struct Coordination {
    gate: Mutex<File>,
    probe: Mutex<File>,
}

impl Coordination {
    pub(crate) fn open(database: &Path) -> Result<Self> {
        let gate = OpenOptions::new().read(true).write(true).open(database)?;
        let probe = OpenOptions::new().read(true).write(true).open(database)?;
        Ok(Self {
            gate: Mutex::new(gate),
            probe: Mutex::new(probe),
        })
    }

    pub(crate) fn shared_gate(&self) -> Result<GateGuard<'_>> {
        self.lock_gate(libc::F_RDLCK as libc::c_short, true)
            .map(Option::unwrap)
    }

    pub(crate) fn exclusive_gate(&self) -> Result<GateGuard<'_>> {
        self.lock_gate(libc::F_WRLCK as libc::c_short, true)
            .map(Option::unwrap)
    }

    pub(crate) fn try_exclusive_gate(&self) -> Result<Option<GateGuard<'_>>> {
        self.lock_gate(libc::F_WRLCK as libc::c_short, false)
    }

    pub(crate) fn register(&self, tx_id: u64) -> Result<ReaderRegistration> {
        let file = self.open_registration_file()?;
        let first = READER_SLOT.fetch_add(1, Ordering::Relaxed) % MAX_READER_SLOTS;
        for attempt in 0..MAX_READER_SLOTS {
            let slot = (first + attempt) % MAX_READER_SLOTS;
            if !try_set_lock(&file, token_offset(slot), 1, libc::F_WRLCK as libc::c_short)? {
                continue;
            }
            let (start, length) = encode_tx_id(slot, tx_id);
            set_lock(&file, start, length, libc::F_RDLCK as libc::c_short, false)?;
            return Ok(ReaderRegistration { _file: file });
        }
        Err(Error::Sync("reader registration table is full"))
    }

    pub(crate) fn readers(&self) -> Result<(u64, Option<u64>)> {
        let probe = self.probe.lock()?;
        let slots = locked_tokens(&probe)?;
        let mut count = 0;
        let mut oldest: Option<u64> = None;
        for slot in &slots {
            let block = payload_block(*slot);
            let Some(lock) = get_lock(&probe, block, PAYLOAD_STRIDE)? else {
                continue;
            };
            let tx_id = decode_tx_id(block, lock)?;
            count += 1;
            oldest = Some(oldest.map_or(tx_id, |current| current.min(tx_id)));
        }
        Ok((count, oldest))
    }

    fn lock_gate(&self, kind: libc::c_short, wait: bool) -> Result<Option<GateGuard<'_>>> {
        let gate = match self.gate.try_lock() {
            Ok(gate) => gate,
            Err(std::sync::TryLockError::WouldBlock) if !wait => return Ok(None),
            Err(std::sync::TryLockError::WouldBlock) => self.gate.lock()?,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(Error::Sync("lock poisoned"));
            }
        };
        if wait {
            set_lock(&gate, GATE_OFFSET, 1, kind, true)?;
            Ok(Some(GateGuard { gate }))
        } else if try_set_lock(&gate, GATE_OFFSET, 1, kind)? {
            Ok(Some(GateGuard { gate }))
        } else {
            Ok(None)
        }
    }

    fn open_registration_file(&self) -> Result<File> {
        let probe = self.probe.lock()?;
        open_independent(&probe)
    }
}

pub(crate) struct GateGuard<'a> {
    gate: MutexGuard<'a, File>,
}

impl Drop for GateGuard<'_> {
    fn drop(&mut self) {
        let _ = set_lock(
            &self.gate,
            GATE_OFFSET,
            1,
            libc::F_UNLCK as libc::c_short,
            false,
        );
    }
}

pub(crate) struct ReaderRegistration {
    _file: File,
}

#[derive(Clone, Copy)]
struct LockRange {
    start: u64,
    length: u64,
}

fn locked_tokens(file: &File) -> Result<Vec<u64>> {
    let mut slots = Vec::new();
    let mut ranges = vec![(TOKEN_BASE, TOKEN_BASE + MAX_READER_SLOTS)];
    while let Some((start, end)) = ranges.pop() {
        if start >= end {
            continue;
        }
        let Some(lock) = get_lock(file, start, end - start)? else {
            continue;
        };
        let lock_end = lock
            .start
            .checked_add(lock.length)
            .ok_or_else(|| Error::InvalidDB("reader coordination lock range overflowed".into()))?;
        if lock.start < TOKEN_BASE || lock_end > TOKEN_BASE + MAX_READER_SLOTS {
            return Err(Error::InvalidDB(
                "reader coordination lock escaped the token region".into(),
            ));
        }
        slots.push(lock.start - TOKEN_BASE);
        ranges.push((start, lock.start));
        ranges.push((lock_end, end));
    }
    Ok(slots)
}

fn encode_tx_id(slot: u64, tx_id: u64) -> (u64, u64) {
    let low = tx_id & LOW_MASK;
    let high = (tx_id >> 32) + 1;
    (payload_block(slot) + low, high)
}

fn decode_tx_id(block: u64, lock: LockRange) -> Result<u64> {
    let low = lock
        .start
        .checked_sub(block)
        .filter(|value| *value <= LOW_MASK)
        .ok_or_else(|| Error::InvalidDB("invalid reader transaction lock offset".into()))?;
    let high = lock
        .length
        .checked_sub(1)
        .filter(|value| *value <= LOW_MASK)
        .ok_or_else(|| Error::InvalidDB("invalid reader transaction lock length".into()))?;
    Ok((high << 32) | low)
}

fn token_offset(slot: u64) -> u64 {
    TOKEN_BASE + slot
}

fn payload_block(slot: u64) -> u64 {
    PAYLOAD_BASE + slot * PAYLOAD_STRIDE
}

#[cfg(any(target_os = "android", target_os = "linux"))]
fn open_independent(file: &File) -> Result<File> {
    let path = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
    Ok(OpenOptions::new().read(true).write(true).open(path)?)
}

#[cfg(any(
    target_os = "ios",
    target_os = "macos",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos"
))]
fn open_independent(file: &File) -> Result<File> {
    use std::{ffi::CStr, os::unix::ffi::OsStrExt};

    let mut path = [0_i8; libc::PATH_MAX as usize];
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) };
    if result == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    let path = unsafe { CStr::from_ptr(path.as_ptr()) };
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .open(std::ffi::OsStr::from_bytes(path.to_bytes()))?)
}

fn try_set_lock(file: &File, start: u64, length: u64, kind: libc::c_short) -> Result<bool> {
    match set_lock(file, start, length, kind, false) {
        Ok(()) => Ok(true),
        Err(Error::Io(error))
            if matches!(error.raw_os_error(), Some(libc::EACCES | libc::EAGAIN)) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn set_lock(file: &File, start: u64, length: u64, kind: libc::c_short, wait: bool) -> Result<()> {
    let mut lock = raw_lock(start, length, kind)?;
    let command = if wait {
        libc::F_OFD_SETLKW
    } else {
        libc::F_OFD_SETLK
    };
    let result = unsafe { libc::fcntl(file.as_raw_fd(), command, &mut lock) };
    if result == -1 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

fn get_lock(file: &File, start: u64, length: u64) -> Result<Option<LockRange>> {
    let mut lock = raw_lock(start, length, libc::F_WRLCK as libc::c_short)?;
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &mut lock) };
    if result == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    if lock.l_type == libc::F_UNLCK as libc::c_short {
        return Ok(None);
    }
    Ok(Some(LockRange {
        start: u64::try_from(lock.l_start)
            .map_err(|_| Error::InvalidDB("negative coordination lock offset".into()))?,
        length: u64::try_from(lock.l_len)
            .map_err(|_| Error::InvalidDB("negative coordination lock length".into()))?,
    }))
}

fn raw_lock(start: u64, length: u64, kind: libc::c_short) -> Result<libc::flock> {
    Ok(libc::flock {
        l_type: kind,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: start
            .try_into()
            .map_err(|_| Error::InvalidDB("coordination lock offset is unsupported".into()))?,
        l_len: length
            .try_into()
            .map_err(|_| Error::InvalidDB("coordination lock length is unsupported".into()))?,
        l_pid: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_ids_round_trip_through_lock_ranges() {
        for tx_id in [0, 1, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX] {
            let block = payload_block(7);
            let (start, length) = encode_tx_id(7, tx_id);
            assert!(start + length <= block + PAYLOAD_STRIDE);
            assert_eq!(
                decode_tx_id(block, LockRange { start, length }).unwrap(),
                tx_id
            );
        }
    }
}
