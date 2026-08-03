use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use memmap2::{Mmap, MmapOptions};

use crate::format::{
    CREATE_BUCKET, DELETE, DELETE_BUCKET, FILE_HEADER_LEN, Operation, PUT, Record,
    encode_transaction, file_header, parse_transaction, validate_file_header,
};
use crate::{Error, Result};

#[derive(Clone, Copy, Debug)]
struct Slice {
    offset: usize,
    len: usize,
}

impl Slice {
    fn get(self, mmap: &Mmap) -> &[u8] {
        &mmap[self.offset..self.offset + self.len]
    }
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    bucket: Slice,
    key: Slice,
    value: Slice,
}

struct State {
    mmap: Mmap,
    buckets: HashMap<u64, Vec<Slice>>,
    entries: HashMap<u64, Vec<Entry>>,
    txid: u64,
    valid_len: usize,
}

/// A single-file, memory-mapped database.
pub struct Database {
    file: File,
    state: RwLock<State>,
}

impl Database {
    /// Opens an existing database, or creates a new one at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        if file.metadata()?.len() == 0 {
            file.write_all(&file_header())?;
            file.sync_data()?;
        }
        let mmap = map(&file)?;
        validate_file_header(&mmap)?;
        let mut state = State {
            mmap,
            buckets: HashMap::new(),
            entries: HashMap::new(),
            txid: 0,
            valid_len: FILE_HEADER_LEN,
        };
        recover(&mut state)?;
        if state.valid_len != state.mmap.len() {
            let valid_len = state.valid_len;
            drop(state);
            file.set_len(valid_len as u64)?;
            file.sync_data()?;
            let mmap = map(&file)?;
            let mut repaired = State {
                mmap,
                buckets: HashMap::new(),
                entries: HashMap::new(),
                txid: 0,
                valid_len: FILE_HEADER_LEN,
            };
            recover(&mut repaired)?;
            state = repaired;
        }
        Ok(Self {
            file,
            state: RwLock::new(state),
        })
    }

    /// Runs a read-only transaction against a stable memory-map snapshot.
    pub fn view<T>(&self, read: impl FnOnce(&ReadTransaction<'_>) -> Result<T>) -> Result<T> {
        let state = self.read_state();
        read(&ReadTransaction { state })
    }

    /// Runs and durably commits one write transaction.
    ///
    /// Dropping the callback with an error writes nothing. An empty successful
    /// transaction also performs no I/O.
    pub fn update<T>(
        &self,
        write: impl FnOnce(&mut WriteTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let mut state = self.write_state();
        let mut tx = WriteTransaction {
            state: &state,
            operations: Vec::new(),
            bucket_changes: HashMap::new(),
        };
        let result = write(&mut tx)?;
        if tx.operations.is_empty() {
            return Ok(result);
        }

        validate_operations(&state, &tx.operations)?;
        let txid = state.txid.checked_add(1).ok_or(Error::TooLarge)?;
        let encoded = encode_transaction(txid, &tx.operations)?;
        let start = state.valid_len;
        (&self.file).seek(SeekFrom::Start(start as u64))?;
        (&self.file).write_all(&encoded)?;
        self.file.sync_data()?;

        let new_mmap = map(&self.file)?;
        let parsed = parse_transaction(&new_mmap, start)?
            .ok_or(Error::Corrupt("committed transaction is incomplete"))?;
        state.mmap = new_mmap;
        apply_records(&mut state, &parsed.records)?;
        state.txid = parsed.txid;
        state.valid_len = parsed.end;
        Ok(result)
    }

    /// Returns the last committed transaction identifier.
    pub fn transaction_id(&self) -> u64 {
        self.read_state().txid
    }

    fn read_state(&self) -> RwLockReadGuard<'_, State> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, State> {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A stable, zero-copy read transaction.
pub struct ReadTransaction<'db> {
    state: RwLockReadGuard<'db, State>,
}

impl ReadTransaction<'_> {
    /// Opens a bucket by name.
    pub fn bucket<'tx>(&'tx self, name: &[u8]) -> Result<Bucket<'tx>> {
        let name = find_bucket(&self.state, name).ok_or(Error::BucketNotFound)?;
        Ok(Bucket {
            state: &self.state,
            name,
        })
    }
}

/// A read-only view of a bucket.
pub struct Bucket<'tx> {
    state: &'tx State,
    name: Slice,
}

impl Bucket<'_> {
    /// Gets a value without copying it out of the memory map.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        find_entry(self.state, self.name.get(&self.state.mmap), key)
            .map(|entry| entry.value.get(&self.state.mmap))
    }

    /// Returns true when this bucket contains `key`.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }
}

/// A buffered write transaction.
pub struct WriteTransaction<'db> {
    state: &'db State,
    operations: Vec<Operation>,
    bucket_changes: HashMap<Vec<u8>, bool>,
}

impl WriteTransaction<'_> {
    /// Creates a bucket.
    pub fn create_bucket(&mut self, name: impl AsRef<[u8]>) -> Result<()> {
        let name = name.as_ref();
        if self.bucket_will_exist(name) {
            return Err(Error::BucketExists);
        }
        self.operations
            .push((CREATE_BUCKET, name.to_vec(), Vec::new(), Vec::new()));
        self.bucket_changes.insert(name.to_vec(), true);
        Ok(())
    }

    /// Deletes a bucket and all of its keys.
    pub fn delete_bucket(&mut self, name: impl AsRef<[u8]>) -> Result<()> {
        let name = name.as_ref();
        if !self.bucket_will_exist(name) {
            return Err(Error::BucketNotFound);
        }
        self.operations
            .push((DELETE_BUCKET, name.to_vec(), Vec::new(), Vec::new()));
        self.bucket_changes.insert(name.to_vec(), false);
        Ok(())
    }

    /// Inserts or replaces a key/value pair.
    pub fn put(
        &mut self,
        bucket: impl AsRef<[u8]>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let bucket = bucket.as_ref();
        if !self.bucket_will_exist(bucket) {
            return Err(Error::BucketNotFound);
        }
        self.operations.push((
            PUT,
            bucket.to_vec(),
            key.as_ref().to_vec(),
            value.as_ref().to_vec(),
        ));
        Ok(())
    }

    /// Deletes a key. Deleting an absent key succeeds.
    pub fn delete(&mut self, bucket: impl AsRef<[u8]>, key: impl AsRef<[u8]>) -> Result<()> {
        let bucket = bucket.as_ref();
        if !self.bucket_will_exist(bucket) {
            return Err(Error::BucketNotFound);
        }
        self.operations
            .push((DELETE, bucket.to_vec(), key.as_ref().to_vec(), Vec::new()));
        Ok(())
    }

    fn bucket_will_exist(&self, name: &[u8]) -> bool {
        self.bucket_changes
            .get(name)
            .copied()
            .unwrap_or_else(|| bucket_exists(self.state, name))
    }
}

fn map(file: &File) -> Result<Mmap> {
    // SAFETY: mappings are read-only; `inspace` never truncates or modifies bytes
    // covered by a live mapping. Commits only append, sync, create a larger map,
    // and replace the old map while holding the exclusive state lock.
    Ok(unsafe { MmapOptions::new().map(file)? })
}

fn recover(state: &mut State) -> Result<()> {
    let mut cursor = FILE_HEADER_LEN;
    loop {
        match parse_transaction(&state.mmap, cursor)? {
            Some(transaction) => {
                if transaction.txid != state.txid + 1 {
                    return Err(Error::Corrupt("non-sequential transaction id"));
                }
                apply_records(state, &transaction.records)?;
                state.txid = transaction.txid;
                state.valid_len = transaction.end;
                cursor = transaction.end;
            }
            None => return Ok(()),
        }
    }
}

fn apply_records(state: &mut State, records: &[Record]) -> Result<()> {
    for record in records {
        let bucket = Slice {
            offset: record.bucket_offset,
            len: record.bucket_len,
        };
        let key = Slice {
            offset: record.key_offset,
            len: record.key_len,
        };
        let value = Slice {
            offset: record.value_offset,
            len: record.value_len,
        };
        match record.kind {
            CREATE_BUCKET => insert_bucket(state, bucket),
            DELETE_BUCKET => {
                let bucket_bytes = bucket.get(&state.mmap).to_vec();
                remove_bucket(state, &bucket_bytes);
                remove_bucket_entries(state, &bucket_bytes);
            }
            PUT => insert_entry(state, Entry { bucket, key, value }),
            DELETE => {
                let bucket_bytes = bucket.get(&state.mmap).to_vec();
                let key_bytes = key.get(&state.mmap).to_vec();
                remove_entry(state, &bucket_bytes, &key_bytes);
            }
            _ => return Err(Error::Corrupt("unknown record kind")),
        }
    }
    Ok(())
}

fn validate_operations(state: &State, operations: &[Operation]) -> Result<()> {
    let mut existence = HashMap::<Vec<u8>, bool>::new();
    for (kind, bucket, _, _) in operations {
        let exists = match existence.get(bucket.as_slice()) {
            Some(exists) => *exists,
            None => {
                let exists = bucket_exists(state, bucket);
                existence.insert(bucket.clone(), exists);
                exists
            }
        };
        match *kind {
            CREATE_BUCKET if exists => return Err(Error::BucketExists),
            CREATE_BUCKET => {
                existence.insert(bucket.clone(), true);
            }
            DELETE_BUCKET if !exists => return Err(Error::BucketNotFound),
            DELETE_BUCKET => {
                existence.insert(bucket.clone(), false);
            }
            PUT | DELETE if !exists => return Err(Error::BucketNotFound),
            PUT | DELETE => {}
            _ => return Err(Error::Corrupt("unknown staged operation")),
        }
    }
    Ok(())
}

fn insert_bucket(state: &mut State, bucket: Slice) {
    let hash = hash(bucket.get(&state.mmap));
    let mmap = &state.mmap;
    let list = state.buckets.entry(hash).or_default();
    list.retain(|existing| existing.get(mmap) != bucket.get(mmap));
    list.push(bucket);
}

fn remove_bucket(state: &mut State, bucket: &[u8]) {
    if let Some(list) = state.buckets.get_mut(&hash(bucket)) {
        let mmap = &state.mmap;
        list.retain(|existing| existing.get(mmap) != bucket);
    }
}

fn insert_entry(state: &mut State, entry: Entry) {
    let bucket = entry.bucket.get(&state.mmap);
    let key = entry.key.get(&state.mmap);
    let combined_hash = pair_hash(bucket, key);
    let mmap = &state.mmap;
    let list = state.entries.entry(combined_hash).or_default();
    list.retain(|existing| existing.bucket.get(mmap) != bucket || existing.key.get(mmap) != key);
    list.push(entry);
}

fn remove_entry(state: &mut State, bucket: &[u8], key: &[u8]) {
    if let Some(list) = state.entries.get_mut(&pair_hash(bucket, key)) {
        let mmap = &state.mmap;
        list.retain(|entry| entry.bucket.get(mmap) != bucket || entry.key.get(mmap) != key);
    }
}

fn remove_bucket_entries(state: &mut State, bucket: &[u8]) {
    let mmap = &state.mmap;
    state.entries.retain(|_, list| {
        list.retain(|entry| entry.bucket.get(mmap) != bucket);
        !list.is_empty()
    });
}

fn bucket_exists(state: &State, bucket: &[u8]) -> bool {
    find_bucket(state, bucket).is_some()
}

fn find_bucket(state: &State, bucket: &[u8]) -> Option<Slice> {
    state.buckets.get(&hash(bucket)).and_then(|list| {
        list.iter()
            .copied()
            .find(|stored| stored.get(&state.mmap) == bucket)
    })
}

fn find_entry<'a>(state: &'a State, bucket: &[u8], key: &[u8]) -> Option<&'a Entry> {
    state.entries.get(&pair_hash(bucket, key)).and_then(|list| {
        list.iter().find(|entry| {
            entry.bucket.get(&state.mmap) == bucket && entry.key.get(&state.mmap) == key
        })
    })
}

fn hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn pair_hash(bucket: &[u8], key: &[u8]) -> u64 {
    let mut combined = hash(bucket);
    combined ^= 0xff;
    combined = combined.wrapping_mul(0x0000_0100_0000_01b3);
    for byte in key {
        combined ^= u64::from(*byte);
        combined = combined.wrapping_mul(0x0000_0100_0000_01b3);
    }
    combined
}
