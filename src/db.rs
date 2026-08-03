use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use fs4::FileExt;
use memmap2::{Mmap, MmapOptions};

use crate::format::{
    CREATE_BUCKET, DELETE, DELETE_BUCKET, FILE_HEADER_LEN, Operation, PUT, Record,
    encode_transaction, file_header, parse_transaction, validate_file_header,
};
use crate::{BucketName, Cursor, Data, Error, KVPair, Range, Result};

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
    next_ints: HashMap<Vec<u8>, u64>,
    txid: u64,
    valid_len: usize,
}

/// A single-file, memory-mapped database.
#[derive(Clone)]
pub struct Database {
    inner: Arc<Inner>,
}

struct Inner {
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
        FileExt::lock(&file)?;
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
            next_ints: HashMap::new(),
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
                next_ints: HashMap::new(),
                txid: 0,
                valid_len: FILE_HEADER_LEN,
            };
            recover(&mut repaired)?;
            state = repaired;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                file,
                state: RwLock::new(state),
            }),
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
        (&self.inner.file).seek(SeekFrom::Start(start as u64))?;
        (&self.inner.file).write_all(&encoded)?;
        self.inner.file.sync_data()?;

        let new_mmap = map(&self.inner.file)?;
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

    pub fn check(&self) -> Result<()> {
        let state = self.read_state();
        let mmap = map(&self.inner.file)?;
        validate_file_header(&mmap)?;
        let mut checked = State {
            mmap,
            buckets: HashMap::new(),
            entries: HashMap::new(),
            next_ints: HashMap::new(),
            txid: 0,
            valid_len: FILE_HEADER_LEN,
        };
        recover(&mut checked)?;
        if checked.valid_len != checked.mmap.len()
            || checked.txid != state.txid
            || checked.buckets.values().map(Vec::len).sum::<usize>()
                != state.buckets.values().map(Vec::len).sum::<usize>()
            || checked.entries.values().map(Vec::len).sum::<usize>()
                != state.entries.values().map(Vec::len).sum::<usize>()
        {
            return Err(Error::Corrupt("state does not match committed data"));
        }
        Ok(())
    }

    fn read_state(&self) -> RwLockReadGuard<'_, State> {
        self.inner
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, State> {
        self.inner
            .state
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
        let path = root_path(name)?;
        find_bucket(&self.state, &path).ok_or(Error::BucketNotFound)?;
        Ok(Bucket {
            state: &self.state,
            path,
        })
    }

    pub fn buckets(&self) -> impl Iterator<Item = (BucketName<'_>, Bucket<'_>)> {
        direct_buckets(&self.state, &[]).into_iter()
    }
}

/// A read-only view of a bucket.
pub struct Bucket<'tx> {
    state: &'tx State,
    path: Vec<u8>,
}

impl<'tx> Bucket<'tx> {
    /// Gets an entry without copying it out of the memory map.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<Data<'tx>> {
        let key = key.as_ref();
        let child = child_path(&self.path, key).ok()?;
        if let Some(stored) = find_bucket(self.state, &child) {
            let path = stored.get(&self.state.mmap);
            return Some(Data::Bucket(BucketName::new(path_name(path)?)));
        }
        self.get_kv(key).map(Data::KeyValue)
    }

    pub fn get_kv(&self, key: impl AsRef<[u8]>) -> Option<KVPair<'tx>> {
        find_entry(self.state, &self.path, key.as_ref()).map(|entry| {
            KVPair::new(
                entry.key.get(&self.state.mmap),
                entry.value.get(&self.state.mmap),
            )
        })
    }

    /// Returns true when this bucket contains `key`.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    pub fn cursor(&self) -> Cursor<'tx> {
        let mut items = Vec::new();
        for (name, _) in direct_buckets(self.state, &self.path) {
            items.push(Data::Bucket(name));
        }
        for entries in self.state.entries.values() {
            for entry in entries {
                if entry.bucket.get(&self.state.mmap) == self.path {
                    items.push(Data::KeyValue(KVPair::new(
                        entry.key.get(&self.state.mmap),
                        entry.value.get(&self.state.mmap),
                    )));
                }
            }
        }
        Cursor::new(items)
    }

    pub fn get_bucket(&self, name: impl AsRef<[u8]>) -> Result<Bucket<'tx>> {
        let path = child_path(&self.path, name.as_ref())?;
        find_bucket(self.state, &path).ok_or(Error::BucketNotFound)?;
        Ok(Bucket {
            state: self.state,
            path,
        })
    }

    pub fn buckets(&self) -> impl Iterator<Item = (BucketName<'tx>, Bucket<'tx>)> {
        direct_buckets(self.state, &self.path).into_iter()
    }

    pub fn kv_pairs(&self) -> impl Iterator<Item = KVPair<'tx>> {
        self.cursor().filter_map(|entry| match entry {
            Data::KeyValue(pair) => Some(pair),
            Data::Bucket(_) => None,
        })
    }

    pub fn range<'a, R>(&self, bounds: R) -> Range<'tx>
    where
        R: RangeBounds<&'a [u8]>,
    {
        let items = self
            .cursor()
            .filter(|entry| {
                let key = entry.key();
                let after_start = match bounds.start_bound() {
                    Bound::Included(start) => key >= *start,
                    Bound::Excluded(start) => key > *start,
                    Bound::Unbounded => true,
                };
                let before_end = match bounds.end_bound() {
                    Bound::Included(end) => key <= *end,
                    Bound::Excluded(end) => key < *end,
                    Bound::Unbounded => true,
                };
                after_start && before_end
            })
            .collect();
        Range::new(items)
    }

    pub fn next_int(&self) -> u64 {
        self.state.next_ints.get(&self.path).copied().unwrap_or(0)
    }
}

impl<'tx> IntoIterator for Bucket<'tx> {
    type Item = Data<'tx>;
    type IntoIter = Cursor<'tx>;

    fn into_iter(self) -> Self::IntoIter {
        self.cursor()
    }
}

/// A buffered write transaction.
pub struct WriteTransaction<'db> {
    state: &'db State,
    operations: Vec<Operation>,
    bucket_changes: HashMap<Vec<u8>, bool>,
}

impl<'db> WriteTransaction<'db> {
    /// Creates a bucket.
    pub fn create_bucket(&mut self, name: impl AsRef<[u8]>) -> Result<WriteBucket<'_, 'db>> {
        let path = root_path(name.as_ref())?;
        if self.bucket_will_exist(&path) {
            return Err(Error::BucketExists);
        }
        self.operations
            .push((CREATE_BUCKET, path.clone(), Vec::new(), Vec::new()));
        self.bucket_changes.insert(path.clone(), true);
        Ok(WriteBucket { tx: self, path })
    }

    /// Deletes a bucket and all of its keys.
    pub fn delete_bucket(&mut self, name: impl AsRef<[u8]>) -> Result<()> {
        let path = root_path(name.as_ref())?;
        if !self.bucket_will_exist(&path) {
            return Err(Error::BucketNotFound);
        }
        self.operations
            .push((DELETE_BUCKET, path.clone(), Vec::new(), Vec::new()));
        self.bucket_changes.insert(path, false);
        Ok(())
    }

    pub fn bucket(&mut self, name: impl AsRef<[u8]>) -> Result<WriteBucket<'_, 'db>> {
        let path = root_path(name.as_ref())?;
        if !self.bucket_will_exist(&path) {
            return Err(Error::BucketNotFound);
        }
        Ok(WriteBucket { tx: self, path })
    }

    pub fn get_or_create_bucket(&mut self, name: impl AsRef<[u8]>) -> Result<WriteBucket<'_, 'db>> {
        let path = root_path(name.as_ref())?;
        if !self.bucket_will_exist(&path) {
            self.operations
                .push((CREATE_BUCKET, path.clone(), Vec::new(), Vec::new()));
            self.bucket_changes.insert(path.clone(), true);
        }
        Ok(WriteBucket { tx: self, path })
    }

    /// Inserts or replaces a key/value pair.
    pub fn put(
        &mut self,
        bucket: impl AsRef<[u8]>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let bucket = root_path(bucket.as_ref())?;
        if !self.bucket_will_exist(&bucket) {
            return Err(Error::BucketNotFound);
        }
        let key = key.as_ref();
        if self.bucket_will_exist(&child_path(&bucket, key)?) {
            return Err(Error::IncompatibleValue);
        }
        self.operations
            .push((PUT, bucket, key.to_vec(), value.as_ref().to_vec()));
        Ok(())
    }

    /// Deletes a key.
    pub fn delete(&mut self, bucket: impl AsRef<[u8]>, key: impl AsRef<[u8]>) -> Result<()> {
        let bucket = root_path(bucket.as_ref())?;
        if !self.bucket_will_exist(&bucket) {
            return Err(Error::BucketNotFound);
        }
        let key = key.as_ref();
        if self.bucket_will_exist(&child_path(&bucket, key)?) {
            return Err(Error::IncompatibleValue);
        }
        if !self.key_will_exist(&bucket, key) {
            return Err(Error::KeyValueMissing);
        }
        self.operations
            .push((DELETE, bucket, key.to_vec(), Vec::new()));
        Ok(())
    }

    fn bucket_will_exist(&self, name: &[u8]) -> bool {
        if let Some(exists) = self.bucket_changes.get(name) {
            return *exists;
        }
        if self
            .bucket_changes
            .iter()
            .any(|(path, exists)| !exists && name.starts_with(path))
        {
            return false;
        }
        bucket_exists(self.state, name)
    }

    fn key_will_exist(&self, bucket: &[u8], key: &[u8]) -> bool {
        for (kind, operation_bucket, operation_key, _) in self.operations.iter().rev() {
            if *kind == DELETE_BUCKET && bucket.starts_with(operation_bucket) {
                return false;
            }
            if operation_bucket == bucket && operation_key == key {
                return *kind == PUT;
            }
        }
        find_entry(self.state, bucket, key).is_some()
    }

    fn staged_next_int(&self, bucket: &[u8]) -> u64 {
        let reset = self
            .operations
            .iter()
            .rposition(|(kind, path, _, _)| *kind == CREATE_BUCKET && path == bucket);
        let mut next = if reset.is_some() {
            0
        } else {
            self.state.next_ints.get(bucket).copied().unwrap_or(0)
        };
        let mut keys = HashMap::<Vec<u8>, bool>::new();
        let start = reset.map_or(0, |index| index + 1);
        for (kind, path, key, _) in &self.operations[start..] {
            if *kind == CREATE_BUCKET && parent_path(path) == Some(bucket) {
                next = next.saturating_add(1);
            } else if path == bucket && matches!(*kind, PUT | DELETE) {
                let exists = keys
                    .get(key.as_slice())
                    .copied()
                    .unwrap_or_else(|| find_entry(self.state, bucket, key).is_some());
                if *kind == PUT && !exists {
                    next = next.saturating_add(1);
                }
                keys.insert(key.clone(), *kind == PUT);
            }
        }
        next
    }
}

pub struct WriteBucket<'tx, 'db> {
    tx: &'tx mut WriteTransaction<'db>,
    path: Vec<u8>,
}

impl<'tx, 'db> WriteBucket<'tx, 'db> {
    pub fn next_int(&self) -> u64 {
        self.tx.staged_next_int(&self.path)
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        if self.tx.bucket_will_exist(&child_path(&self.path, key)?) {
            return Err(Error::IncompatibleValue);
        }
        self.tx.operations.push((
            PUT,
            self.path.clone(),
            key.to_vec(),
            value.as_ref().to_vec(),
        ));
        Ok(())
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        if self.tx.bucket_will_exist(&child_path(&self.path, key)?) {
            return Err(Error::IncompatibleValue);
        }
        if !self.tx.key_will_exist(&self.path, key) {
            return Err(Error::KeyValueMissing);
        }
        self.tx
            .operations
            .push((DELETE, self.path.clone(), key.to_vec(), Vec::new()));
        Ok(())
    }

    pub fn create_bucket(&mut self, name: impl AsRef<[u8]>) -> Result<WriteBucket<'_, 'db>> {
        let path = child_path(&self.path, name.as_ref())?;
        if self.tx.bucket_will_exist(&path) {
            return Err(Error::BucketExists);
        }
        if self.tx.key_will_exist(&self.path, name.as_ref()) {
            return Err(Error::IncompatibleValue);
        }
        self.tx
            .operations
            .push((CREATE_BUCKET, path.clone(), Vec::new(), Vec::new()));
        self.tx.bucket_changes.insert(path.clone(), true);
        Ok(WriteBucket { tx: self.tx, path })
    }

    pub fn get_bucket(&mut self, name: impl AsRef<[u8]>) -> Result<WriteBucket<'_, 'db>> {
        let path = child_path(&self.path, name.as_ref())?;
        if !self.tx.bucket_will_exist(&path) {
            if self.tx.key_will_exist(&self.path, name.as_ref()) {
                return Err(Error::IncompatibleValue);
            }
            return Err(Error::BucketNotFound);
        }
        Ok(WriteBucket { tx: self.tx, path })
    }

    pub fn get_or_create_bucket(&mut self, name: impl AsRef<[u8]>) -> Result<WriteBucket<'_, 'db>> {
        let path = child_path(&self.path, name.as_ref())?;
        if !self.tx.bucket_will_exist(&path) {
            if self.tx.key_will_exist(&self.path, name.as_ref()) {
                return Err(Error::IncompatibleValue);
            }
            self.tx
                .operations
                .push((CREATE_BUCKET, path.clone(), Vec::new(), Vec::new()));
            self.tx.bucket_changes.insert(path.clone(), true);
        }
        Ok(WriteBucket { tx: self.tx, path })
    }

    pub fn delete_bucket(&mut self, name: impl AsRef<[u8]>) -> Result<()> {
        let path = child_path(&self.path, name.as_ref())?;
        if !self.tx.bucket_will_exist(&path) {
            return Err(Error::BucketNotFound);
        }
        self.tx
            .operations
            .push((DELETE_BUCKET, path.clone(), Vec::new(), Vec::new()));
        self.tx.bucket_changes.insert(path, false);
        Ok(())
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
            CREATE_BUCKET => {
                let path = bucket.get(&state.mmap).to_vec();
                if bucket_exists(state, &path) {
                    return Err(Error::Corrupt("bucket created twice"));
                }
                let parent = parent_path(&path).ok_or(Error::Corrupt("invalid bucket path"))?;
                let name = path_name(&path).ok_or(Error::Corrupt("invalid bucket path"))?;
                if !parent.is_empty() {
                    if !bucket_exists(state, parent) {
                        return Err(Error::Corrupt("bucket parent is missing"));
                    }
                    if find_entry(state, parent, name).is_some() {
                        return Err(Error::Corrupt("bucket conflicts with key"));
                    }
                    *state.next_ints.entry(parent.to_vec()).or_default() += 1;
                }
                insert_bucket(state, bucket);
                state.next_ints.insert(path, 0);
            }
            DELETE_BUCKET => {
                let bucket_bytes = bucket.get(&state.mmap).to_vec();
                if !bucket_exists(state, &bucket_bytes) {
                    return Err(Error::Corrupt("deleted bucket is missing"));
                }
                remove_bucket(state, &bucket_bytes);
                remove_bucket_entries(state, &bucket_bytes);
                state
                    .next_ints
                    .retain(|path, _| !path.starts_with(&bucket_bytes));
            }
            PUT => {
                let bucket_bytes = bucket.get(&state.mmap).to_vec();
                let key_bytes = key.get(&state.mmap);
                if !bucket_exists(state, &bucket_bytes) {
                    return Err(Error::Corrupt("key bucket is missing"));
                }
                let child = child_path(&bucket_bytes, key_bytes)?;
                if bucket_exists(state, &child) {
                    return Err(Error::Corrupt("key conflicts with bucket"));
                }
                if find_entry(state, &bucket_bytes, key_bytes).is_none() {
                    *state.next_ints.entry(bucket_bytes).or_default() += 1;
                }
                insert_entry(state, Entry { bucket, key, value });
            }
            DELETE => {
                let bucket_bytes = bucket.get(&state.mmap).to_vec();
                let key_bytes = key.get(&state.mmap).to_vec();
                if !bucket_exists(state, &bucket_bytes)
                    || find_entry(state, &bucket_bytes, &key_bytes).is_none()
                {
                    return Err(Error::Corrupt("deleted key is missing"));
                }
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
    let mmap = &state.mmap;
    state.buckets.retain(|_, list| {
        list.retain(|existing| !existing.get(mmap).starts_with(bucket));
        !list.is_empty()
    });
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
        list.retain(|entry| !entry.bucket.get(mmap).starts_with(bucket));
        !list.is_empty()
    });
}

fn root_path(name: &[u8]) -> Result<Vec<u8>> {
    child_path(&[], name)
}

fn child_path(parent: &[u8], name: &[u8]) -> Result<Vec<u8>> {
    let len = u32::try_from(name.len()).map_err(|_| Error::TooLarge)?;
    let mut path = Vec::with_capacity(parent.len() + 4 + name.len());
    path.extend_from_slice(parent);
    path.extend_from_slice(&len.to_le_bytes());
    path.extend_from_slice(name);
    Ok(path)
}

fn path_name(path: &[u8]) -> Option<&[u8]> {
    let mut cursor = 0;
    let mut name = None;
    while cursor < path.len() {
        let end = cursor.checked_add(4)?;
        let len = u32::from_le_bytes(path.get(cursor..end)?.try_into().ok()?) as usize;
        cursor = end;
        let end = cursor.checked_add(len)?;
        name = Some(path.get(cursor..end)?);
        cursor = end;
    }
    name
}

fn parent_path(path: &[u8]) -> Option<&[u8]> {
    let mut cursor = 0;
    let mut previous = 0;
    while cursor < path.len() {
        previous = cursor;
        let end = cursor.checked_add(4)?;
        let len = u32::from_le_bytes(path.get(cursor..end)?.try_into().ok()?) as usize;
        cursor = end.checked_add(len)?;
        if cursor > path.len() {
            return None;
        }
    }
    (cursor == path.len()).then_some(&path[..previous])
}

fn direct_child_name<'a>(path: &'a [u8], parent: &[u8]) -> Option<&'a [u8]> {
    let suffix = path.strip_prefix(parent)?;
    if suffix.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes(suffix[..4].try_into().ok()?) as usize;
    if suffix.len() != 4 + len {
        return None;
    }
    Some(&suffix[4..])
}

fn direct_buckets<'a>(state: &'a State, parent: &[u8]) -> Vec<(BucketName<'a>, Bucket<'a>)> {
    let mut buckets = Vec::new();
    for paths in state.buckets.values() {
        for stored in paths {
            let path = stored.get(&state.mmap);
            if let Some(name) = direct_child_name(path, parent) {
                buckets.push((
                    BucketName::new(name),
                    Bucket {
                        state,
                        path: path.to_vec(),
                    },
                ));
            }
        }
    }
    buckets.sort_unstable_by(|left, right| left.0.name().cmp(right.0.name()));
    buckets
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
