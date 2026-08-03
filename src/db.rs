use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use fs4::FileExt;
use memmap2::{Mmap, MmapOptions};

use crate::page::{BuiltTree, Meta, Node, Record as PageRecord, build_tree};
use crate::{BucketName, Cursor, Data, Error, KVPair, Range, Result};

const PAGE_SIZE: usize = 4096;
const CREATE_BUCKET: u8 = 1;
const DELETE_BUCKET: u8 = 2;
const PUT: u8 = 3;
const DELETE: u8 = 4;
type Operation = (u8, Vec<u8>, Vec<u8>, Vec<u8>);

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
    meta: Meta,
    buckets: HashMap<u64, Vec<Slice>>,
    entries: HashMap<u64, Vec<Entry>>,
    next_ints: HashMap<Vec<u8>, u64>,
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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        FileExt::lock(&file)?;
        if file.metadata()?.len() == 0 {
            initialize(&file)?;
        }
        let mmap = map(&file)?;
        let state = load_state(mmap)?;
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
        let mut owned = OwnedState::from_state(&state);
        owned.apply(&tx.operations)?;
        let txid = state.meta.txid.checked_add(1).ok_or(Error::TooLarge)?;
        let records = owned.records()?;
        let tree = build_tree(
            &records,
            state.meta.page_size as usize,
            state.meta.high_water,
        )?;
        let meta = Meta {
            page_size: state.meta.page_size,
            txid,
            root: tree.root,
            high_water: tree.high_water,
            freelist: 0,
        };
        let empty = MmapOptions::new().len(1).map_anon()?.make_read_only()?;
        state.mmap = empty;
        write_tree(&self.inner.file, &tree, meta)?;
        *state = load_state(map(&self.inner.file)?)?;
        Ok(result)
    }

    /// Returns the last committed transaction identifier.
    pub fn transaction_id(&self) -> u64 {
        self.read_state().meta.txid
    }

    pub fn check(&self) -> Result<()> {
        let state = self.read_state();
        let checked = load_state(map(&self.inner.file)?)?;
        if checked.meta != state.meta
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
    // The exclusive state lock removes the old map before committed pages change.
    Ok(unsafe { MmapOptions::new().map(file)? })
}

fn initialize(file: &File) -> Result<()> {
    let tree = build_tree(&[], PAGE_SIZE, 2)?;
    let meta = Meta {
        page_size: PAGE_SIZE as u32,
        txid: 0,
        root: tree.root,
        high_water: tree.high_water,
        freelist: 0,
    };
    file.set_len(meta.high_water * PAGE_SIZE as u64)?;
    for (page, bytes) in &tree.pages {
        write_at(file, page * PAGE_SIZE as u64, bytes)?;
    }
    let encoded = meta.encode()?;
    write_at(file, 0, &encoded)?;
    write_at(file, PAGE_SIZE as u64, &encoded)?;
    file.sync_data()?;
    Ok(())
}

fn write_tree(file: &File, tree: &BuiltTree, meta: Meta) -> Result<()> {
    file.set_len(meta.high_water * u64::from(meta.page_size))?;
    for (page, bytes) in &tree.pages {
        write_at(file, page * u64::from(meta.page_size), bytes)?;
    }
    file.sync_data()?;
    let slot = meta.txid & 1;
    write_at(file, slot * u64::from(meta.page_size), &meta.encode()?)?;
    file.sync_data()?;
    Ok(())
}

fn write_at(file: &File, offset: u64, bytes: &[u8]) -> Result<()> {
    let mut file = file;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)?;
    Ok(())
}

fn load_state(mmap: Mmap) -> Result<State> {
    let meta = select_meta(&mmap)?;
    let required = meta
        .high_water
        .checked_mul(u64::from(meta.page_size))
        .ok_or(Error::Corrupt("file length overflow"))?;
    if required > mmap.len() as u64 {
        return Err(Error::Corrupt("committed pages exceed file"));
    }
    let mut leaves = Vec::new();
    let mut visited = BTreeSet::new();
    walk_tree(&mmap, meta, meta.root, &mut visited, &mut leaves)?;
    let mut loaded = Vec::with_capacity(leaves.len());
    let mut previous: Option<Vec<u8>> = None;
    for (key_slice, value_slice) in leaves {
        let key = key_slice.get(&mmap);
        let value = value_slice.get(&mmap);
        if previous.as_deref().is_some_and(|old| old >= key) {
            return Err(Error::Corrupt("tree keys are not ordered"));
        }
        previous = Some(key.to_vec());
        match key.first() {
            Some(0) if value.len() == 8 => {
                let path = Slice {
                    offset: key_slice.offset + 1,
                    len: key_slice.len - 1,
                };
                let next = u64::from_le_bytes(value.try_into().expect("length checked"));
                loaded.push(LoadedRecord::Bucket(path, next));
            }
            Some(1) if key.len() >= 5 => {
                let path_len =
                    u32::from_le_bytes(key[1..5].try_into().expect("length checked")) as usize;
                if key.len() < 5 + path_len {
                    return Err(Error::Corrupt("invalid key path length"));
                }
                loaded.push(LoadedRecord::Entry(Entry {
                    bucket: Slice {
                        offset: key_slice.offset + 5,
                        len: path_len,
                    },
                    key: Slice {
                        offset: key_slice.offset + 5 + path_len,
                        len: key.len() - 5 - path_len,
                    },
                    value: value_slice,
                }));
            }
            _ => return Err(Error::Corrupt("invalid tree record")),
        }
    }
    let mut state = State {
        mmap,
        meta,
        buckets: HashMap::new(),
        entries: HashMap::new(),
        next_ints: HashMap::new(),
    };
    for record in loaded {
        match record {
            LoadedRecord::Bucket(path, next) => {
                let bytes = path.get(&state.mmap);
                if parent_path(bytes).is_none() || bucket_exists(&state, bytes) {
                    return Err(Error::Corrupt("invalid bucket record"));
                }
                state.next_ints.insert(bytes.to_vec(), next);
                insert_bucket(&mut state, path);
            }
            LoadedRecord::Entry(entry) => {
                let bucket = entry.bucket.get(&state.mmap);
                let key = entry.key.get(&state.mmap);
                if !bucket_exists(&state, bucket) || find_entry(&state, bucket, key).is_some() {
                    return Err(Error::Corrupt("invalid key record"));
                }
                insert_entry(&mut state, entry);
            }
        }
    }
    validate_loaded_state(&state)?;
    Ok(state)
}

fn select_meta(mmap: &Mmap) -> Result<Meta> {
    if mmap.len() < PAGE_SIZE * 2 {
        return Err(Error::Corrupt("meta pages are truncated"));
    }
    let first = Meta::decode(&mmap[..PAGE_SIZE]).ok();
    let second = Meta::decode(&mmap[PAGE_SIZE..PAGE_SIZE * 2]).ok();
    let meta = match (first, second) {
        (Some(left), Some(right)) => {
            if left.txid >= right.txid {
                left
            } else {
                right
            }
        }
        (Some(meta), None) | (None, Some(meta)) => meta,
        (None, None) => return Err(Error::Corrupt("both meta pages are invalid")),
    };
    if meta.page_size as usize != PAGE_SIZE {
        return Err(Error::Corrupt("unsupported page size"));
    }
    Ok(meta)
}

fn walk_tree(
    mmap: &Mmap,
    meta: Meta,
    page: u64,
    visited: &mut BTreeSet<u64>,
    leaves: &mut Vec<(Slice, Slice)>,
) -> Result<()> {
    if page < 2 || page >= meta.high_water || !visited.insert(page) {
        return Err(Error::Corrupt("invalid or repeated tree page"));
    }
    let offset = usize::try_from(page)
        .ok()
        .and_then(|page| page.checked_mul(meta.page_size as usize))
        .ok_or(Error::Corrupt("page offset overflow"))?;
    let node = Node::decode(&mmap[offset..], meta.page_size as usize)?;
    if node.page() != page || page + node.span() as u64 > meta.high_water {
        return Err(Error::Corrupt("node page identity mismatch"));
    }
    for covered in page..page + node.span() as u64 {
        if covered != page && !visited.insert(covered) {
            return Err(Error::Corrupt("overlapping tree pages"));
        }
    }
    if node.is_leaf() {
        for (key, value) in node.leaf_records()? {
            leaves.push((slice_in_map(mmap, key)?, slice_in_map(mmap, value)?));
        }
    } else {
        let branches = node.branches()?;
        if branches.is_empty() {
            return Err(Error::Corrupt("empty branch node"));
        }
        for (_, child) in branches {
            walk_tree(mmap, meta, child, visited, leaves)?;
        }
    }
    Ok(())
}

enum LoadedRecord {
    Bucket(Slice, u64),
    Entry(Entry),
}

fn slice_in_map(mmap: &Mmap, bytes: &[u8]) -> Result<Slice> {
    let base = mmap.as_ptr() as usize;
    let start = bytes.as_ptr() as usize;
    let offset = start
        .checked_sub(base)
        .ok_or(Error::Corrupt("slice is outside mapping"))?;
    if offset + bytes.len() > mmap.len() {
        return Err(Error::Corrupt("slice is outside mapping"));
    }
    Ok(Slice {
        offset,
        len: bytes.len(),
    })
}

fn validate_loaded_state(state: &State) -> Result<()> {
    for paths in state.buckets.values() {
        for path in paths {
            let path = path.get(&state.mmap);
            let parent = parent_path(path).ok_or(Error::Corrupt("invalid bucket path"))?;
            if !parent.is_empty() && !bucket_exists(state, parent) {
                return Err(Error::Corrupt("bucket parent is missing"));
            }
        }
    }
    for entries in state.entries.values() {
        for entry in entries {
            let bucket = entry.bucket.get(&state.mmap);
            let key = entry.key.get(&state.mmap);
            if bucket_exists(state, &child_path(bucket, key)?) {
                return Err(Error::Corrupt("key conflicts with bucket"));
            }
        }
    }
    Ok(())
}

struct OwnedState {
    buckets: BTreeMap<Vec<u8>, u64>,
    entries: BTreeMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
}

impl OwnedState {
    fn from_state(state: &State) -> Self {
        let mut buckets = BTreeMap::new();
        for paths in state.buckets.values() {
            for path in paths {
                let path = path.get(&state.mmap).to_vec();
                let next = state.next_ints.get(&path).copied().unwrap_or(0);
                buckets.insert(path, next);
            }
        }
        let mut entries = BTreeMap::new();
        for list in state.entries.values() {
            for entry in list {
                entries.insert(
                    (
                        entry.bucket.get(&state.mmap).to_vec(),
                        entry.key.get(&state.mmap).to_vec(),
                    ),
                    entry.value.get(&state.mmap).to_vec(),
                );
            }
        }
        Self { buckets, entries }
    }

    fn apply(&mut self, operations: &[Operation]) -> Result<()> {
        for (kind, bucket, key, value) in operations {
            match *kind {
                CREATE_BUCKET => {
                    if self.buckets.contains_key(bucket) {
                        return Err(Error::BucketExists);
                    }
                    if let Some(parent) = parent_path(bucket)
                        && !parent.is_empty()
                    {
                        let next = self.buckets.get_mut(parent).ok_or(Error::BucketNotFound)?;
                        *next = next.saturating_add(1);
                    }
                    self.buckets.insert(bucket.clone(), 0);
                }
                DELETE_BUCKET => {
                    if self.buckets.remove(bucket).is_none() {
                        return Err(Error::BucketNotFound);
                    }
                    self.buckets.retain(|path, _| !path.starts_with(bucket));
                    self.entries
                        .retain(|(path, _), _| !path.starts_with(bucket));
                }
                PUT => {
                    if !self.buckets.contains_key(bucket) {
                        return Err(Error::BucketNotFound);
                    }
                    if self
                        .entries
                        .insert((bucket.clone(), key.clone()), value.clone())
                        .is_none()
                    {
                        *self.buckets.get_mut(bucket).expect("checked") += 1;
                    }
                }
                DELETE => {
                    if self
                        .entries
                        .remove(&(bucket.clone(), key.clone()))
                        .is_none()
                    {
                        return Err(Error::KeyValueMissing);
                    }
                }
                _ => return Err(Error::Corrupt("unknown staged operation")),
            }
        }
        Ok(())
    }

    fn records(&self) -> Result<Vec<PageRecord>> {
        let mut records = Vec::with_capacity(self.buckets.len() + self.entries.len());
        for (path, next) in &self.buckets {
            let mut key = Vec::with_capacity(1 + path.len());
            key.push(0);
            key.extend_from_slice(path);
            records.push(PageRecord {
                key,
                value: next.to_le_bytes().to_vec(),
            });
        }
        for ((path, entry_key), value) in &self.entries {
            let path_len = u32::try_from(path.len()).map_err(|_| Error::TooLarge)?;
            let mut key = Vec::with_capacity(5 + path.len() + entry_key.len());
            key.push(1);
            key.extend_from_slice(&path_len.to_le_bytes());
            key.extend_from_slice(path);
            key.extend_from_slice(entry_key);
            records.push(PageRecord {
                key,
                value: value.clone(),
            });
        }
        Ok(records)
    }
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

fn insert_entry(state: &mut State, entry: Entry) {
    let bucket = entry.bucket.get(&state.mmap);
    let key = entry.key.get(&state.mmap);
    let combined_hash = pair_hash(bucket, key);
    let mmap = &state.mmap;
    let list = state.entries.entry(combined_hash).or_default();
    list.retain(|existing| existing.bucket.get(mmap) != bucket || existing.key.get(mmap) != key);
    list.push(entry);
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
