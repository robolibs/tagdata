use std::{
    cell::RefCell,
    collections::HashSet,
    fs::File,
    io::{Seek, SeekFrom, Write},
    marker::PhantomData,
    rc::Rc,
    sync::MutexGuard,
};

use fs4::FileExt;

use crate::{
    BucketName,
    bucket::{Bucket, BucketMeta, InnerBucket},
    bytes::ToBytes,
    changes::{ChangeOperation, ChangeTracker},
    coordination::{GateGuard, ReaderRegistration},
    cursor::ToBuckets,
    db::{DB, FORMAT_VERSION},
    errors::{Error, Result},
    freelist::TxFreelist,
    meta::Meta,
    node::Node,
    page::{Page, PageID, Pages, seal_block},
    support::failpoints,
};

pub(crate) struct WriteGuard<'tx> {
    file: MutexGuard<'tx, File>,
    _gate: GateGuard<'tx>,
}

impl<'tx> WriteGuard<'tx> {
    fn new(db: &'tx DB) -> Result<Self> {
        let coordination = db.inner.coordination.as_ref().unwrap();
        loop {
            let gate = coordination.exclusive_gate()?;
            let file = db.inner.file.lock()?;
            match FileExt::try_lock_exclusive(&*file) {
                Ok(()) => return Ok(Self { file, _gate: gate }),
                Err(error) if error.kind() == fs4::lock_contended_error().kind() => {
                    drop(file);
                    drop(gate);
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn try_new(db: &'tx DB) -> Result<Option<Self>> {
        let coordination = db.inner.coordination.as_ref().unwrap();
        let Some(gate) = coordination.try_exclusive_gate()? else {
            return Ok(None);
        };
        let file = match db.inner.file.try_lock() {
            Ok(file) => file,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(Error::Sync("lock poisoned"));
            }
        };
        match FileExt::try_lock_exclusive(&*file) {
            Ok(()) => Ok(Some(Self { file, _gate: gate })),
            Err(error) if error.kind() == fs4::lock_contended_error().kind() => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&*self.file);
    }
}

pub(crate) enum TxLock<'tx> {
    Rw(WriteGuard<'tx>),
    Ro {
        _registration: Option<ReaderRegistration>,
    },
}

impl<'tx> TxLock<'tx> {
    fn writable(&self) -> bool {
        match self {
            Self::Rw(_) => true,
            Self::Ro { .. } => false,
        }
    }
}

/// An isolated view of the database
///
/// Transactions are how you can interact with the database.
/// They are created from a [`DB`](struct.DB.html),
/// and can be read-only or writable<sup>1</sup> depending on the paramater you pass into the [`tx`](struct.DB.html#method.tx) method.
/// Transactions are completely isolated from each other, so a read-only transaction can expect the data to stay exactly the same for the life
/// of the transaction, regardless of how many changes are made in other transactions<sup>2</sup>.
///
/// There are four important methods. Check out their documentation for more details:
/// 1. [`get_bucket`](#method.get_bucket) retreives buckets from the root level. Available in read-only or writable transactions.
/// 2. [`create_bucket`](#method.create_bucket) makes new buckets at the root level. Available in writable transactions only.
/// 3. [`detete_bucket`](#method.delete_bucket) deletes a bucket (including all nested buckets) from the database. Available in writable transactions only.
/// 4. [`commit`](#method.commit) saves a writable transaction. Available in writable transactions.
///
/// Trying to use the methods that require writable transactions from a read-only transaction will result in an error. If you make edits in a writable transaction,
/// and you want to save them, you must call the [`commit`](#method.commit) method, otherwise when the transaction is dropped all your changes will be lost.
///
/// # Examples
///
/// ```no_run
/// use inspace::{DB, Data};
/// # use inspace::Error;
///
/// # fn main() -> Result<(), Error> {
/// # let db = DB::open("my.db")?;
/// // create a read-only transaction
/// let mut tx1 = db.tx(false)?;
/// // create a writable transcation
/// let mut tx2 = db.tx(true)?;
///
/// // create a new bucket in the writable transaction
/// tx2.create_bucket("new-bucket")?;
///
/// // the read-only transaction will not be able to see the new bucket
/// assert!(tx1.get_bucket("new-bucket").is_err());
///
/// // get a view of an existing bucket from both transactions
/// let mut b1 = tx1.get_bucket("existing-bucket")?;
/// let mut b2 = tx2.get_bucket("existing-bucket")?;
///
/// // make an edit to the bucket
/// b2.put("new-key", "new-value")?;
///
/// // the read-only transaction will not have this new key
/// assert_eq!(b1.get("new-key"), None);
/// // but it will be able to see data that already existed!
/// assert!(b1.get("existing-key").is_some());
///
/// # Ok(())
/// # }
/// ```
///
///
/// <sup>1</sup> There can only be a single writeable transaction at a time, so trying to open
/// two writable transactions on the same thread will deadlock.
///
/// <sup>2</sup> Keep in mind that long running read-only transactions will prevent the database from
/// reclaiming old pages and your database may increase in disk size quickly if you're writing lots of data,
/// so it's a good idea to keep transactions short.
pub struct Tx<'tx> {
    pub(crate) inner: RefCell<TxInner<'tx>>,
}

pub(crate) struct TxInner<'tx> {
    pub(crate) db: &'tx DB,
    pub(crate) lock: TxLock<'tx>,
    pub(crate) root: Rc<RefCell<InnerBucket<'tx>>>,
    pub(crate) meta: Meta,
    pub(crate) freelist: Rc<RefCell<TxFreelist>>,
    pub(crate) changes: Rc<RefCell<ChangeTracker>>,
    pages: Pages,
    num_freelist_pages: u64,
}

impl<'tx> Tx<'tx> {
    pub(crate) fn new(db: &'tx DB, writable: bool) -> Result<Tx<'tx>> {
        Ok(Self::new_impl(db, writable, WriteMode::Blocking)?.unwrap())
    }

    pub(crate) fn try_new_writable(db: &'tx DB) -> Result<Option<Tx<'tx>>> {
        Self::new_impl(db, true, WriteMode::Try)
    }

    pub(crate) fn new_writable_timeout(
        db: &'tx DB,
        timeout: std::time::Duration,
    ) -> Result<Tx<'tx>> {
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(std::time::Instant::now);
        Self::new_impl(db, true, WriteMode::Deadline(deadline))?.ok_or(Error::WriterTimeout)
    }

    fn new_impl(db: &'tx DB, writable: bool, mode: WriteMode) -> Result<Option<Tx<'tx>>> {
        if writable && db.inner.flags.read_only {
            return Err(Error::ReadOnlyDB);
        }

        let (lock, meta, freelist) = if writable {
            let lock = match mode {
                WriteMode::Blocking => WriteGuard::new(db)?,
                WriteMode::Try => {
                    let Some(lock) = WriteGuard::try_new(db)? else {
                        return Ok(None);
                    };
                    lock
                }
                WriteMode::Deadline(deadline) => loop {
                    if let Some(lock) = WriteGuard::try_new(db)? {
                        break lock;
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(Error::WriterTimeout);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(2));
                },
            };
            db.inner.refresh(&lock.file)?;
            db.inner.reload_freelist()?;
            let coordination = db.inner.coordination.as_ref().unwrap();
            let (_, oldest_reader) = coordination.readers()?;
            let mut freelist = db.inner.freelist.lock()?.clone();
            let mut meta = db.inner.meta()?;
            debug_assert!(meta.valid());
            meta.tx_id += 1;
            freelist.release(oldest_reader.unwrap_or(meta.tx_id));
            (TxLock::Rw(lock), meta, freelist)
        } else {
            let (meta, registration) = match &db.inner.coordination {
                Some(coordination) => {
                    let _gate = coordination.shared_gate()?;
                    {
                        let file = db.inner.file.lock()?;
                        db.inner.refresh(&file)?;
                    }
                    let meta = db.inner.meta()?;
                    let registration = coordination.register(meta.tx_id)?;
                    (meta, Some(registration))
                }
                None => (db.inner.meta()?, None),
            };
            debug_assert!(meta.valid());
            let mut open_ro_txs = db.inner.open_ro_txs.lock().unwrap();
            open_ro_txs.push(meta.tx_id);
            open_ro_txs.sort_unstable();
            let freelist = db.inner.freelist.lock()?.clone();
            (
                TxLock::Ro {
                    _registration: registration,
                },
                meta,
                freelist,
            )
        };
        let freelist = Rc::new(RefCell::new(TxFreelist::new(meta.clone(), freelist)));
        let changes = ChangeTracker::shared(writable);

        let data = db.inner.data.lock()?.clone();
        let pages = Pages::new(data, db.inner.pagesize);
        let num_freelist_pages = pages.validate(meta.freelist_page)?.overflow + 1;
        let root = InnerBucket::from_meta(meta.root, pages.clone());
        let root = Rc::new(RefCell::new(root));
        let inner = TxInner {
            db,
            lock,
            root,
            meta,
            freelist,
            changes,
            num_freelist_pages,
            pages,
        };
        Ok(Some(Tx {
            inner: RefCell::new(inner),
        }))
    }

    pub(crate) fn writable(&self) -> bool {
        self.inner.borrow().lock.writable()
    }

    /// Returns a reference to the root level bucket with the given name.
    ///
    /// # Errors
    ///
    /// Will return a [`BucketMissing`](enum.Error.html#variant.BucketMissing) error if the bucket does not exist,
    /// or an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket.
    ///
    /// In a read-only transaction, you will get an error when trying to use any of the bucket's methods that modify data.    
    pub fn get_bucket<'b, T: ToBytes<'tx>>(&'b self, name: T) -> Result<Bucket<'b, 'tx>> {
        let tx = self.inner.borrow();
        let name = name.to_bytes();
        let path = vec![name.as_ref().to_vec()];
        let mut root = tx.root.borrow_mut();
        let inner = root.get_bucket(&name)?;
        Ok(Bucket {
            inner,
            freelist: tx.freelist.clone(),
            writable: tx.lock.writable(),
            path,
            changes: tx.changes.clone(),
            _phantom: PhantomData,
        })
    }

    /// Creates a new bucket with the given name and returns a reference it.
    ///
    /// # Errors
    ///
    /// Will return a [`BucketExists`](enum.Error.html#variant.BucketExists) error if the bucket already exists,
    /// an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket,
    /// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
    pub fn create_bucket<'b, T: ToBytes<'tx>>(&'b self, name: T) -> Result<Bucket<'b, 'tx>> {
        let tx = self.inner.borrow();
        if !tx.lock.writable() {
            return Err(Error::ReadOnlyTx);
        }
        let name = name.to_bytes();
        let key = name.as_ref().to_vec();
        let mut root = tx.root.borrow_mut();
        let inner = root.create_bucket(name)?;
        tx.changes
            .borrow_mut()
            .record(&[], &key, ChangeOperation::BucketCreate);
        Ok(Bucket {
            inner,
            freelist: tx.freelist.clone(),
            writable: true,
            path: vec![key],
            changes: tx.changes.clone(),
            _phantom: PhantomData,
        })
    }

    /// Creates an existing root-level bucket with the given name if it does not already exist.
    /// Gets the existing bucket if it does exist.
    ///
    /// # Errors
    ///
    /// Will return an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket,
    /// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
    pub fn get_or_create_bucket<'b, T: ToBytes<'tx>>(&'b self, name: T) -> Result<Bucket<'b, 'tx>> {
        let tx = self.inner.borrow();
        if !tx.lock.writable() {
            return Err(Error::ReadOnlyTx);
        }
        let name = name.to_bytes();
        let key = name.as_ref().to_vec();
        let mut root = tx.root.borrow_mut();
        let existed = root.get_bucket(&name).is_ok();
        let inner = root.get_or_create_bucket(name)?;
        if !existed {
            tx.changes
                .borrow_mut()
                .record(&[], &key, ChangeOperation::BucketCreate);
        }
        Ok(Bucket {
            inner,
            freelist: tx.freelist.clone(),
            writable: true,
            path: vec![key],
            changes: tx.changes.clone(),
            _phantom: PhantomData,
        })
    }

    /// Deletes an existing root-level bucket with the given name
    ///
    /// # Errors
    ///
    /// Will return a [`BucketMissing`](enum.Error.html#variant.BucketMissing) error if the bucket does not exist,
    /// an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket,
    /// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
    pub fn delete_bucket<T: ToBytes<'tx>>(&self, key: T) -> Result<()> {
        let tx = self.inner.borrow();
        if !tx.lock.writable() {
            return Err(Error::ReadOnlyTx);
        }
        let key = key.to_bytes();
        let change_key = key.as_ref().to_vec();
        let freelist = tx.freelist.clone();
        let mut freelist = freelist.borrow_mut();
        let mut root = tx.root.borrow_mut();
        root.delete_bucket(key, &mut freelist)?;
        tx.changes
            .borrow_mut()
            .record(&[], &change_key, ChangeOperation::BucketDelete);
        Ok(())
    }

    /// Iterator over the root level buckets
    pub fn buckets<'b>(&'b self) -> impl Iterator<Item = (BucketName<'b, 'tx>, Bucket<'b, 'tx>)> {
        let tx = self.inner.borrow();
        let bucket = Bucket {
            inner: tx.root.clone(),
            freelist: tx.freelist.clone(),
            writable: tx.lock.writable(),
            path: Vec::new(),
            changes: tx.changes.clone(),
            _phantom: PhantomData,
        };
        bucket.cursor().to_buckets()
    }

    /// Writes the changes made in the writable transaction to the underlying file.
    /// Data pages are synced before the metadata page that publishes the transaction.
    /// The metadata page is then synced before this method returns.
    ///
    /// # Errors
    ///
    /// Will return an [`IOError`](enum.Error.html#variant.IOError) error if there are any io errors while writing to disk,
    /// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
    pub fn commit(self) -> Result<()> {
        if !self.writable() {
            return Err(Error::ReadOnlyTx);
        }
        let mut tx = self.inner.borrow_mut();
        let journal_changes = tx.changes.borrow().snapshot(tx.meta.tx_id);
        crate::journal::persist(&mut tx, &journal_changes)?;
        let freelist = tx.freelist.clone();
        let mut freelist = freelist.borrow_mut();
        let meta = {
            let mut root = tx.root.borrow_mut();
            root.rebalance(&mut freelist)?;
            root.spill(&mut freelist)?
        };
        tx.meta.root = meta;
        tx.write_data(&mut freelist)
    }

    pub(crate) fn check(&self) -> Result<()> {
        self.inner.borrow().check()
    }
}

#[derive(Clone, Copy)]
enum WriteMode {
    Blocking,
    Try,
    Deadline(std::time::Instant),
}

impl<'tx> TxInner<'tx> {
    fn write_data(&mut self, freelist: &mut TxFreelist) -> Result<()> {
        if let TxLock::Rw(lock) = &mut self.lock {
            let file = &mut *lock.file;
            // Write the freelist to a new page
            {
                freelist.free(self.meta.freelist_page, self.num_freelist_pages);
                let freelist_size = freelist.inner.size();
                let page = freelist.allocate(freelist_size)?;
                self.meta.freelist_page = page.id;
                page.page_type = Page::TYPE_FREELIST;
                let entries = freelist.inner.entries();
                page.count = entries.len() as u64;
                page.retired_pages_mut().copy_from_slice(&entries);
            }

            // Update our num_pages from the freelist now that we've allocated everything
            self.meta.num_pages = freelist.meta.num_pages;

            // Grow the file, if needed
            let required_size = self
                .meta
                .num_pages
                .checked_mul(self.db.inner.pagesize)
                .ok_or_else(|| Error::InvalidDB("required file size overflow".into()))?;
            if let Some(maximum) = self.db.inner.max_file_bytes
                && required_size > maximum
            {
                return Err(Error::CapacityExceeded {
                    required: required_size,
                    maximum,
                });
            }
            let current_size = file.metadata()?.len();
            if current_size < required_size {
                let size_diff = required_size - current_size;
                let increment = self.db.inner.growth_increment;
                let increments = size_diff.div_ceil(increment);
                let mut new_size = current_size
                    .checked_add(increments.saturating_mul(increment))
                    .ok_or_else(|| Error::InvalidDB("file growth overflow".into()))?;
                if let Some(maximum) = self.db.inner.max_file_bytes {
                    new_size = new_size.min(maximum);
                }
                let data = self.db.inner.resize(file, new_size)?;
                self.pages = Pages::new(data, self.db.inner.pagesize);
            }

            // write the data to the file
            {
                // freelist.pages is a BTreeMap so we're writing the pages in order to minmize
                // the random seeks.
                for (page_id, (ptr, size)) in freelist.pages.iter() {
                    let buf = unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr(), *size) };
                    seal_block(buf)?;
                    file.seek(SeekFrom::Start(self.db.inner.pagesize * page_id))?;
                    file.write_all(buf)?;
                }
            }

            failpoints::hit("after-data-write");
            file.flush()?;
            file.sync_all()?;
            failpoints::hit("after-data-sync");
        }
        if self.db.inner.flags.strict_mode {
            self.check()?;
        }
        if let TxLock::Rw(lock) = &mut self.lock {
            let file = &mut *lock.file;
            // write meta page to file
            {
                let mut buf = vec![0; self.db.inner.pagesize as usize];

                #[allow(clippy::cast_ptr_alignment)]
                let page = unsafe { &mut *(&mut buf[0] as *mut u8 as *mut Page) };
                let meta_page_id = u64::from(self.meta.meta_page == 0);
                page.id = meta_page_id;
                page.page_type = Page::TYPE_META;
                let m = page.meta_mut();
                m.meta_page = meta_page_id as u32;
                m.magic = self.meta.magic;
                m.version = FORMAT_VERSION;
                m.pagesize = self.meta.pagesize;
                m.root = self.meta.root;
                m.num_pages = self.meta.num_pages;
                m.freelist_page = self.meta.freelist_page;
                m.tx_id = self.meta.tx_id;
                m.hash = m.hash_self();
                seal_block(&mut buf)?;

                file.seek(SeekFrom::Start(self.db.inner.pagesize * meta_page_id))?;
                file.write_all(buf.as_slice())?;
            }

            failpoints::hit("after-meta-write");
            file.flush()?;
            file.sync_all()?;
            failpoints::hit("after-meta-sync");

            let mut lock = self.db.inner.freelist.lock()?;
            *lock = freelist.inner.clone();
            let written = freelist
                .pages
                .values()
                .map(|(_, size)| *size as u64)
                .sum::<u64>()
                + self.db.inner.pagesize;
            self.db
                .inner
                .committed_transactions
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.db
                .inner
                .bytes_written
                .fetch_add(written, std::sync::atomic::Ordering::Relaxed);
            let changes = self.changes.borrow_mut().finish(self.meta.tx_id);
            self.db.inner.watches.publish(changes);
            Ok(())
        } else {
            unreachable!()
        }
    }

    fn check(&self) -> Result<()> {
        let mut unused_pages: HashSet<PageID> = (2..self.meta.num_pages).collect();
        let mut page_stack = Vec::new();
        page_stack.push(self.meta.root.root_page);
        page_stack.push(self.meta.freelist_page);
        while let Some(page_id) = page_stack.pop() {
            // Make sure this page hasn't already been used
            if !unused_pages.remove(&page_id) {
                return Err(Error::InvalidDB(format!(
                    "Page {} missing from unused_pages",
                    page_id,
                )));
            }
            let page = self.pages.validate(page_id)?;
            // Make sure none of the overflow pages have been used
            for i in 0..page.overflow {
                let page_id = page_id + i + 1;
                if !unused_pages.remove(&page_id) {
                    return Err(Error::InvalidDB(format!(
                        "Overflow Page {} from missing from unused_pages",
                        page_id,
                    )));
                }
            }
            // Check the page type and explore all possible pages
            match page.page_type {
                Page::TYPE_BRANCH => {
                    let mut last: Option<&[u8]> = None;
                    for b in page.branch_elements().iter() {
                        // Make sure we visit every branch page
                        page_stack.push(b.page);
                        // and that the keys are in order
                        if let Some(last) = last
                            && last >= b.key()
                        {
                            return Err(Error::InvalidDB(format!(
                                "Branch page {} contains unsorted elements",
                                page_id
                            )));
                        }
                        last = Some(b.key());
                    }
                }
                Page::TYPE_LEAF => {
                    let mut last: Option<&[u8]> = None;
                    for (i, leaf) in page.leaf_elements().iter().enumerate() {
                        match leaf.node_type {
                            Node::TYPE_BUCKET => {
                                let meta: BucketMeta = leaf.value().into();
                                // Push all nested bucket pages onto the queue for exploration
                                page_stack.push(meta.root_page);
                            }
                            // Ignore data nodes since they don't point to more pages
                            Node::TYPE_DATA => (),
                            // If somehow it isn't a bucket or data, that's really bad...
                            _ => {
                                return Err(Error::InvalidDB(format!(
                                    "Page {} index {} has an invalid leaf node type {}",
                                    page_id, i, leaf.node_type,
                                )));
                            }
                        }
                        // Make sure all leaf elements are in order
                        if let Some(last) = last
                            && last >= leaf.key()
                        {
                            return Err(Error::InvalidDB(format!(
                                "Leaf page {} contains unsorted elements",
                                page_id
                            )));
                        }
                        last = Some(leaf.key());
                    }
                }
                Page::TYPE_FREELIST => {
                    // Make sure our metadata is pointing at the correct freelist page
                    // and we didn't somehow find our way to another one.
                    if page_id != self.meta.freelist_page {
                        return Err(Error::InvalidDB(format!(
                            "Found Invalid Freelist Page {}",
                            page_id
                        )));
                    }
                    // "visit" all freelist pages (we don't actually care what data is in these pages)
                    let entries = page.retired_pages();
                    if let Some(entry) = entries
                        .iter()
                        .find(|entry| entry.retired_tx_id > self.meta.tx_id)
                    {
                        return Err(Error::InvalidDB(format!(
                            "Page {} has future retirement transaction {}",
                            entry.page_id, entry.retired_tx_id
                        )));
                    }
                    let free_pages = entries
                        .iter()
                        .map(|entry| entry.page_id)
                        .collect::<Vec<_>>();
                    for page_id in free_pages {
                        if !unused_pages.remove(&page_id) {
                            return Err(Error::InvalidDB(format!(
                                "Page {} from freelist missing from unused_pages",
                                page_id,
                            )));
                        }
                    }
                }
                // There are no other valid page types, so getting here is really bad 😅
                _ => {
                    return Err(Error::InvalidDB(format!(
                        "Invalid page type {} for page {}",
                        page.page_type, page_id,
                    )));
                }
            }
        }

        // Once we've explored all of the pages we can reach from the root bucket and freelist,
        // If there are any pages left then we have an invalid database.
        if !unused_pages.is_empty() {
            return Err(Error::InvalidDB(format!(
                "Unreachable pages {:?}",
                unused_pages,
            )));
        }
        Ok(())
    }
}

impl<'tx> Drop for TxInner<'tx> {
    fn drop(&mut self) {
        if !self.lock.writable() {
            let mut open_txs = self.db.inner.open_ro_txs.lock().unwrap();
            let index = match open_txs.binary_search(&self.meta.tx_id) {
                Ok(i) => i,
                _ => return, // this shouldn't happen, but isn't the end of the world if it does
            };
            open_txs.remove(index);
        }
    }
}

#[cfg(test)]
#[path = "tx_tests.rs"]
mod tests;
