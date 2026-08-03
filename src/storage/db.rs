#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{File, OpenOptions as FileOpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use fs4::FileExt;
use memmap2::Mmap;
use page_size::get as get_page_size;

use crate::{
    bucket::BucketMeta,
    changes::WatchHub,
    coordination::Coordination,
    errors::{Error, Result},
    format::FormatInfo,
    freelist::Freelist,
    meta::Meta,
    page::{Page, seal_block},
    stats::Stats,
    tx::Tx,
};

pub(crate) const MAGIC_VALUE: u32 = 0x00AB_CDEF;
pub(crate) const VERSION: u32 = 2;

// Minimum number of bytes to allocate when growing the databse
pub(crate) const MIN_ALLOC_SIZE: u64 = 8 * 1024 * 1024;

// Number of pages to allocate when creating the database
const DEFAULT_NUM_PAGES: usize = 32;

/// Options to configure how a [`DB`] is opened.
///
/// This struct acts as a builder for a [`DB`] and allows you to specify
/// the initial pagesize and number of pages you want to allocate for a new database file.
///
/// # Examples
///
/// ```no_run
/// use inspace::{DB, OpenOptions};
/// # use inspace::Error;
///
/// # fn main() -> Result<(), Error> {
/// let db = OpenOptions::new()
///     .pagesize(4096)
///     .num_pages(32)
///     .open("my.db")?;
///
/// // do whatever you want with the DB
/// # Ok(())
/// # }
/// ```
pub struct OpenOptions {
    pagesize: Option<u64>,
    num_pages: usize,
    max_file_bytes: Option<u64>,
    growth_increment: u64,
    flags: DBFlags,
}

impl OpenOptions {
    /// Returns a new OpenOptions, with the default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the pagesize for the database
    ///
    /// By default, your OS's pagesize is used as the database's pagesize, but if the file is
    /// moved across systems with different page sizes, it is necessary to set the correct value.
    /// Trying to open an existing database with the incorrect page size will result in a panic.
    ///
    /// # Panics
    /// Will panic if you try to set the pagesize < 1024 bytes.
    pub fn pagesize(mut self, pagesize: u64) -> Self {
        if pagesize < 1024 {
            panic!("Pagesize must be 1024 bytes minimum");
        }
        self.pagesize = Some(pagesize);
        self
    }

    /// Sets a hard upper bound for the database file.
    pub fn max_file_bytes(mut self, bytes: u64) -> Self {
        assert!(
            bytes >= 4096,
            "Maximum file size must be at least 4096 bytes"
        );
        self.max_file_bytes = Some(bytes);
        self
    }

    /// Sets the allocation quantum used when the file grows.
    pub fn growth_increment(mut self, bytes: u64) -> Self {
        assert!(bytes > 0, "Growth increment must be non-zero");
        self.growth_increment = bytes;
        self
    }

    /// Sets the number of pages to allocate for a new database file.
    ///
    /// The default `num_pages` is set to 32, so if your pagesize is 4096 bytes (4kb), then 131,072 bytes (128kb) will be allocated for the initial file.
    /// Setting `num_pages` when opening an existing database has no effect.
    ///
    /// # Panics
    /// Since a minimum of four pages are required for the database, this function will panic if you provide a value < 4.
    pub fn num_pages(mut self, num_pages: usize) -> Self {
        if num_pages < 4 {
            panic!("Must have a minimum of 4 pages");
        }
        self.num_pages = num_pages;
        self
    }

    /// Enables or disables "Strict Mode", where each transaction will check the database for errors before finalizing a write.
    ///
    /// The default is `false`, but you may enable this if you want an extra degree of safety for your data at the cost of
    /// slower writes.
    pub fn strict_mode(mut self, strict_mode: bool) -> Self {
        self.flags.strict_mode = strict_mode;
        self
    }

    /// Enables or disables the [MAP_POPULATE flag](https://man7.org/linux/man-pages/man2/mmap.2.html)
    /// for the `mmap` call, which will cause Linux to eagerly load pages into memory.
    ///
    /// The default is `false`, but you may enable this if your database file will stay smaller than your available memory.
    /// It is not recommended to enable this unless you know what you are doing.
    ///
    /// This setting only works on Linux, and is a no-op on other platforms.
    pub fn mmap_populate(mut self, mmap_populate: bool) -> Self {
        self.flags.mmap_populate = mmap_populate;
        self
    }

    /// Enables or disables the O_DIRECT flag when opening the database file.
    /// This gives a hint to Linux to bypass any operarating system caches when writing to this file.
    ///
    /// The default is `false`, but you may enable this if your database is much larger than your available memory to avoid throttling the page cache.
    /// It is not recommended to enable this unless you know what you are doing.
    ///
    /// This setting only works on Linux, and is a no-op on other platforms.
    pub fn direct_writes(mut self, direct_writes: bool) -> Self {
        self.flags.direct_writes = direct_writes;
        self
    }

    /// Opens an existing database without write permission.
    pub fn read_only(mut self) -> Self {
        self.flags.read_only = true;
        self.flags.direct_writes = false;
        self
    }

    /// Verifies the reachable page tree before returning from `open`.
    pub fn verify_on_open(mut self, verify: bool) -> Self {
        self.flags.verify_on_open = verify;
        self
    }

    /// Opens the database with the current options.
    ///
    /// If the file does not exist, it will initialize an empty database with a size of (`num_pages * pagesize`) bytes.
    /// If it does exist, the file is opened with both read and write permissions, and we attempt to create an
    /// [exclusive lock](https://en.wikipedia.org/wiki/File_locking) on the file. Getting the file lock will block until the lock
    /// is released to prevent you from having two processes modifying the file at the same time. This lock is not foolproof though,
    /// so it is up to the user to make sure only one process has access to the database at a time (unless it is read-only).
    ///
    /// # Errors
    ///
    /// Will return an error if there are issues creating a new file, opening an existing file, obtaining the file lock, or creating the memory map.
    ///
    /// # Panics
    ///
    /// Will panic if the pagesize the database is opened with is not the same as the pagesize it was created with.
    pub fn open<P: AsRef<Path>>(self, path: P) -> Result<DB> {
        let path: &Path = path.as_ref();
        let exists = path.exists();
        let pagesize = if exists {
            match (self.pagesize, FormatInfo::inspect(path)) {
                (None, Ok(info)) => info.page_size,
                (Some(explicit), Ok(info)) if explicit == info.page_size => explicit,
                (Some(explicit), Ok(info)) => {
                    return Err(Error::InvalidDB(format!(
                        "explicit page size {explicit} conflicts with detected page size {}",
                        info.page_size
                    )));
                }
                (Some(explicit), Err(_)) => explicit,
                (None, Err(error)) => return Err(error),
            }
        } else {
            self.pagesize.unwrap_or_else(|| get_page_size() as u64)
        };
        let initial_bytes = pagesize.saturating_mul(self.num_pages as u64);
        if !exists
            && let Some(maximum) = self.max_file_bytes
            && initial_bytes > maximum
        {
            return Err(Error::CapacityExceeded {
                required: initial_bytes,
                maximum,
            });
        }
        let file = if self.flags.read_only {
            open_file(path, false, false, true)?
        } else if !exists {
            init_file(path, pagesize, self.num_pages, self.flags.direct_writes)?
        } else {
            open_file(path, false, self.flags.direct_writes, false)?
        };

        if let Some(maximum) = self.max_file_bytes {
            let required = file.metadata()?.len();
            if required > maximum {
                return Err(Error::CapacityExceeded { required, maximum });
            }
        }

        let verify_on_open = self.flags.verify_on_open;
        let db = DB {
            inner: Arc::new(DBInner::open(
                file,
                pagesize,
                self.flags,
                path,
                self.max_file_bytes,
                self.growth_increment,
            )?),
        };
        if verify_on_open {
            db.verify()?;
        }
        Ok(db)
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        let pagesize = get_page_size() as u64;
        if pagesize < 1024 {
            panic!("Pagesize must be 1024 bytes minimum");
        }
        OpenOptions {
            pagesize: None,
            num_pages: DEFAULT_NUM_PAGES,
            max_file_bytes: None,
            growth_increment: MIN_ALLOC_SIZE,
            flags: DBFlags {
                strict_mode: false,
                mmap_populate: false,
                direct_writes: false,
                read_only: false,
                verify_on_open: false,
            },
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct DBFlags {
    pub(crate) strict_mode: bool,
    pub(crate) mmap_populate: bool,
    pub(crate) direct_writes: bool,
    pub(crate) read_only: bool,
    pub(crate) verify_on_open: bool,
}

/// A database
///
/// A DB can created from an [`OpenOptions`] builder, or by calling [`open`](#method.open).
/// From a DB, you can create a [`Tx`] to access the data in the database.
/// If you want to use the database across threads, so you can `clone` the database
/// to have concurrent transactions (you're really just cloning an [`Arc`] so it's pretty cheap).
/// **Do not** try to open multiple transactions in the same thread, you're pretty likely to cause a deadlock.
#[derive(Clone)]
pub struct DB {
    pub(crate) inner: Arc<DBInner>,
}

impl DB {
    /// Opens a database using the default [`OpenOptions`].
    ///
    /// Same as calling `OpenOptions::new().open(path)`.
    /// Please read the documentation for [`OpenOptions::open`](struct.OpenOptions.html#method.open) for details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inspace::{DB};
    /// # use inspace::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    ///
    /// // do whatever you want with the DB
    /// # Ok(())
    /// # }
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DB> {
        OpenOptions::new().open(path)
    }

    /// Creates a [`Tx`].
    /// This transaction is either read-only or writable depending on the `writable` parameter.
    /// Please read the docs on a [`Tx`] for more details.
    pub fn tx(&self, writable: bool) -> Result<Tx<'_>> {
        Tx::new(self, writable)
    }

    /// Starts a read-only snapshot transaction.
    pub fn read_tx(&self) -> Result<Tx<'_>> {
        Tx::new(self, false)
    }

    /// Starts a writable transaction, waiting until the single writer is available.
    pub fn write_tx(&self) -> Result<Tx<'_>> {
        Tx::new(self, true)
    }

    /// Attempts to start a writable transaction without waiting for another writer.
    ///
    /// Returns `Ok(None)` when either this process or another process currently owns
    /// the writer slot.
    pub fn try_write_tx(&self) -> Result<Option<Tx<'_>>> {
        if self.inner.flags.read_only {
            return Err(Error::ReadOnlyDB);
        }
        Tx::try_new_writable(self)
    }

    /// Waits up to `timeout` for the single writer slot.
    pub fn write_tx_timeout(&self, timeout: std::time::Duration) -> Result<Tx<'_>> {
        if self.inner.flags.read_only {
            return Err(Error::ReadOnlyDB);
        }
        Tx::new_writable_timeout(self, timeout)
    }

    /// Runs synchronous work in a read-only transaction.
    pub fn view<T>(&self, operation: impl FnOnce(&Tx<'_>) -> Result<T>) -> Result<T> {
        let tx = self.read_tx()?;
        operation(&tx)
    }

    /// Runs synchronous work in a write transaction and commits only on `Ok`.
    pub fn update<T>(&self, operation: impl FnOnce(&Tx<'_>) -> Result<T>) -> Result<T> {
        let tx = self.write_tx()?;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(&tx))) {
            Ok(Ok(result)) => {
                tx.commit()?;
                Ok(result)
            }
            Ok(Err(error)) => Err(error),
            Err(payload) => {
                // Drop the transaction after catch_unwind has cleared the panicking
                // state so its mutex guards are not poisoned during rollback.
                drop(tx);
                std::panic::resume_unwind(payload)
            }
        }
    }

    /// Returns the database's pagesize.
    pub fn pagesize(&self) -> u64 {
        self.inner.pagesize
    }

    /// Returns a point-in-time snapshot of database statistics.
    pub fn stats(&self) -> Result<Stats> {
        let _gate = match &self.inner.coordination {
            Some(coordination) => Some(coordination.shared_gate()?),
            None => None,
        };
        let file_bytes = {
            let file = self.inner.file.lock()?;
            self.inner.refresh(&file)?;
            file.metadata()?.len()
        };
        let meta = self.inner.meta()?;
        let (free_pages, pending_pages) = {
            let freelist = self.inner.freelist.lock()?;
            (freelist.free_count(), freelist.pending_count())
        };
        let (active_readers, oldest_reader_tx_id) = match &self.inner.coordination {
            Some(coordination) => coordination.readers()?,
            None => {
                let readers = self.inner.open_ro_txs.lock()?;
                (readers.len() as u64, readers.first().copied())
            }
        };

        Ok(Stats {
            file_bytes,
            page_size: self.inner.pagesize,
            allocated_pages: meta.num_pages,
            free_pages,
            pending_pages,
            reader_pinned_pages: if active_readers == 0 {
                0
            } else {
                pending_pages
            },
            current_tx_id: meta.tx_id,
            active_readers,
            oldest_reader_tx_id,
            committed_transactions: self.inner.committed_transactions.load(Ordering::Relaxed),
            bytes_written: self.inner.bytes_written.load(Ordering::Relaxed),
        })
    }

    /// Validates checksums, page bounds, ordering, and reachability.
    pub fn verify(&self) -> Result<()> {
        self.tx(false)?.check()
    }

    #[doc(hidden)]
    pub fn check(&self) -> Result<()> {
        self.verify()
    }
}
pub(crate) struct DBInner {
    pub(crate) data: Mutex<Arc<Mmap>>,
    pub(crate) freelist: Mutex<Freelist>,
    pub(crate) file: Mutex<File>,
    pub(crate) open_ro_txs: Mutex<Vec<u64>>,
    pub(crate) coordination: Option<Coordination>,
    pub(crate) flags: DBFlags,

    pub(crate) pagesize: u64,
    pub(crate) max_file_bytes: Option<u64>,
    pub(crate) growth_increment: u64,
    pub(crate) committed_transactions: AtomicU64,
    pub(crate) bytes_written: AtomicU64,
    pub(crate) path: PathBuf,
    pub(crate) watches: WatchHub,
}

impl DBInner {
    pub(crate) fn open(
        file: File,
        pagesize: u64,
        flags: DBFlags,
        path: &Path,
        max_file_bytes: Option<u64>,
        growth_increment: u64,
    ) -> Result<DBInner> {
        if flags.read_only {
            FileExt::lock_shared(&file)?;
        }
        let coordination = if flags.read_only {
            None
        } else {
            Some(Coordination::open(path)?)
        };
        let mmap = mmap(&file, flags.mmap_populate)?;
        let mmap = Mutex::new(Arc::new(mmap));
        let db = DBInner {
            data: mmap,
            freelist: Mutex::new(Freelist::new()),

            file: Mutex::new(file),
            open_ro_txs: Mutex::new(Vec::new()),
            coordination,

            pagesize,
            max_file_bytes,
            growth_increment,
            flags,
            committed_transactions: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            path: path.to_path_buf(),
            watches: WatchHub::new(),
        };

        {
            let meta = db.meta()?;
            let data = db.data.lock()?;
            let free_pages =
                Page::validate_block(&data, meta.freelist_page, pagesize, meta.version)?.freelist();

            if !free_pages.is_empty() {
                db.freelist.lock()?.init(free_pages);
            }
        }

        Ok(db)
    }

    pub(crate) fn resize(&self, file: &File, new_size: u64) -> Result<Arc<Mmap>> {
        file.allocate(new_size)?;
        let mut data = self.data.lock()?;
        let mmap = mmap(file, self.flags.mmap_populate)?;
        *data = Arc::new(mmap);
        Ok(data.clone())
    }

    pub(crate) fn refresh(&self, file: &File) -> Result<()> {
        let file_len = file.metadata()?.len();
        let mut data = self.data.lock()?;
        if data.len() as u64 != file_len {
            *data = Arc::new(mmap(file, self.flags.mmap_populate)?);
        }
        Ok(())
    }

    pub(crate) fn reload_freelist(&self) -> Result<()> {
        let meta = self.meta()?;
        let data = self.data.lock()?;
        let free_pages =
            Page::validate_block(&data, meta.freelist_page, self.pagesize, meta.version)?
                .freelist();
        let mut freelist = Freelist::new();
        freelist.init(free_pages);
        *self.freelist.lock()? = freelist;
        Ok(())
    }

    pub(crate) fn meta(&self) -> Result<Meta> {
        let data = self.data.lock()?;

        macro_rules! check_meta {
            ($func:ident) => {{
                let meta1 = Page::validate_block(&data, 0, self.pagesize, 1)
                    .ok()
                    .map(|page| page.$func());
                let meta2 = Page::validate_block(&data, 1, self.pagesize, 1)
                    .ok()
                    .map(|page| page.$func());
                let valid1 = meta1.is_some_and(|meta| {
                    meta.valid()
                        && meta.magic == MAGIC_VALUE
                        && (1..=VERSION).contains(&meta.version)
                        && meta.pagesize == self.pagesize
                        && Page::validate_block(&data, 0, self.pagesize, meta.version).is_ok()
                });
                let valid2 = meta2.is_some_and(|meta| {
                    meta.valid()
                        && meta.magic == MAGIC_VALUE
                        && (1..=VERSION).contains(&meta.version)
                        && meta.pagesize == self.pagesize
                        && Page::validate_block(&data, 1, self.pagesize, meta.version).is_ok()
                });
                match (valid1, valid2) {
                    (true, true) => {
                        let meta1 = meta1.unwrap();
                        let meta2 = meta2.unwrap();
                        if meta1.tx_id > meta2.tx_id {
                            Some(meta1)
                        } else {
                            Some(meta2)
                        }
                    }
                    (true, false) => meta1,
                    (false, true) => meta2,
                    (false, false) => None,
                }
            }};
        }

        if let Some(meta) = check_meta!(meta) {
            Ok(meta.clone())
        } else if let Some(old_meta) = check_meta!(old_meta) {
            Ok(old_meta.into())
        } else {
            Err(Error::InvalidDB("no valid metadata pages".into()))
        }
    }
}

fn init_file(path: &Path, pagesize: u64, num_pages: usize, direct_write: bool) -> Result<File> {
    init_file_version(path, pagesize, num_pages, direct_write, VERSION)
}

fn init_file_version(
    path: &Path,
    pagesize: u64,
    num_pages: usize,
    direct_write: bool,
    version: u32,
) -> Result<File> {
    let mut file = open_file(path, true, direct_write, false)?;
    file.allocate(pagesize * (num_pages as u64))?;
    let mut buf = vec![0; (pagesize * 4) as usize];
    let mut get_page = |index: u64| {
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &mut *(&mut buf[(index * pagesize) as usize] as *mut u8 as *mut Page)
        }
    };
    for i in 0..2 {
        let page = get_page(i);
        page.id = i;
        page.page_type = Page::TYPE_META;
        let m = page.meta_mut();
        m.meta_page = i as u32;
        m.magic = MAGIC_VALUE;
        m.version = version;
        m.pagesize = pagesize;
        m.freelist_page = 2;
        m.root = BucketMeta {
            root_page: 3,
            next_int: 0,
        };
        m.num_pages = 4;
        m.hash = m.hash_self();
    }

    let p = get_page(2);
    p.id = 2;
    p.page_type = Page::TYPE_FREELIST;
    p.count = 0;

    let p = get_page(3);
    p.id = 3;
    p.page_type = Page::TYPE_LEAF;
    p.count = 0;

    for page in buf.chunks_exact_mut(pagesize as usize) {
        seal_block(page, version)?;
    }

    file.write_all(&buf[..])?;
    file.flush()?;
    file.sync_all()?;
    Ok(file)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use crate::testutil::RandomFile;

    #[test]
    fn test_open_options() {
        assert_ne!(get_page_size(), 5000);
        let random_file = RandomFile::new();
        {
            let db = OpenOptions::new()
                .pagesize(5000)
                .num_pages(100)
                .open(&random_file)
                .unwrap();
            assert_eq!(db.pagesize(), 5000);
        }
        {
            let metadata = random_file.path.metadata().unwrap();
            assert!(metadata.is_file());
            assert_eq!(metadata.len(), 500_000);
        }
        {
            let db = OpenOptions::new()
                .pagesize(5000)
                .num_pages(100)
                .open(&random_file)
                .unwrap();
            assert_eq!(db.pagesize(), 5000);
        }
    }

    #[test]
    #[should_panic]
    fn test_open_options_min_pages() {
        OpenOptions::new().num_pages(3);
    }

    #[test]
    #[should_panic]
    fn test_open_options_min_pagesize() {
        OpenOptions::new().pagesize(1000);
    }

    #[test]
    fn test_different_pagesizes_are_detected() {
        assert_ne!(get_page_size(), 5000);
        let random_file = RandomFile::new();
        {
            let db = OpenOptions::new()
                .pagesize(5000)
                .num_pages(100)
                .open(&random_file)
                .unwrap();
            assert_eq!(db.pagesize(), 5000);
        }
        assert_eq!(DB::open(&random_file).unwrap().pagesize(), 5000);
    }

    #[test]
    fn opens_and_migrates_version_one_databases() -> Result<()> {
        let source = RandomFile::new();
        let destination = RandomFile::new();
        drop(init_file_version(&source.path, 4096, 32, false, 1)?);

        let db = OpenOptions::new().pagesize(4096).open(&source)?;
        assert_eq!(db.inner.meta()?.version, 1);
        let tx = db.tx(true)?;
        tx.create_bucket("legacy")?.put("key", "value")?;
        tx.commit()?;
        db.verify()?;

        db.compact_to(&destination.path)?;
        let migrated = OpenOptions::new().pagesize(4096).open(&destination)?;
        assert_eq!(migrated.inner.meta()?.version, VERSION);
        assert_eq!(
            migrated
                .tx(false)?
                .get_bucket("legacy")?
                .get_kv("key")
                .unwrap()
                .value(),
            b"value"
        );
        migrated.verify()
    }
}

// Have different mmap functions for Unix and Windows
#[cfg(unix)]
fn mmap(file: &File, populate: bool) -> Result<Mmap> {
    use memmap2::MmapOptions;

    let mut options = MmapOptions::new();
    if populate {
        options.populate();
    }
    let mmap = unsafe { options.map(file)? };
    // On Unix we advice the OS that page access will be random.
    mmap.advise(memmap2::Advice::Random)?;
    Ok(mmap)
}

// On Windows there is no advice to give.
#[cfg(windows)]
fn mmap(file: &File, populate: bool) -> Result<Mmap> {
    let mmap = unsafe { Mmap::map(file)? };
    Ok(mmap)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const O_DIRECT: libc::c_int = libc::O_DIRECT;

#[cfg(not(any(target_os = "linux", target_os = "android")))]
const O_DIRECT: libc::c_int = 0;

// Have different mmap functions for Unix and Windows
#[cfg(unix)]
fn open_file<P: AsRef<Path>>(
    path: P,
    create: bool,
    direct_write: bool,
    read_only: bool,
) -> Result<File> {
    let mut open_options = FileOpenOptions::new();
    open_options.read(true).write(!read_only);
    if create {
        open_options.create_new(true);
    }
    if direct_write {
        open_options.custom_flags(O_DIRECT);
    }
    Ok(open_options.open(path)?)
}

#[cfg(windows)]
fn open_file<P: AsRef<Path>>(
    path: P,
    create: bool,
    _direct_write: bool,
    read_only: bool,
) -> Result<File> {
    let mut open_options = FileOpenOptions::new();
    open_options.read(true).write(!read_only);
    if create {
        open_options.create_new(true);
    }
    Ok(open_options.open(path)?)
}
