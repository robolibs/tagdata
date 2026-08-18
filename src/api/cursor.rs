use std::{
    cell::RefCell,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    rc::Rc,
};

use crate::{
    BucketName, KVPair,
    bucket::{Bucket, InnerBucket},
    changes::{ChangePath, SharedChangeTracker},
    data::Data,
    errors::Result,
    freelist::TxFreelist,
    page::PageID,
    page_node::PageNodeID,
};

/// An iterator over a bucket
///
/// A cursor is created by using the [`cursor`](struct.Bucket.html#method.cursor)
/// function on a [`Bucket`]. It's primary purpose is to be an [`Iterator`] over
/// the bucket's [`Data`]. By default, a newly created cursor will start at the first
/// element in the bucket (sorted by key), but you can use the [`seek`](#method.seek) method to
/// move the cursor to a certain key / prefix before beginning to iterate.
///
/// Note that if the key you seek to exists, the cursor will begin to iterate after
/// the
///
/// # Examples
///
/// ```no_run
/// use tagdata::{DB, Data};
/// # use tagdata::Error;
///
/// # fn main() -> Result<(), Error> {
/// let db = DB::open("my.db")?;
/// let mut tx = db.tx(false)?;
/// let bucket = tx.get_bucket("my-bucket")?;
///
/// // create a cursor and use it to iterate over the entire bucket
/// for data in bucket.cursor() {
///     match data? {
///         Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
///         Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
///     }
/// }
///
/// let mut cursor = bucket.cursor();
/// // seek to the key "f"
/// // if it doesn't exist, it will start at the position where it should have been
/// cursor.seek("f")?;
/// //
/// for data in cursor {
/// }
///
/// # Ok(())
/// # }
/// ```
pub struct Cursor<'b, 'tx> {
    bucket: Rc<RefCell<InnerBucket<'tx>>>,
    freelist: Rc<RefCell<TxFreelist>>,
    writable: bool,
    path: ChangePath,
    changes: SharedChangeTracker,
    stack: Vec<SearchPath>,
    next_called: bool,
    previous_called: bool,
    _phantom: PhantomData<&'b ()>,
}

impl<'b, 'tx> Cursor<'b, 'tx> {
    pub(crate) fn new(b: &Bucket<'b, 'tx>) -> Cursor<'b, 'tx> {
        Cursor {
            bucket: b.inner.clone(),
            freelist: b.freelist.clone(),
            writable: b.writable,
            path: b.path.clone(),
            changes: b.changes.clone(),
            stack: Vec::new(),
            next_called: false,
            previous_called: false,
            _phantom: PhantomData,
        }
    }

    /// Moves the cursor to the given key.
    /// If the key does not exist, the cursor stops "just before"
    /// where the key _would_ be.
    ///
    /// Returns whether or not the key exists in the bucket.
    pub fn seek<T: AsRef<[u8]>>(&mut self, key: T) -> Result<bool> {
        self.next_called = false;
        self.previous_called = false;
        let mut b = self.bucket.borrow_mut();
        if b.deleted {
            panic!("Cannot seek cursor on a deleted bucket.");
        }
        let (exists, stack) = search(key.as_ref(), b.meta.root_page, &mut b)?;
        self.stack = stack;
        Ok(exists)
    }

    /// Returns the data at the cursor's current position.
    /// You can use this to get data after doing a [`seek`](#method.seek).
    pub fn current<'a>(&'a self) -> Result<Option<Data<'b, 'tx>>> {
        let b = self.bucket.borrow_mut();
        if b.deleted {
            panic!("Cannot get data from a deleted bucket.");
        }
        match self.stack.last() {
            Some(e) => {
                let n = b.page_node(e.id)?;
                Ok(n.val(e.index).map(|data| data.into()))
            }
            None => Ok(None),
        }
    }

    fn seek_first(&mut self) -> Result<()> {
        let b = self.bucket.borrow();
        if self.stack.is_empty() {
            self.stack.push(SearchPath {
                index: 0,
                id: PageNodeID::Page(b.meta.root_page),
            });
        }
        loop {
            let elem = self.stack.last().unwrap();
            let page_node = b.page_node(elem.id)?;
            if page_node.leaf() {
                break;
            }
            if page_node.len() == 0 {
                break;
            }
            let page_id = page_node.index_page(elem.index);

            self.stack.push(SearchPath {
                index: 0,
                id: PageNodeID::Page(page_id),
            });
        }
        Ok(())
    }

    /// Positions the cursor at the greatest key in the bucket.
    pub fn seek_last(&mut self) -> Result<()> {
        self.stack.clear();
        self.next_called = false;
        self.previous_called = false;
        let b = self.bucket.borrow();
        let mut id = PageNodeID::Page(b.meta.root_page);
        loop {
            let page_node = b.page_node(id)?;
            if page_node.len() == 0 {
                return Ok(());
            }
            let index = page_node.len() - 1;
            self.stack.push(SearchPath { index, id });
            if page_node.leaf() {
                return Ok(());
            }
            id = PageNodeID::Page(page_node.index_page(index));
        }
    }

    /// Returns the current item and then traverses toward smaller keys.
    pub fn previous(&mut self) -> Result<Option<Data<'b, 'tx>>> {
        if self.stack.is_empty() {
            self.seek_last()?;
        } else if self.previous_called {
            loop {
                let (moved, descend) = {
                    let b = self.bucket.borrow();
                    let Some(element) = self.stack.last_mut() else {
                        return Ok(None);
                    };
                    if element.index == 0 {
                        (false, None)
                    } else {
                        element.index -= 1;
                        let node = b.page_node(element.id)?;
                        (
                            true,
                            (!node.leaf())
                                .then(|| PageNodeID::Page(node.index_page(element.index))),
                        )
                    }
                };
                if !moved {
                    self.stack.pop();
                    if self.stack.is_empty() {
                        return Ok(None);
                    }
                    continue;
                }
                if let Some(mut id) = descend {
                    let b = self.bucket.borrow();
                    loop {
                        let node = b.page_node(id)?;
                        if node.len() == 0 {
                            return Ok(None);
                        }
                        let index = node.len() - 1;
                        self.stack.push(SearchPath { index, id });
                        if node.leaf() {
                            break;
                        }
                        id = PageNodeID::Page(node.index_page(index));
                    }
                }
                break;
            }
        }
        self.previous_called = true;
        self.next_called = false;
        self.current()
    }
}

// function that searches the bucket for a given key
pub(crate) fn search(
    key: &[u8],
    mut page_id: PageID,
    b: &mut InnerBucket,
) -> Result<(bool, Vec<SearchPath>)> {
    let mut stack = Vec::new();
    loop {
        let page_node = b.page_node(PageNodeID::Page(page_id))?;
        let id = page_node.id();
        let (index, exact) = page_node.index(key);
        let leaf = page_node.leaf();
        stack.push(SearchPath { index, id });
        if leaf {
            return Ok((exact, stack));
        }
        let next_page_id = page_node.index_page(index);
        if next_page_id == 0 {
            return Ok((false, stack));
        }
        b.add_page_parent(next_page_id, page_id);
        page_id = next_page_id;
    }
}

pub(crate) fn search_leaf(
    key: &[u8],
    mut page_id: PageID,
    b: &InnerBucket,
) -> Result<(bool, SearchPath)> {
    loop {
        let page_node = b.page_node(PageNodeID::Page(page_id))?;
        let id = page_node.id();
        let (index, exact) = page_node.index(key);
        if page_node.leaf() {
            return Ok((exact, SearchPath { index, id }));
        }
        let next_page_id = page_node.index_page(index);
        if next_page_id == 0 {
            return Ok((false, SearchPath { index, id }));
        }
        page_id = next_page_id;
    }
}

// Keeps track of the path we've taken to search a PageNode.
pub(crate) struct SearchPath {
    pub(crate) index: usize,
    pub(crate) id: PageNodeID,
}

impl<'b, 'tx> Iterator for Cursor<'b, 'tx> {
    type Item = Result<Data<'b, 'tx>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.advance() {
            Ok(Some(data)) => Some(Ok(data)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'b, 'tx> Cursor<'b, 'tx> {
    // The fallible body of Iterator::next.
    fn advance(&mut self) -> Result<Option<Data<'b, 'tx>>> {
        self.previous_called = false;
        if self.stack.is_empty() {
            self.seek_first()?;
        } else if self.next_called {
            loop {
                {
                    let b = self.bucket.borrow();
                    if b.deleted {
                        panic!("Cannot get data from a deleted bucket.");
                    }
                    let elem = self.stack.last_mut().unwrap();
                    let page_node = b.page_node(elem.id)?;
                    if elem.index >= (page_node.len() - 1) {
                        if self.stack.len() == 1 {
                            return Ok(None);
                        }
                        self.stack.pop();
                        continue;
                    } else {
                        elem.index += 1;
                    }
                }
                self.seek_first()?;
                break;
            }
        }
        self.next_called = true;
        self.current()
    }
}

/// A bounded iterator over the data in a bucket.
pub struct Range<'r, 'b, 'tx, R>
where
    R: RangeBounds<&'r [u8]>,
{
    pub(crate) c: Cursor<'b, 'tx>,
    pub(crate) bounds: R,
    pub(crate) _phantom: PhantomData<&'r ()>,
}

impl<'r, 'b, 'tx, R> Iterator for Range<'r, 'b, 'tx, R>
where
    R: RangeBounds<&'r [u8]>,
{
    type Item = Result<Data<'b, 'tx>>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.c.next_called
            && let Bound::Included(s) = self.bounds.start_bound()
        {
            let exists = match self.c.seek(*s) {
                Ok(exists) => exists,
                Err(e) => return Some(Err(e)),
            };
            if !exists {
                match self.c.current() {
                    Ok(Some(data)) if data.key() < *s => {
                        self.c.next();
                    }
                    Ok(_) => (),
                    Err(e) => return Some(Err(e)),
                }
            }
        }
        let data = match self.c.next()? {
            Ok(data) => data,
            Err(e) => return Some(Err(e)),
        };
        match self.bounds.end_bound() {
            Bound::Excluded(e) => (data.key() < *e).then_some(Ok(data)),
            Bound::Included(e) => (data.key() <= *e).then_some(Ok(data)),
            Bound::Unbounded => Some(Ok(data)),
        }
    }
}

/// An iterator over a bucket's sub-buckets.
pub struct Buckets<'b, 'tx, I> {
    pub(crate) i: I,
    pub(crate) bucket: Rc<RefCell<InnerBucket<'tx>>>,
    pub(crate) freelist: Rc<RefCell<TxFreelist>>,
    pub(crate) writable: bool,
    pub(crate) path: ChangePath,
    pub(crate) changes: SharedChangeTracker,
    pub(crate) _phantom: PhantomData<&'b ()>,
}

impl<'b, 'tx: 'b, I> Iterator for Buckets<'b, 'tx, I>
where
    I: Iterator<Item = Result<Data<'b, 'tx>>>,
{
    type Item = Result<(BucketName<'b, 'tx>, Bucket<'b, 'tx>)>;

    fn next(&mut self) -> Option<Self::Item> {
        for data in self.i.by_ref() {
            let data = match data {
                Ok(data) => data,
                Err(e) => return Some(Err(e)),
            };
            if let Data::Bucket(bucket_data) = data {
                let mut b = self.bucket.borrow_mut();
                let r = match b.get_bucket(&bucket_data) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(e)),
                };
                let path = self.path.child(bucket_data.name());
                return Some(Ok((
                    bucket_data,
                    Bucket {
                        writable: self.writable,
                        freelist: self.freelist.clone(),
                        inner: r,
                        path,
                        changes: self.changes.clone(),
                        _phantom: PhantomData,
                    },
                )));
            }
        }
        None
    }
}

pub trait ToBuckets<'b, 'tx: 'b>: Iterator<Item = Result<Data<'b, 'tx>>> + Sized {
    fn to_buckets(self) -> Buckets<'b, 'tx, Self>;
}

impl<'b, 'tx: 'b> ToBuckets<'b, 'tx> for Cursor<'b, 'tx> {
    fn to_buckets(self) -> Buckets<'b, 'tx, Self> {
        let freelist = self.freelist.clone();
        let bucket = self.bucket.clone();
        let writable = self.writable;
        let path = self.path.clone();
        let changes = self.changes.clone();
        Buckets {
            i: self,
            bucket,
            freelist,
            writable,
            path,
            changes,
            _phantom: PhantomData,
        }
    }
}

impl<'r, 'b, 'tx: 'b, R> ToBuckets<'b, 'tx> for Range<'r, 'b, 'tx, R>
where
    R: RangeBounds<&'r [u8]>,
{
    fn to_buckets(self) -> Buckets<'b, 'tx, Self> {
        let freelist = self.c.freelist.clone();
        let bucket = self.c.bucket.clone();
        let writable = self.c.writable;
        let path = self.c.path.clone();
        let changes = self.c.changes.clone();
        Buckets {
            i: self,
            bucket,
            freelist,
            writable,
            path,
            changes,
            _phantom: PhantomData,
        }
    }
}

/// An iterator over a bucket's key / value pairs.
pub struct KVPairs<I> {
    pub(crate) i: I,
}

impl<'b, 'tx, I> Iterator for KVPairs<I>
where
    I: Iterator<Item = Result<Data<'b, 'tx>>>,
{
    type Item = Result<KVPair<'b, 'tx>>;

    fn next(&mut self) -> Option<Self::Item> {
        for data in self.i.by_ref() {
            match data {
                Ok(Data::KeyValue(kv)) => return Some(Ok(kv)),
                Ok(Data::Bucket(_)) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

pub trait ToKVPairs<'b, 'tx>: Iterator<Item = Result<Data<'b, 'tx>>> + Sized {
    fn to_kv_pairs(self) -> KVPairs<Self>;
}

impl<'b, 'tx> ToKVPairs<'b, 'tx> for Cursor<'b, 'tx> {
    fn to_kv_pairs(self) -> KVPairs<Self> {
        KVPairs { i: self }
    }
}

impl<'r, 'b, 'tx, R> ToKVPairs<'b, 'tx> for Range<'r, 'b, 'tx, R>
where
    R: RangeBounds<&'r [u8]>,
{
    fn to_kv_pairs(self) -> KVPairs<Self> {
        KVPairs { i: self }
    }
}

#[cfg(test)]
mod tests {
    use crate::{db::DB, errors::Result, testutil::RandomFile};

    #[test]
    fn test_iters() -> Result<()> {
        let random_file = RandomFile::new();
        let db = DB::open(&random_file)?;
        // Put in some intermixed key / value pairs and sub-buckets.
        {
            let tx = db.tx(true)?;
            let b = tx.create_bucket("abc")?;
            b.put("a", "1")?;
            b.create_bucket("b")?;
            b.put("c", "3")?;
            b.create_bucket("d")?;
            b.put("e", "5")?;
            b.create_bucket("f")?;
            tx.commit()?;
        }
        // Make sure we iterate over all sub-buckets
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            let mut buckets = b.buckets();
            // We should get the three sub-buckets in order
            let (data, _) = buckets.next().unwrap()?;
            assert_eq!(data.name(), b"b");
            let (data, _) = buckets.next().unwrap()?;
            assert_eq!(data.name(), b"d");
            let (data, _) = buckets.next().unwrap()?;
            assert_eq!(data.name(), b"f");
            // Make sure there are no more buckets
            assert!(buckets.next().is_none());
        }
        // Make sure we iterate over all kvpairs
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            let mut kvpairs = b.kv_pairs();

            // We should find the three kv pairs in order
            let data = kvpairs.next().unwrap()?;
            let (k, v) = data.kv();
            assert_eq!(k, b"a");
            assert_eq!(v, b"1");

            let data = kvpairs.next().unwrap()?;
            let (k, v) = data.kv();
            assert_eq!(k, b"c");
            assert_eq!(v, b"3");

            let data = kvpairs.next().unwrap()?;
            let (k, v) = data.kv();
            assert_eq!(k, b"e");
            assert_eq!(v, b"5");

            // There should be no more buckets
            assert!(kvpairs.next().is_none());
        }

        db.check()
    }

    #[test]
    #[should_panic]
    fn deleted_bucket_create_cursor() {
        let random_file = RandomFile::new();
        let db = DB::open(&random_file).unwrap();
        let tx = db.tx(true).unwrap();
        let b = tx.create_bucket("abc").unwrap();
        tx.delete_bucket("abc").unwrap();

        b.cursor();
    }

    #[test]
    #[should_panic]
    fn deleted_bucket_create_iterate() {
        let random_file = RandomFile::new();
        let db = DB::open(&random_file).unwrap();
        let tx = db.tx(true).unwrap();
        let b = tx.create_bucket("abc").unwrap();
        let mut c = b.cursor();
        tx.delete_bucket("abc").unwrap();
        c.next();
    }
}
