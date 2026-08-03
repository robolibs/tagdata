use std::{
    cell::{RefCell, RefMut},
    collections::HashMap,
    marker::PhantomData,
    mem::{align_of, size_of},
    ops::RangeBounds,
    rc::Rc,
};

use crate::{
    BucketName,
    bytes::{Bytes, ToBytes},
    cursor::{Cursor, Range, ToBuckets, ToKVPairs, search},
    data::{Data, KVPair},
    errors::{Error, Result},
    freelist::TxFreelist,
    node::{Leaf, Node, NodeData, NodeID},
    page::{Page, PageID, Pages},
    page_node::{PageNode, PageNodeID},
};

mod inner;
pub(crate) use inner::{BucketMeta, InnerBucket, META_SIZE};

#[cfg(test)]
mod tests;

/// A collection of data
///
/// Buckets contain a collection of data, sorted by key.
/// The data can either be key / value pairs, or nested buckets.
/// You can use buckets to [`get`](#method.get) and [`put`](#method.put) data,
/// as well as [`get`](#method.get_bucket) and [`create`](#method.create_bucket)
/// nested buckets.
///
/// You can use a [`Cursor`] to iterate over all the data in a bucket.
///
/// Buckets have an inner auto-incremented counter that keeps track
/// of how many unique keys have been inserted into the bucket.
/// You can access that using the [`next_int()`](#method.next_int) function.
///
/// # Examples
///
/// ```no_run
/// use inspace::{DB, Data};
/// # use inspace::Error;
///
/// # fn main() -> Result<(), Error> {
/// let db = DB::open("my.db")?;
/// let mut tx = db.tx(true)?;
///
/// // create a root-level bucket
/// let bucket = tx.create_bucket("my-bucket")?;
///
/// // create nested bucket
/// bucket.create_bucket("nested-bucket")?;
///
/// // insert a key / value pair (using &str)
/// bucket.put("key", "value");
///
/// // insert a key / value pair (using [u8])
/// bucket.put([1,2,3], [4,5,6]);
///
/// for data in bucket.cursor() {
///     match data {
///         Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
///         Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
///     }
/// }
///
/// println!("Bucket next_int {:?}", bucket.next_int());
/// # Ok(())
/// # }
/// ```
///
/// In order to keep the database flexible, it is possible to obtain references to multiple sub-buckets from a single parent.
/// That means it is possible to obtain a reference to a bucket, then delete that bucket from the parent. Do not do this.
/// If you try to use a bucket that has been deleted it will panic, and nobody wants that 🙃.
/// The same is true for any iterator over a bucket as well, like a [`Cursor`],
/// [`crate::Buckets`], or [`crate::KVPairs`].
pub struct Bucket<'b, 'tx: 'b> {
    pub(crate) inner: Rc<RefCell<InnerBucket<'tx>>>,
    pub(crate) freelist: Rc<RefCell<TxFreelist>>,
    pub(crate) writable: bool,
    pub(crate) _phantom: PhantomData<&'b ()>,
}

impl<'b, 'tx> Bucket<'b, 'tx> {
    /// Adds to or replaces key / value data in the bucket.
    /// Returns an error if the key currently exists but is a bucket instead of a key / value pair.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inspace::{DB};
    /// # use inspace::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(true)?;
    ///
    /// // create a root-level bucket
    /// let bucket = tx.create_bucket("my-bucket")?;
    ///
    /// // insert data
    /// bucket.put("123", "456")?;
    ///
    /// // update data
    /// bucket.put("123", "789")?;
    ///
    /// bucket.create_bucket("nested-bucket")?;
    ///
    /// assert!(bucket.put("nested-bucket", "data").is_err());
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn put<'a, T: ToBytes<'tx>, S: ToBytes<'tx>>(
        &'a self,
        key: T,
        value: S,
    ) -> Result<Option<KVPair<'b, 'tx>>> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        let mut b = self.inner.borrow_mut();
        if b.deleted {
            panic!("Cannot put data into a deleted bucket.");
        }
        Ok(b.put(key, value)?.map(|v| v.into()))
    }

    pub fn get<'a, T: AsRef<[u8]>>(&'a self, key: T) -> Option<Data<'b, 'tx>> {
        let mut b = self.inner.borrow_mut();
        if b.deleted {
            panic!("Cannot get data from a deleted bucket.");
        }
        b.get(key).map(|data| data.into())
    }

    pub fn get_kv<'a, T: AsRef<[u8]>>(&'a self, key: T) -> Option<KVPair<'b, 'tx>> {
        let mut b = self.inner.borrow_mut();
        if b.deleted {
            panic!("Cannot get data from a deleted bucket.");
        }
        match b.get(key) {
            Some(data) => data.into(),
            None => None,
        }
    }

    /// Deletes a key / value pair from the bucket
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inspace::{DB};
    /// # use inspace::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(false)?;
    ///
    /// let bucket = tx.get_bucket("my-bucket")?;
    /// // check if data is there
    /// assert!(bucket.get_kv("some-key").is_some());
    /// // delete the key / value pair
    /// bucket.delete("some-key")?;
    /// // data should no longer exist
    /// assert!(bucket.get_kv("some-key").is_none());
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete<T: AsRef<[u8]>>(&self, key: T) -> Result<KVPair<'_, '_>> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        let mut b = self.inner.borrow_mut();
        if b.deleted {
            panic!("Cannot delete data from a deleted bucket.");
        }
        Ok(b.delete(key)?.into())
    }

    /// Gets an already created bucket.
    ///
    /// Returns an error if
    /// 1. the given key does not exist
    /// 2. the key is for key / value data, not a bucket
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inspace::{DB};
    /// # use inspace::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(false)?;
    ///
    /// // get a root-level bucket
    /// let bucket = tx.get_bucket("my-bucket")?;
    ///
    /// // get nested bucket
    /// let mut sub_bucket = bucket.get_bucket("nested-bucket")?;
    ///
    /// // get nested bucket
    /// let sub_sub_bucket = sub_bucket.get_bucket("double-nested-bucket")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_bucket<'a, T: ToBytes<'tx>>(&'a self, name: T) -> Result<Bucket<'b, 'tx>> {
        let mut b = self.inner.borrow_mut();
        if b.deleted {
            panic!("Cannot get bucket from a deleted bucket.");
        }
        let inner = b.get_bucket(name)?;
        Ok(Bucket {
            inner,
            freelist: self.freelist.clone(),
            writable: self.writable,
            _phantom: PhantomData,
        })
    }

    /// Creates a new bucket.
    ///
    /// Returns an error if
    /// 1. the given key already exists
    /// 2. It is in a read-only transaction
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inspace::{DB};
    /// # use inspace::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(true)?;
    ///
    /// // create a root-level bucket
    /// let bucket = tx.create_bucket("my-bucket")?;
    ///
    /// // create nested bucket
    /// let mut sub_bucket = bucket.create_bucket("nested-bucket")?;
    ///
    /// // create nested bucket
    /// let mut sub_sub_bucket = sub_bucket.create_bucket("double-nested-bucket")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_bucket<'a, T: ToBytes<'tx>>(&'a self, name: T) -> Result<Bucket<'b, 'tx>> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        let mut b = self.inner.borrow_mut();
        if b.deleted {
            panic!("Cannot create bucket in a deleted bucket.");
        }
        let inner = b.create_bucket(name)?;
        Ok(Bucket {
            inner,
            freelist: self.freelist.clone(),
            writable: self.writable,
            _phantom: PhantomData,
        })
    }

    /// Creates a new bucket if it doesn't exist
    ///
    /// Returns an error if
    /// 1. It is in a read-only transaction
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inspace::{DB};
    /// # use inspace::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// {
    ///     let mut tx = db.tx(true)?;
    ///     // create a root-level bucket
    ///     let bucket = tx.get_or_create_bucket("my-bucket")?;
    ///     tx.commit()?;
    /// }
    /// {
    ///     let mut tx = db.tx(true)?;
    ///     // get the existing a root-level bucket
    ///     let bucket = tx.get_or_create_bucket("my-bucket")?;
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```    
    pub fn get_or_create_bucket<'a, T: ToBytes<'tx>>(&'a self, name: T) -> Result<Bucket<'b, 'tx>> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        let mut b = self.inner.borrow_mut();
        if b.deleted {
            panic!("Cannot get or create bucket from a deleted bucket.");
        }
        let inner = b.get_or_create_bucket(name)?;
        Ok(Bucket {
            inner,
            freelist: self.freelist.clone(),
            writable: self.writable,
            _phantom: PhantomData,
        })
    }

    /// Deletes an bucket.
    ///
    /// Returns an error if
    /// 1. the given key does not exist
    /// 2. the key is for key / value data, not a bucket
    /// 3. It is in a read-only transaction
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inspace::{DB};
    /// # use inspace::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(true)?;
    ///
    /// // get a root-level bucket
    /// let bucket = tx.get_bucket("my-bucket")?;
    ///
    /// // delete nested bucket
    /// bucket.delete_bucket("nested-bucket")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_bucket<T: ToBytes<'tx>>(&self, key: T) -> Result<()> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }

        let mut freelist = self.freelist.borrow_mut();
        let mut b = self.inner.borrow_mut();
        if b.deleted {
            panic!("Cannot delete bucket from a deleted bucket.");
        }
        b.delete_bucket(key, &mut freelist)
    }

    /// Get a cursor to iterate over the bucket.
    ///
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inspace::{DB, Data};
    /// # use inspace::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(false)?;
    ///
    /// let bucket = tx.get_bucket("my-bucket")?;
    ///
    /// for data in bucket.cursor() {
    ///     match data {
    ///         Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
    ///         Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
    ///     }
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn cursor<'a>(&'a self) -> Cursor<'b, 'tx> {
        {
            let b = self.inner.borrow();
            if b.deleted {
                panic!("Cannot create cursor from a deleted bucket.");
            }
        }
        Cursor::new(self)
    }

    /// Returns the next integer for the bucket.
    /// The integer is automatically incremented each time a new key is added to the bucket.
    /// You can it as a unique key for the bucket, since it will increment each time you add something new.
    /// It will not increment if you [`put`](#method.put) a key that already exists
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inspace::{DB};
    /// # use inspace::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(true)?;
    ///
    /// // create a root-level bucket
    /// let bucket = tx.create_bucket("my-bucket")?;
    /// // starts at 0
    /// assert_eq!(bucket.next_int(), 0);
    ///
    /// let next_int = bucket.next_int();
    /// bucket.put(next_int.to_be_bytes(), [0]);
    /// // auto-incremented after inserting a key / value pair
    /// assert_eq!(bucket.next_int(), 1);
    ///
    /// bucket.put(0_u64.to_be_bytes(), [0, 0]);
    /// // not incremented after updating a key / value pair
    /// assert_eq!(bucket.next_int(), 1);
    ///
    /// bucket.create_bucket("nested-bucket")?;
    /// // auto-incremented after creating a nested bucket
    /// assert_eq!(bucket.next_int(), 2);
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_int(&self) -> u64 {
        let b = self.inner.borrow();
        if b.deleted {
            panic!("Cannot get next int from a deleted bucket.");
        }
        b.meta.next_int
    }

    /// Iterator over the sub-buckets in this bucket.
    pub fn buckets<'a>(&'a self) -> impl Iterator<Item = (BucketName<'b, 'tx>, Bucket<'b, 'tx>)> {
        self.cursor().to_buckets()
    }

    /// Iterator over the key / value pairs in this bucket.
    pub fn kv_pairs<'a>(&'a self) -> impl Iterator<Item = KVPair<'b, 'tx>> {
        self.cursor().to_kv_pairs()
    }

    pub fn range<'a, R>(&'a self, r: R) -> Range<'a, 'b, 'tx, R>
    where
        R: RangeBounds<&'a [u8]>,
    {
        Range {
            c: self.cursor(),
            bounds: r,
            _phantom: PhantomData,
        }
    }
}

// and we'll implement IntoIterator
impl<'b, 'tx> IntoIterator for Bucket<'b, 'tx> {
    type Item = Data<'b, 'tx>;
    type IntoIter = Cursor<'b, 'tx>;

    fn into_iter(self) -> Self::IntoIter {
        self.cursor()
    }
}
