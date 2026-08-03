use crate::{BucketName, Data, KVPair};

/// An ordered bucket cursor.
pub struct Cursor<'tx> {
    items: Vec<Data<'tx>>,
    index: usize,
    started: bool,
}

impl<'tx> Cursor<'tx> {
    pub(crate) fn new(mut items: Vec<Data<'tx>>) -> Self {
        items.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        Self {
            items,
            index: 0,
            started: false,
        }
    }

    pub fn seek(&mut self, key: impl AsRef<[u8]>) -> bool {
        let key = key.as_ref();
        match self.items.binary_search_by(|entry| entry.key().cmp(key)) {
            Ok(index) => {
                self.index = index;
                self.started = false;
                true
            }
            Err(index) => {
                self.index = index;
                self.started = false;
                false
            }
        }
    }

    pub fn current(&self) -> Option<Data<'tx>> {
        self.items.get(self.index).copied()
    }
}

impl<'tx> Iterator for Cursor<'tx> {
    type Item = Data<'tx>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            self.index = self.index.saturating_add(1);
        } else {
            self.started = true;
        }
        self.current()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let next = self.index.saturating_add(usize::from(self.started));
        let remaining = self.items.len().saturating_sub(next);
        (remaining, Some(remaining))
    }
}

pub struct Range<'tx> {
    inner: std::vec::IntoIter<Data<'tx>>,
}

impl<'tx> Range<'tx> {
    pub(crate) fn new(items: Vec<Data<'tx>>) -> Self {
        Self {
            inner: items.into_iter(),
        }
    }
}

impl<'tx> Iterator for Range<'tx> {
    type Item = Data<'tx>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pub struct KVPairs<I> {
    inner: I,
}

impl<'tx, I> Iterator for KVPairs<I>
where
    I: Iterator<Item = Data<'tx>>,
{
    type Item = KVPair<'tx>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find_map(|entry| match entry {
            Data::KeyValue(pair) => Some(pair),
            Data::Bucket(_) => None,
        })
    }
}

pub trait ToKVPairs<'tx>: Iterator<Item = Data<'tx>> + Sized {
    fn to_kv_pairs(self) -> KVPairs<Self> {
        KVPairs { inner: self }
    }
}

impl<'tx, I> ToKVPairs<'tx> for I where I: Iterator<Item = Data<'tx>> + Sized {}

pub struct Buckets<I> {
    inner: I,
}

impl<'tx, I> Iterator for Buckets<I>
where
    I: Iterator<Item = Data<'tx>>,
{
    type Item = BucketName<'tx>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find_map(|entry| match entry {
            Data::Bucket(bucket) => Some(bucket),
            Data::KeyValue(_) => None,
        })
    }
}

pub trait ToBuckets<'tx>: Iterator<Item = Data<'tx>> + Sized {
    fn to_buckets(self) -> Buckets<Self> {
        Buckets { inner: self }
    }
}

impl<'tx, I> ToBuckets<'tx> for I where I: Iterator<Item = Data<'tx>> + Sized {}
