/// A bucket entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Data<'tx> {
    Bucket(BucketName<'tx>),
    KeyValue(KVPair<'tx>),
}

impl<'tx> Data<'tx> {
    pub fn is_kv(&self) -> bool {
        matches!(self, Self::KeyValue(_))
    }

    pub fn kv(&self) -> &KVPair<'tx> {
        match self {
            Self::KeyValue(pair) => pair,
            Self::Bucket(_) => panic!("entry is a bucket"),
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            Self::Bucket(bucket) => bucket.name(),
            Self::KeyValue(pair) => pair.key(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BucketName<'tx> {
    name: &'tx [u8],
}

impl<'tx> BucketName<'tx> {
    pub(crate) fn new(name: &'tx [u8]) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &'tx [u8] {
        self.name
    }
}

impl AsRef<[u8]> for BucketName<'_> {
    fn as_ref(&self) -> &[u8] {
        self.name
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KVPair<'tx> {
    key: &'tx [u8],
    value: &'tx [u8],
}

impl<'tx> KVPair<'tx> {
    pub(crate) fn new(key: &'tx [u8], value: &'tx [u8]) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> &'tx [u8] {
        self.key
    }

    pub fn value(&self) -> &'tx [u8] {
        self.value
    }

    pub fn kv(&self) -> (&'tx [u8], &'tx [u8]) {
        (self.key, self.value)
    }
}
