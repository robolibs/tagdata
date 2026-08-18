use crate::{Error, Result, ToBytes, node::Leaf};

#[cfg(feature = "changefeed")]
use crate::changes::ChangeOperation;

use super::Bucket;

/// The owned result of an atomic bucket mutation.
///
/// `observed` is the value present when the operation was evaluated. `current`
/// is the value after evaluation. Owned bytes allow the result to outlive the
/// bucket borrow while the enclosing transaction still determines persistence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtomicResult {
    pub applied: bool,
    pub observed: Option<Vec<u8>>,
    pub current: Option<Vec<u8>>,
}

impl<'b, 'tx> Bucket<'b, 'tx> {
    /// Inserts `value` only when `key` is missing.
    ///
    /// A missing key is inserted. An existing byte value is reported as a
    /// conflict, and an existing nested bucket returns [`Error::IncompatibleValue`].
    pub fn put_if_absent<K, V>(&self, key: K, value: V) -> Result<AtomicResult>
    where
        K: ToBytes<'tx>,
        V: ToBytes<'tx>,
    {
        self.ensure_atomic_write()?;
        let key = key.to_bytes();
        #[cfg(feature = "changefeed")]
        let change_key = key.clone();
        let value = value.to_bytes();
        let mut bucket = self.inner.borrow_mut();
        match bucket.get(key.as_ref())? {
            Some(Leaf::Bucket(_, _)) => Err(Error::IncompatibleValue),
            Some(Leaf::Kv(_, current)) => {
                let current = current.as_ref().to_vec();
                Ok(AtomicResult {
                    applied: false,
                    observed: Some(current.clone()),
                    current: Some(current),
                })
            }
            None => {
                let current = value.as_ref().to_vec();
                bucket.put(key, value)?;
                #[cfg(feature = "changefeed")]
                self.changes
                    .record(&self.path, change_key.as_ref(), ChangeOperation::Put);
                Ok(AtomicResult {
                    applied: true,
                    observed: None,
                    current: Some(current),
                })
            }
        }
    }

    /// Replaces a value only when its current bytes equal `expected`.
    ///
    /// `expected == None` matches only a missing key, allowing an atomic insert.
    pub fn compare_exchange<K, V>(
        &self,
        key: K,
        expected: Option<&[u8]>,
        value: V,
    ) -> Result<AtomicResult>
    where
        K: ToBytes<'tx>,
        V: ToBytes<'tx>,
    {
        self.ensure_atomic_write()?;
        let key = key.to_bytes();
        #[cfg(feature = "changefeed")]
        let change_key = key.clone();
        let value = value.to_bytes();
        let mut bucket = self.inner.borrow_mut();
        let observed = match bucket.get(key.as_ref())? {
            Some(Leaf::Bucket(_, _)) => return Err(Error::IncompatibleValue),
            Some(Leaf::Kv(_, current)) => Some(current.as_ref().to_vec()),
            None => None,
        };
        if observed.as_deref() != expected {
            return Ok(AtomicResult {
                applied: false,
                observed: observed.clone(),
                current: observed,
            });
        }

        let current = value.as_ref().to_vec();
        bucket.put(key, value)?;
        #[cfg(feature = "changefeed")]
        self.changes
            .record(&self.path, change_key.as_ref(), ChangeOperation::Put);
        Ok(AtomicResult {
            applied: true,
            observed,
            current: Some(current),
        })
    }

    /// Deletes `key` only when its current bytes equal `expected`.
    ///
    /// A missing key is a conflict rather than an error. Nested buckets return
    /// [`Error::IncompatibleValue`].
    pub fn delete_if_value<K>(&self, key: K, expected: &[u8]) -> Result<AtomicResult>
    where
        K: ToBytes<'tx>,
    {
        self.ensure_atomic_write()?;
        let key = key.to_bytes();
        let mut bucket = self.inner.borrow_mut();
        let observed = match bucket.get(key.as_ref())? {
            Some(Leaf::Bucket(_, _)) => return Err(Error::IncompatibleValue),
            Some(Leaf::Kv(_, current)) => Some(current.as_ref().to_vec()),
            None => None,
        };
        if observed.as_deref() != Some(expected) {
            return Ok(AtomicResult {
                applied: false,
                observed: observed.clone(),
                current: observed,
            });
        }

        bucket.delete(key.as_ref())?;
        #[cfg(feature = "changefeed")]
        self.changes
            .record(&self.path, key.as_ref(), ChangeOperation::Delete);
        Ok(AtomicResult {
            applied: true,
            observed,
            current: None,
        })
    }

    fn ensure_atomic_write(&self) -> Result<()> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        let bucket = self.inner.borrow();
        if bucket.deleted {
            panic!("Cannot mutate data in a deleted bucket.");
        }
        Ok(())
    }
}
