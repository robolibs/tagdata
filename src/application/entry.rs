use crate::{CodecError, KeyCodec, ValueCodec};

use super::WriteCollection;

/// Typed state of a key in a writable collection.
pub struct Entry<'a, 'b, 'tx, K, V, C> {
    pub(crate) collection: &'a WriteCollection<'b, 'tx, K, V, C>,
    pub(crate) key: K,
    pub(crate) current: Option<V>,
}

impl<'a, 'b, 'tx, K, V, C> Entry<'a, 'b, 'tx, K, V, C>
where
    C: KeyCodec<K> + ValueCodec<V> + Clone,
{
    pub fn is_occupied(&self) -> bool {
        self.current.is_some()
    }

    pub fn is_vacant(&self) -> bool {
        self.current.is_none()
    }

    pub fn get(&self) -> Option<&V> {
        self.current.as_ref()
    }

    pub fn and_modify(mut self, update: impl FnOnce(&mut V)) -> Result<Self, CodecError> {
        if let Some(value) = &mut self.current {
            update(value);
            self.collection.insert(&self.key, value)?;
        }
        Ok(self)
    }

    pub fn or_insert(self, default: V) -> Result<V, CodecError> {
        self.or_insert_with(|| default)
    }

    pub fn or_insert_with(self, default: impl FnOnce() -> V) -> Result<V, CodecError> {
        match self.current {
            Some(value) => Ok(value),
            None => {
                let value = default();
                self.collection.insert(&self.key, &value)?;
                Ok(value)
            }
        }
    }

    pub fn replace(mut self, value: V) -> Result<Option<V>, CodecError> {
        let previous = self.collection.insert(&self.key, &value)?;
        self.current = Some(value);
        Ok(previous)
    }

    pub fn remove(self) -> Result<Option<V>, CodecError> {
        self.collection.remove(&self.key)
    }
}

/// Owned result of a typed compare-and-exchange operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompareOutcome<V> {
    pub applied: bool,
    pub observed: Option<V>,
    pub current: Option<V>,
}
