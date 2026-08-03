use std::marker::PhantomData;

use crate::{Bucket, CodecError, KeyCodec, ValueCodec};

use super::CollectionIter;

/// Read capability for a typed collection.
///
/// It intentionally exposes no mutating methods.
///
/// ```compile_fail
/// use inspace::{ReadCollection, StringCodec, TypedCodec, U64Codec};
///
/// fn cannot_write(
///     users: &ReadCollection<'_, '_, u64, String, TypedCodec<U64Codec, StringCodec>>,
/// ) {
///     users.insert(&1, &"Ada".to_owned()).unwrap();
/// }
/// ```
pub struct ReadCollection<'b, 'tx, K, V, C> {
    pub(crate) raw: Bucket<'b, 'tx>,
    pub(crate) codec: C,
    pub(crate) marker: PhantomData<fn() -> (K, V)>,
}

impl<'b, 'tx, K, V, C> ReadCollection<'b, 'tx, K, V, C>
where
    C: KeyCodec<K> + ValueCodec<V> + Clone,
{
    pub fn get(&self, key: &K) -> Result<Option<V>, CodecError> {
        let key = self.codec.encode_key(key)?;
        self.raw
            .get_kv(key)
            .map(|pair| self.codec.decode_value(pair.value()))
            .transpose()
    }

    pub fn get_owned(&self, key: &K) -> Result<Option<V>, CodecError> {
        self.get(key)
    }

    pub fn contains_key(&self, key: &K) -> Result<bool, CodecError> {
        Ok(self.get(key)?.is_some())
    }

    pub fn multi_get<I>(&self, keys: I) -> Result<Vec<Option<V>>, CodecError>
    where
        I: IntoIterator<Item = K>,
    {
        keys.into_iter().map(|key| self.get(&key)).collect()
    }

    pub fn iter(&self) -> CollectionIter<'b, 'tx, K, V, C> {
        CollectionIter {
            cursor: self.raw.cursor(),
            codec: self.codec.clone(),
            remaining: None,
            prefix: None,
            end_exclusive: None,
            marker: PhantomData,
        }
    }

    pub fn prefix(&self, prefix: &[u8]) -> CollectionIter<'b, 'tx, K, V, C> {
        let mut cursor = self.raw.cursor();
        cursor.seek(prefix);
        CollectionIter {
            cursor,
            codec: self.codec.clone(),
            remaining: None,
            prefix: Some(prefix.to_vec()),
            end_exclusive: None,
            marker: PhantomData,
        }
    }

    pub fn range(
        &self,
        start: &K,
        end_exclusive: &K,
    ) -> Result<CollectionIter<'b, 'tx, K, V, C>, CodecError> {
        if !C::ORDER_PRESERVING {
            return Err(CodecError::OrderingRequired);
        }
        let start = self.codec.encode_key(start)?;
        let end_exclusive = self.codec.encode_key(end_exclusive)?;
        let mut cursor = self.raw.cursor();
        cursor.seek(&start);
        Ok(CollectionIter {
            cursor,
            codec: self.codec.clone(),
            remaining: None,
            prefix: None,
            end_exclusive: Some(end_exclusive),
            marker: PhantomData,
        })
    }

    pub fn first(&self) -> Result<Option<(K, V)>, CodecError> {
        self.iter().next().transpose()
    }

    pub fn len(&self) -> usize {
        self.raw.kv_pairs().count()
    }

    pub fn is_empty(&self) -> bool {
        self.raw.kv_pairs().next().is_none()
    }
}

/// Write capability for a typed collection.
pub struct WriteCollection<'b, 'tx, K, V, C> {
    pub(crate) read: ReadCollection<'b, 'tx, K, V, C>,
}

impl<'b, 'tx, K, V, C> WriteCollection<'b, 'tx, K, V, C>
where
    C: KeyCodec<K> + ValueCodec<V> + Clone,
{
    pub fn as_read(&self) -> &ReadCollection<'b, 'tx, K, V, C> {
        &self.read
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, CodecError> {
        self.read.get(key)
    }

    pub fn contains_key(&self, key: &K) -> Result<bool, CodecError> {
        self.read.contains_key(key)
    }

    pub fn iter(&self) -> CollectionIter<'b, 'tx, K, V, C> {
        self.read.iter()
    }

    pub fn insert(&self, key: &K, value: &V) -> Result<Option<V>, CodecError> {
        let key = self.read.codec.encode_key(key)?;
        let value = self.read.codec.encode_value(value)?;
        self.read
            .raw
            .put(key, value)?
            .map(|pair| self.read.codec.decode_value(pair.value()))
            .transpose()
    }

    pub fn remove(&self, key: &K) -> Result<Option<V>, CodecError> {
        let key = self.read.codec.encode_key(key)?;
        let Some(previous) = self.read.raw.get_kv(&key) else {
            return Ok(None);
        };
        let value = self.read.codec.decode_value(previous.value())?;
        self.read.raw.delete(key)?;
        Ok(Some(value))
    }

    pub fn insert_many<I>(&self, entries: I) -> Result<(), CodecError>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        for (key, value) in entries {
            self.insert(&key, &value)?;
        }
        Ok(())
    }

    pub fn remove_many<I>(&self, keys: I) -> Result<Vec<Option<V>>, CodecError>
    where
        I: IntoIterator<Item = K>,
    {
        keys.into_iter().map(|key| self.remove(&key)).collect()
    }

    pub fn clear(&self) -> Result<usize, CodecError> {
        let keys = self
            .read
            .raw
            .kv_pairs()
            .map(|pair| pair.key().to_vec())
            .collect::<Vec<_>>();
        for key in &keys {
            self.read.raw.delete(key)?;
        }
        Ok(keys.len())
    }
}
