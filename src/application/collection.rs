use std::{marker::PhantomData, ops::Add, time::SystemTime};

use crate::{Bucket, CodecError, KeyCodec, ValueCodec};

use super::{CollectionIter, CompareOutcome, Entry, PageToken, ScanPage};

/// Optional behavior attached to a typed write.
#[derive(Clone, Copy, Debug, Default)]
pub struct WriteOptions {
    pub expires_at: Option<SystemTime>,
}

impl WriteOptions {
    pub const fn expires_at(time: SystemTime) -> Self {
        Self {
            expires_at: Some(time),
        }
    }
}

/// Marker implemented only by codecs intended for numeric updates.
pub trait NumericValueCodec<N>: ValueCodec<N> {}

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
            .get_live(key)?
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
            raw: self.raw.clone_handle(),
            codec: self.codec.clone(),
            remaining: None,
            prefix: None,
            end_exclusive: None,
            reverse: false,
            marker: PhantomData,
        }
    }

    pub fn iter_rev(&self) -> CollectionIter<'b, 'tx, K, V, C> {
        let mut cursor = self.raw.cursor();
        cursor.seek_last();
        CollectionIter {
            cursor,
            raw: self.raw.clone_handle(),
            codec: self.codec.clone(),
            remaining: None,
            prefix: None,
            end_exclusive: None,
            reverse: true,
            marker: PhantomData,
        }
    }

    pub fn prefix(&self, prefix: &[u8]) -> CollectionIter<'b, 'tx, K, V, C> {
        let mut cursor = self.raw.cursor();
        cursor.seek(prefix);
        CollectionIter {
            cursor,
            raw: self.raw.clone_handle(),
            codec: self.codec.clone(),
            remaining: None,
            prefix: Some(prefix.to_vec()),
            end_exclusive: None,
            reverse: false,
            marker: PhantomData,
        }
    }

    pub fn seek(&self, key: &K) -> Result<CollectionIter<'b, 'tx, K, V, C>, CodecError> {
        if !C::ORDER_PRESERVING {
            return Err(CodecError::OrderingRequired);
        }
        let key = self.codec.encode_key(key)?;
        Ok(self.iter_from_encoded(&key, false))
    }

    /// Returns a bounded page. `after` is exclusive and can be passed from the
    /// prior page without decoding or retaining database-backed memory.
    pub fn page_after(
        &self,
        after: Option<&PageToken>,
        limit: usize,
    ) -> Result<ScanPage<K, V>, CodecError> {
        if !C::ORDER_PRESERVING {
            return Err(CodecError::OrderingRequired);
        }
        if limit == 0 {
            return Ok(ScanPage {
                items: Vec::new(),
                next: None,
            });
        }
        let mut iter = match after {
            Some(token) => self.iter_from_encoded(token.as_bytes(), true),
            None => self.iter(),
        };
        let mut items = Vec::with_capacity(limit);
        for _ in 0..limit {
            let Some(item) = iter.next() else { break };
            items.push(item?);
        }
        let has_more = iter.next().transpose()?.is_some();
        let next = if has_more {
            items
                .last()
                .map(|(key, _)| self.codec.encode_key(key).map(PageToken))
                .transpose()?
        } else {
            None
        };
        Ok(ScanPage { items, next })
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
            raw: self.raw.clone_handle(),
            codec: self.codec.clone(),
            remaining: None,
            prefix: None,
            end_exclusive: Some(end_exclusive),
            reverse: false,
            marker: PhantomData,
        })
    }

    pub fn first(&self) -> Result<Option<(K, V)>, CodecError> {
        self.iter().next().transpose()
    }

    pub fn last(&self) -> Result<Option<(K, V)>, CodecError> {
        self.iter_rev().next().transpose()
    }

    pub fn len(&self) -> usize {
        self.raw.kv_pairs().count()
    }

    pub fn is_empty(&self) -> bool {
        self.raw.kv_pairs().next().is_none()
    }

    fn iter_from_encoded(&self, key: &[u8], exclusive: bool) -> CollectionIter<'b, 'tx, K, V, C> {
        let mut cursor = self.raw.cursor();
        let exists = cursor.seek(key);
        if exclusive && exists {
            cursor.next();
        }
        CollectionIter {
            cursor,
            raw: self.raw.clone_handle(),
            codec: self.codec.clone(),
            remaining: None,
            prefix: None,
            end_exclusive: None,
            reverse: false,
            marker: PhantomData,
        }
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

    pub fn iter_rev(&self) -> CollectionIter<'b, 'tx, K, V, C> {
        self.read.iter_rev()
    }

    pub fn insert(&self, key: &K, value: &V) -> Result<Option<V>, CodecError> {
        self.insert_with_options(key, value, WriteOptions::default())
    }

    pub fn insert_with_options(
        &self,
        key: &K,
        value: &V,
        options: WriteOptions,
    ) -> Result<Option<V>, CodecError> {
        let key = self.read.codec.encode_key(key)?;
        let value = self.read.codec.encode_value(value)?;
        let previous = if let Some(expires_at) = options.expires_at {
            self.read
                .raw
                .put_with_ttl(key.clone(), value, expires_at)?
                .previous
        } else {
            let previous = self
                .read
                .raw
                .put(key.clone(), value)?
                .map(|pair| pair.value().to_vec());
            self.read.raw.clear_ttl(&key)?;
            previous
        };
        previous
            .map(|bytes| self.read.codec.decode_value(&bytes))
            .transpose()
    }

    pub fn remove(&self, key: &K) -> Result<Option<V>, CodecError> {
        let key = self.read.codec.encode_key(key)?;
        let Some(previous) = self.read.raw.get_kv(&key) else {
            return Ok(None);
        };
        let value = self.read.codec.decode_value(previous.value())?;
        self.read.raw.delete(&key)?;
        self.read.raw.clear_ttl(&key)?;
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

    /// Inserts strictly increasing input through one collection handle.
    pub fn insert_ordered<I>(&self, entries: I) -> Result<usize, CodecError>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        if !C::ORDER_PRESERVING {
            return Err(CodecError::OrderingRequired);
        }
        let mut previous = None::<Vec<u8>>;
        let mut count = 0;
        for (key, value) in entries {
            let key = self.read.codec.encode_key(&key)?;
            if previous.as_ref().is_some_and(|prior| prior >= &key) {
                return Err(CodecError::InputNotOrdered);
            }
            let value = self.read.codec.encode_value(&value)?;
            self.read.raw.put(key.clone(), value)?;
            self.read.raw.clear_ttl(&key)?;
            previous = Some(key);
            count += 1;
        }
        Ok(count)
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

    pub fn entry(&self, key: K) -> Result<Entry<'_, 'b, 'tx, K, V, C>, CodecError> {
        let current = self.get(&key)?;
        Ok(Entry {
            collection: self,
            key,
            current,
        })
    }

    pub fn compare_exchange(
        &self,
        key: &K,
        expected: Option<&V>,
        value: &V,
    ) -> Result<CompareOutcome<V>, CodecError> {
        let key = self.read.codec.encode_key(key)?;
        let expected = expected
            .map(|value| self.read.codec.encode_value(value))
            .transpose()?;
        let value = self.read.codec.encode_value(value)?;
        let result = self
            .read
            .raw
            .compare_exchange(key, expected.as_deref(), value)?;
        Ok(CompareOutcome {
            applied: result.applied,
            observed: result
                .observed
                .map(|bytes| self.read.codec.decode_value(&bytes))
                .transpose()?,
            current: result
                .current
                .map(|bytes| self.read.codec.decode_value(&bytes))
                .transpose()?,
        })
    }

    pub fn pop_first(&self) -> Result<Option<(K, V)>, CodecError> {
        let Some((key, value)) = self.read.first()? else {
            return Ok(None);
        };
        self.remove(&key)?;
        Ok(Some((key, value)))
    }

    pub fn pop_last(&self) -> Result<Option<(K, V)>, CodecError> {
        let Some((key, value)) = self.read.last()? else {
            return Ok(None);
        };
        self.remove(&key)?;
        Ok(Some((key, value)))
    }

    pub fn delete_range(&self, start: &K, end_exclusive: &K) -> Result<usize, CodecError> {
        let keys = self
            .read
            .range(start, end_exclusive)?
            .map(|entry| entry.map(|(key, _)| key))
            .collect::<Result<Vec<_>, _>>()?;
        for key in &keys {
            self.remove(key)?;
        }
        Ok(keys.len())
    }

    pub fn delete_prefix(&self, prefix: &[u8]) -> Result<usize, CodecError> {
        let keys = self
            .read
            .prefix(prefix)
            .map(|entry| entry.map(|(key, _)| key))
            .collect::<Result<Vec<_>, _>>()?;
        for key in &keys {
            self.remove(key)?;
        }
        Ok(keys.len())
    }
}

impl<'b, 'tx, K, V, C> WriteCollection<'b, 'tx, K, V, C>
where
    C: KeyCodec<K> + ValueCodec<V> + NumericValueCodec<V> + Clone,
    V: Copy + Default + Add<Output = V>,
{
    pub fn fetch_add(&self, key: &K, amount: V) -> Result<V, CodecError> {
        let previous = self.get(key)?.unwrap_or_default();
        self.insert(key, &(previous + amount))?;
        Ok(previous)
    }
}
