use crate::{Data, KVPair, Result, ToBytes};

use super::Bucket;

impl<'b, 'tx> Bucket<'b, 'tx> {
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        Ok(self.get_kv(key)?.is_some())
    }

    pub fn insert<K, V>(&self, key: K, value: V) -> Result<Option<KVPair<'b, 'tx>>>
    where
        K: ToBytes<'tx>,
        V: ToBytes<'tx>,
    {
        self.put(key, value)
    }

    pub fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let Some(previous) = self.get_kv(key.as_ref())? else {
            return Ok(None);
        };
        let previous = previous.value().to_vec();
        self.delete(key)?;
        Ok(Some(previous))
    }

    pub fn first(&self) -> Result<Option<KVPair<'b, 'tx>>> {
        for data in self.cursor() {
            if let Data::KeyValue(pair) = data? {
                return Ok(Some(pair));
            }
        }
        Ok(None)
    }

    pub fn last(&self) -> Result<Option<KVPair<'b, 'tx>>> {
        let mut cursor = self.cursor();
        cursor.seek_last()?;
        while let Some(data) = cursor.previous()? {
            if let Data::KeyValue(pair) = data {
                return Ok(Some(pair));
            }
        }
        Ok(None)
    }

    pub fn len(&self) -> Result<usize> {
        let mut count = 0;
        for pair in self.kv_pairs() {
            pair?;
            count += 1;
        }
        Ok(count)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.kv_pairs().next().transpose()?.is_none())
    }

    pub fn clear(&self) -> Result<usize> {
        let keys = self
            .kv_pairs()
            .map(|pair| Ok(pair?.key().to_vec()))
            .collect::<Result<Vec<_>>>()?;
        for key in &keys {
            self.delete(key)?;
        }
        Ok(keys.len())
    }

    pub fn multi_get<I, K>(&self, keys: I) -> Result<Vec<Option<Vec<u8>>>>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        keys.into_iter()
            .map(|key| Ok(self.get_kv(key)?.map(|pair| pair.value().to_vec())))
            .collect()
    }

    pub fn delete_range(
        &self,
        start: impl AsRef<[u8]>,
        end_exclusive: impl AsRef<[u8]>,
    ) -> Result<usize> {
        let start = start.as_ref();
        let end = end_exclusive.as_ref();
        let mut keys = Vec::new();
        for pair in self.kv_pairs() {
            let pair = pair?;
            if pair.key() >= start && pair.key() < end {
                keys.push(pair.key().to_vec());
            }
        }
        for key in &keys {
            self.delete(key)?;
        }
        Ok(keys.len())
    }

    pub fn delete_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<usize> {
        let prefix = prefix.as_ref();
        let mut keys = Vec::new();
        for pair in self.kv_pairs() {
            let pair = pair?;
            if pair.key().starts_with(prefix) {
                keys.push(pair.key().to_vec());
            }
        }
        for key in &keys {
            self.delete(key)?;
        }
        Ok(keys.len())
    }

    pub fn insert_many<I>(&self, entries: I) -> Result<usize>
    where
        I: IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        let mut count = 0;
        for (key, value) in entries {
            self.put(key, value)?;
            count += 1;
        }
        Ok(count)
    }

    /// Computes a replacement from the current bytes in this transaction.
    /// Returning `None` deletes an existing value. The returned value is the
    /// owned value observed before the update.
    pub fn update_value<F>(&self, key: impl AsRef<[u8]>, update: F) -> Result<Option<Vec<u8>>>
    where
        F: FnOnce(Option<&[u8]>) -> Option<Vec<u8>>,
    {
        let key = key.as_ref().to_vec();
        let previous = self.get_kv(&key)?.map(|pair| pair.value().to_vec());
        match update(previous.as_deref()) {
            Some(value) => {
                self.put(key.clone(), value)?;
                self.clear_ttl(&key)?;
            }
            None if previous.is_some() => {
                self.delete(&key)?;
                self.clear_ttl(&key)?;
            }
            None => {}
        }
        Ok(previous)
    }

    pub fn remove_many<I, K>(&self, keys: I) -> Result<usize>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        let mut count = 0;
        for key in keys {
            count += usize::from(self.remove(key)?.is_some());
        }
        Ok(count)
    }
}
