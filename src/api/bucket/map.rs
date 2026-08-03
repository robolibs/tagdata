use crate::{Data, KVPair, Result, ToBytes};

use super::Bucket;

impl<'b, 'tx> Bucket<'b, 'tx> {
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.get_kv(key).is_some()
    }

    pub fn insert<K, V>(&self, key: K, value: V) -> Result<Option<KVPair<'b, 'tx>>>
    where
        K: ToBytes<'tx>,
        V: ToBytes<'tx>,
    {
        self.put(key, value)
    }

    pub fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let Some(previous) = self.get_kv(key.as_ref()) else {
            return Ok(None);
        };
        let previous = previous.value().to_vec();
        self.delete(key)?;
        Ok(Some(previous))
    }

    pub fn first(&self) -> Option<KVPair<'b, 'tx>> {
        self.cursor().find_map(|data| match data {
            Data::KeyValue(pair) => Some(pair),
            Data::Bucket(_) => None,
        })
    }

    pub fn last(&self) -> Option<KVPair<'b, 'tx>> {
        let mut cursor = self.cursor();
        cursor.seek_last();
        while let Some(data) = cursor.previous() {
            if let Data::KeyValue(pair) = data {
                return Some(pair);
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.kv_pairs().count()
    }

    pub fn is_empty(&self) -> bool {
        self.kv_pairs().next().is_none()
    }

    pub fn clear(&self) -> Result<usize> {
        let keys = self
            .kv_pairs()
            .map(|pair| pair.key().to_vec())
            .collect::<Vec<_>>();
        for key in &keys {
            self.delete(key)?;
        }
        Ok(keys.len())
    }

    pub fn multi_get<I, K>(&self, keys: I) -> Vec<Option<Vec<u8>>>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        keys.into_iter()
            .map(|key| self.get_kv(key).map(|pair| pair.value().to_vec()))
            .collect()
    }

    pub fn delete_range(
        &self,
        start: impl AsRef<[u8]>,
        end_exclusive: impl AsRef<[u8]>,
    ) -> Result<usize> {
        let start = start.as_ref();
        let end = end_exclusive.as_ref();
        let keys = self
            .kv_pairs()
            .filter(|pair| pair.key() >= start && pair.key() < end)
            .map(|pair| pair.key().to_vec())
            .collect::<Vec<_>>();
        for key in &keys {
            self.delete(key)?;
        }
        Ok(keys.len())
    }

    pub fn delete_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<usize> {
        let prefix = prefix.as_ref();
        let keys = self
            .kv_pairs()
            .filter(|pair| pair.key().starts_with(prefix))
            .map(|pair| pair.key().to_vec())
            .collect::<Vec<_>>();
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
        let previous = self.get_kv(&key).map(|pair| pair.value().to_vec());
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
