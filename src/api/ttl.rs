use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    Bucket, DB, Error, KVPair, Result, ToBytes,
    changes::{ChangeOperation, TTL_BUCKET, TTL_DEADLINES_BUCKET},
    node::Leaf,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TtlWriteResult {
    pub previous: Option<Vec<u8>>,
    pub expires_at_millis: u64,
}

impl<'b, 'tx> Bucket<'b, 'tx> {
    pub(crate) fn expiration_bucket(&self) -> Result<Option<Bucket<'b, 'tx>>> {
        match self.get_bucket(TTL_BUCKET) {
            Ok(bucket) => Ok(Some(bucket)),
            Err(Error::BucketMissing) => Ok(None),
            Err(error) => Err(error),
        }
    }

    #[cfg(feature = "typed")]
    pub(crate) fn key_is_live(&self, key: &[u8]) -> Result<bool> {
        match self.get_kv(key) {
            Some(expiration) => {
                Ok(decode_expiration(expiration.value())? > epoch_millis(SystemTime::now())?)
            }
            None => Ok(true),
        }
    }

    /// Writes a value and its persistent wall-clock expiration atomically.
    pub fn put_with_ttl<K, V>(
        &self,
        key: K,
        value: V,
        expires_at: SystemTime,
    ) -> Result<TtlWriteResult>
    where
        K: ToBytes<'tx>,
        V: ToBytes<'tx>,
    {
        let expires_at_millis = epoch_millis(expires_at)?;
        let key = key.to_bytes();
        let previous = self.put(&key, value)?.map(|pair| pair.value().to_vec());
        let expirations = self.get_or_create_bucket(TTL_BUCKET)?;
        if let Some(previous_deadline) = expirations.get_kv(&key) {
            let previous_deadline = decode_expiration(previous_deadline.value())?;
            remove_deadline(self, previous_deadline, key.as_ref())?;
        }
        let deadline_key = deadline_key(expires_at_millis, key.as_ref());
        expirations.put(key, expires_at_millis.to_be_bytes())?;
        self.get_or_create_bucket(TTL_DEADLINES_BUCKET)?
            .put(deadline_key, [])?;
        Ok(TtlWriteResult {
            previous,
            expires_at_millis,
        })
    }

    /// Reads a value only when it has not expired at the current wall clock.
    pub fn get_live<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<KVPair<'b, 'tx>>> {
        self.get_live_at(key, SystemTime::now())
    }

    /// Reads a value only when it has not expired at the supplied wall clock.
    pub fn get_live_at<K: AsRef<[u8]>>(
        &self,
        key: K,
        now: SystemTime,
    ) -> Result<Option<KVPair<'b, 'tx>>> {
        let key = key.as_ref();
        let Some(expiration) = self.expiration(key)? else {
            return Ok(self.get_kv(key));
        };
        if expiration <= epoch_millis(now)? {
            Ok(None)
        } else {
            Ok(self.get_kv(key))
        }
    }

    /// Removes a key's TTL while leaving its value intact.
    pub fn clear_ttl<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        let key = key.as_ref();
        match self.get_bucket(TTL_BUCKET) {
            Ok(expirations) => match expirations.delete(key) {
                Ok(previous) => {
                    remove_deadline(self, decode_expiration(previous.value())?, key)?;
                    Ok(true)
                }
                Err(Error::KeyValueMissing) => Ok(false),
                Err(error) => Err(error),
            },
            Err(Error::BucketMissing) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Lazily removes up to `limit` entries expired at `now`.
    pub fn purge_expired(&self, now: SystemTime, limit: usize) -> Result<usize> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        if limit == 0 {
            return Ok(0);
        }
        let now = epoch_millis(now)?;
        let expirations = match self.get_bucket(TTL_BUCKET) {
            Ok(bucket) => bucket,
            Err(Error::BucketMissing) => return Ok(0),
            Err(error) => return Err(error),
        };
        let deadlines = self.deadline_index(&expirations)?;
        let mut expired = Vec::with_capacity(limit);
        for pair in deadlines.kv_pairs() {
            let (deadline, key) = decode_deadline_key(pair.key())?;
            if deadline > now {
                break;
            }
            expired.push((pair.key().to_vec(), key.to_vec()));
            if expired.len() == limit {
                break;
            }
        }

        for (deadline_key, key) in &expired {
            let mut bucket = self.inner.borrow_mut();
            match bucket.get(key) {
                Some(Leaf::Kv(_, _)) => {
                    bucket.delete(key)?;
                    drop(bucket);
                    self.changes
                        .borrow_mut()
                        .record(&self.path, key, ChangeOperation::Expire);
                }
                Some(Leaf::Bucket(_, _)) => return Err(Error::IncompatibleValue),
                None => drop(bucket),
            }
            expirations.delete(key)?;
            deadlines.delete(deadline_key)?;
        }
        Ok(expired.len())
    }

    fn expiration(&self, key: &[u8]) -> Result<Option<u64>> {
        match self.expiration_bucket()? {
            Some(expirations) => expirations
                .get_kv(key)
                .map(|pair| decode_expiration(pair.value()))
                .transpose(),
            None => Ok(None),
        }
    }

    fn deadline_index(&self, expirations: &Bucket<'b, 'tx>) -> Result<Bucket<'b, 'tx>> {
        match self.get_bucket(TTL_DEADLINES_BUCKET) {
            Ok(deadlines) => Ok(deadlines),
            Err(Error::BucketMissing) => {
                let deadlines = self.create_bucket(TTL_DEADLINES_BUCKET)?;
                for pair in expirations.kv_pairs() {
                    let deadline = decode_expiration(pair.value())?;
                    deadlines.put(deadline_key(deadline, pair.key()), [])?;
                }
                Ok(deadlines)
            }
            Err(error) => Err(error),
        }
    }
}

impl DB {
    /// Removes at most `limit` expired records across every nested bucket.
    pub fn purge_expired(&self, now: SystemTime, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        self.update(|tx| {
            let names = tx
                .buckets()
                .map(|(name, _)| name.name().to_vec())
                .filter(|name| !is_ttl_bucket(name))
                .collect::<Vec<_>>();
            let mut removed = 0;
            for name in names {
                let bucket = tx.get_bucket(name)?;
                removed += purge_bucket_tree(&bucket, now, limit - removed)?;
                if removed == limit {
                    break;
                }
            }
            Ok(removed)
        })
    }
}

fn purge_bucket_tree(bucket: &Bucket<'_, '_>, now: SystemTime, limit: usize) -> Result<usize> {
    if limit == 0 {
        return Ok(0);
    }
    let mut removed = bucket.purge_expired(now, limit)?;
    if removed == limit {
        return Ok(removed);
    }
    let names = bucket
        .buckets()
        .map(|(name, _)| name.name().to_vec())
        .filter(|name| !is_ttl_bucket(name))
        .collect::<Vec<_>>();
    for name in names {
        let child = bucket.get_bucket(name)?;
        removed += purge_bucket_tree(&child, now, limit - removed)?;
        if removed == limit {
            break;
        }
    }
    Ok(removed)
}

fn is_ttl_bucket(name: &[u8]) -> bool {
    name == TTL_BUCKET || name == TTL_DEADLINES_BUCKET
}

fn remove_deadline(bucket: &Bucket<'_, '_>, deadline: u64, key: &[u8]) -> Result<()> {
    match bucket.get_bucket(TTL_DEADLINES_BUCKET) {
        Ok(deadlines) => match deadlines.delete(deadline_key(deadline, key)) {
            Ok(_) | Err(Error::KeyValueMissing) => Ok(()),
            Err(error) => Err(error),
        },
        Err(Error::BucketMissing) => Ok(()),
        Err(error) => Err(error),
    }
}

fn deadline_key(deadline: u64, key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(8 + key.len());
    encoded.extend_from_slice(&deadline.to_be_bytes());
    encoded.extend_from_slice(key);
    encoded
}

fn decode_deadline_key(encoded: &[u8]) -> Result<(u64, &[u8])> {
    let deadline = encoded
        .get(..8)
        .ok_or_else(|| Error::InvalidDB("TTL deadline key is shorter than eight bytes".into()))?;
    Ok((decode_expiration(deadline)?, &encoded[8..]))
}

fn epoch_millis(time: SystemTime) -> Result<u64> {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| invalid_time(error.to_string()))?
        .as_millis();
    u64::try_from(millis).map_err(|error| invalid_time(error.to_string()))
}

fn decode_expiration(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::InvalidDB("TTL expiration must contain eight bytes".into()))?;
    Ok(u64::from_be_bytes(bytes))
}

fn invalid_time(message: String) -> Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message).into()
}
