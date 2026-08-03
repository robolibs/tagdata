use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    Bucket, Error, KVPair, Result, ToBytes,
    changes::{ChangeOperation, TTL_BUCKET},
    node::Leaf,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TtlWriteResult {
    pub previous: Option<Vec<u8>>,
    pub expires_at_millis: u64,
}

impl<'b, 'tx> Bucket<'b, 'tx> {
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
        expirations.put(key, expires_at_millis.to_be_bytes())?;
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
        match self.get_bucket(TTL_BUCKET) {
            Ok(expirations) => match expirations.delete(key) {
                Ok(_) => Ok(true),
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
        let mut expired = Vec::new();
        for pair in expirations.kv_pairs() {
            if decode_expiration(pair.value())? <= now {
                expired.push(pair.key().to_vec());
                if expired.len() == limit {
                    break;
                }
            }
        }

        for key in &expired {
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
        }
        Ok(expired.len())
    }

    fn expiration(&self, key: &[u8]) -> Result<Option<u64>> {
        match self.get_bucket(TTL_BUCKET) {
            Ok(expirations) => expirations
                .get_kv(key)
                .map(|pair| decode_expiration(pair.value()))
                .transpose(),
            Err(Error::BucketMissing) => Ok(None),
            Err(error) => Err(error),
        }
    }
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
