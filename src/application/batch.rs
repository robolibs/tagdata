use std::time::SystemTime;

use crate::{Bucket, CodecError, KeyCodec, Tx, ValueCodec};

use super::{CollectionDef, WriteOptions};

enum BatchOperation<'b, 'tx> {
    Insert {
        bucket: Bucket<'b, 'tx>,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: Option<SystemTime>,
    },
    Remove {
        bucket: Bucket<'b, 'tx>,
        key: Vec<u8>,
    },
}

/// An encoded group of writes spanning any number of typed collections.
///
/// Applying the batch mutates the enclosing transaction; durability and
/// all-or-nothing publication are still controlled by that transaction.
pub struct Batch<'b, 'tx> {
    tx: &'b Tx<'tx>,
    operations: Vec<BatchOperation<'b, 'tx>>,
}

impl<'b, 'tx> Batch<'b, 'tx> {
    pub(crate) fn new(tx: &'b Tx<'tx>) -> Self {
        Self {
            tx,
            operations: Vec::new(),
        }
    }

    pub fn insert<K, V, C>(
        &mut self,
        definition: CollectionDef<K, V, C>,
        key: &K,
        value: &V,
    ) -> Result<&mut Self, CodecError>
    where
        C: KeyCodec<K> + ValueCodec<V> + Clone,
    {
        self.insert_with_options(definition, key, value, WriteOptions::default())
    }

    pub fn insert_with_options<K, V, C>(
        &mut self,
        definition: CollectionDef<K, V, C>,
        key: &K,
        value: &V,
        options: WriteOptions,
    ) -> Result<&mut Self, CodecError>
    where
        C: KeyCodec<K> + ValueCodec<V> + Clone,
    {
        let key = definition.codec.encode_key(key)?;
        let value = definition.codec.encode_value(value)?;
        let collection = self.tx.collection_mut(definition)?;
        self.operations.push(BatchOperation::Insert {
            bucket: collection.read.raw.clone_handle(),
            key,
            value,
            expires_at: options.expires_at,
        });
        Ok(self)
    }

    pub fn remove<K, V, C>(
        &mut self,
        definition: CollectionDef<K, V, C>,
        key: &K,
    ) -> Result<&mut Self, CodecError>
    where
        C: KeyCodec<K> + ValueCodec<V> + Clone,
    {
        let key = definition.codec.encode_key(key)?;
        let collection = self.tx.collection_mut(definition)?;
        self.operations.push(BatchOperation::Remove {
            bucket: collection.read.raw.clone_handle(),
            key,
        });
        Ok(self)
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn apply(self) -> Result<usize, CodecError> {
        let count = self.operations.len();
        for operation in self.operations {
            match operation {
                BatchOperation::Insert {
                    bucket,
                    key,
                    value,
                    expires_at,
                } => {
                    if let Some(expires_at) = expires_at {
                        bucket.put_with_ttl(key, value, expires_at)?;
                    } else {
                        bucket.put(key.clone(), value)?;
                        bucket.clear_ttl(&key)?;
                    }
                }
                BatchOperation::Remove { bucket, key } => {
                    if bucket.get_kv(&key)?.is_some() {
                        bucket.delete(&key)?;
                    }
                    bucket.clear_ttl(&key)?;
                }
            }
        }
        Ok(count)
    }
}
