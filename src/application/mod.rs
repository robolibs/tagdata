mod batch;
mod collection;
mod definition;
mod entry;
mod iter;

use std::marker::PhantomData;

use crate::{Bucket, CodecError, KeyCodec, Tx, ValueCodec};

pub use batch::Batch;
pub use collection::{NumericValueCodec, ReadCollection, WriteCollection, WriteOptions};
pub use definition::{CollectionDef, OpenPolicy};
pub use entry::{CompareOutcome, Entry};
pub use iter::{CollectionIter, PageToken, ScanPage};

const SCHEMA_BUCKET: &str = "\0inspace.schema.v1";

impl<'tx> Tx<'tx> {
    /// Starts an encoded multi-collection batch in this write transaction.
    pub fn batch(&self) -> Batch<'_, 'tx> {
        Batch::new(self)
    }

    /// Opens a reusable typed collection with read-only capabilities.
    pub fn collection<'b, K, V, C>(
        &'b self,
        definition: CollectionDef<K, V, C>,
    ) -> Result<ReadCollection<'b, 'tx, K, V, C>, CodecError>
    where
        C: KeyCodec<K> + ValueCodec<V>,
    {
        validate_definition(&definition)?;
        let raw = open_read(self, definition.parents, definition.name)?;
        let schemas = self.get_bucket(SCHEMA_BUCKET)?;
        check_schema(&schemas, &definition)?;
        Ok(ReadCollection {
            raw,
            codec: definition.codec,
            marker: PhantomData,
        })
    }

    /// Opens a reusable typed collection with write capabilities.
    pub fn collection_mut<'b, K, V, C>(
        &'b self,
        definition: CollectionDef<K, V, C>,
    ) -> Result<WriteCollection<'b, 'tx, K, V, C>, CodecError>
    where
        C: KeyCodec<K> + ValueCodec<V>,
    {
        validate_definition(&definition)?;
        let existing = open_read(self, definition.parents, definition.name);
        let existed = existing.is_ok();
        let raw = match definition.policy {
            OpenPolicy::Existing => existing?,
            OpenPolicy::Create => open_write(self, definition.parents, definition.name, true)?,
            OpenPolicy::CreateOrOpen => {
                open_write(self, definition.parents, definition.name, false)?
            }
        };
        let schemas = self.get_or_create_bucket(SCHEMA_BUCKET)?;
        if existed {
            check_schema(&schemas, &definition)?;
        } else {
            schemas.put(schema_key(&definition), schema_bytes(&definition))?;
        }
        Ok(WriteCollection {
            read: ReadCollection {
                raw,
                codec: definition.codec,
                marker: PhantomData,
            },
        })
    }
}

fn validate_definition<K, V, C>(definition: &CollectionDef<K, V, C>) -> Result<(), CodecError> {
    if definition.name.is_empty()
        || definition.name == SCHEMA_BUCKET
        || definition.parents.iter().any(|part| part.is_empty())
        || definition.schema_id.is_empty()
        || definition.schema_version == 0
    {
        return Err(CodecError::InvalidDefinition);
    }
    Ok(())
}

fn schema_bytes<K, V, C>(definition: &CollectionDef<K, V, C>) -> Vec<u8> {
    let mut bytes = definition.schema_version.to_be_bytes().to_vec();
    bytes.extend_from_slice(definition.schema_id.as_bytes());
    bytes
}

fn schema_key<K, V, C>(definition: &CollectionDef<K, V, C>) -> Vec<u8> {
    let mut key = Vec::new();
    for part in definition
        .parents
        .iter()
        .copied()
        .chain(std::iter::once(definition.name))
    {
        key.extend_from_slice(&(part.len() as u32).to_be_bytes());
        key.extend_from_slice(part.as_bytes());
    }
    key
}

fn check_schema<K, V, C>(
    schemas: &Bucket<'_, '_>,
    definition: &CollectionDef<K, V, C>,
) -> Result<(), CodecError> {
    let expected = schema_bytes(definition);
    let Some(actual) = schemas.get_kv(schema_key(definition)) else {
        return Err(CodecError::SchemaMissing {
            collection: definition.name,
        });
    };
    if actual.value() != expected {
        return Err(CodecError::SchemaMismatch {
            collection: definition.name,
            expected_id: definition.schema_id,
            expected_version: definition.schema_version,
        });
    }
    Ok(())
}

fn open_read<'b, 'tx>(
    tx: &'b Tx<'tx>,
    parents: &'static [&'static str],
    name: &'static str,
) -> crate::Result<Bucket<'b, 'tx>> {
    let Some((first, rest)) = parents.split_first() else {
        return tx.get_bucket(name);
    };
    let mut bucket = tx.get_bucket(*first)?;
    for part in rest {
        bucket = bucket.get_bucket(*part)?;
    }
    bucket.get_bucket(name)
}

fn open_write<'b, 'tx>(
    tx: &'b Tx<'tx>,
    parents: &'static [&'static str],
    name: &'static str,
    create_only: bool,
) -> crate::Result<Bucket<'b, 'tx>> {
    let Some((first, rest)) = parents.split_first() else {
        return if create_only {
            tx.create_bucket(name)
        } else {
            tx.get_or_create_bucket(name)
        };
    };
    let mut bucket = tx.get_or_create_bucket(*first)?;
    for part in rest {
        bucket = bucket.get_or_create_bucket(*part)?;
    }
    if create_only {
        bucket.create_bucket(name)
    } else {
        bucket.get_or_create_bucket(name)
    }
}
