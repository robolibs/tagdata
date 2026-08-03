mod collection;
mod definition;
mod iter;

use std::marker::PhantomData;

use crate::{Bucket, CodecError, KeyCodec, Tx, ValueCodec};

pub use collection::{ReadCollection, WriteCollection};
pub use definition::{CollectionDef, OpenPolicy};
pub use iter::CollectionIter;

const SCHEMA_BUCKET: &str = "\0inspace.schema.v1";

impl<'tx> Tx<'tx> {
    /// Opens a reusable typed collection with read-only capabilities.
    pub fn collection<'b, K, V, C>(
        &'b self,
        definition: CollectionDef<K, V, C>,
    ) -> Result<ReadCollection<'b, 'tx, K, V, C>, CodecError>
    where
        C: KeyCodec<K> + ValueCodec<V>,
    {
        validate_definition(&definition)?;
        let raw = self.get_bucket(definition.name)?;
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
        let existed = self.get_bucket(definition.name).is_ok();
        let raw = match definition.policy {
            OpenPolicy::Existing => self.get_bucket(definition.name)?,
            OpenPolicy::Create => self.create_bucket(definition.name)?,
            OpenPolicy::CreateOrOpen => self.get_or_create_bucket(definition.name)?,
        };
        let schemas = self.get_or_create_bucket(SCHEMA_BUCKET)?;
        if existed {
            check_schema(&schemas, &definition)?;
        } else {
            schemas.put(definition.name, schema_bytes(&definition))?;
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

fn check_schema<K, V, C>(
    schemas: &Bucket<'_, '_>,
    definition: &CollectionDef<K, V, C>,
) -> Result<(), CodecError> {
    let expected = schema_bytes(definition);
    let Some(actual) = schemas.get_kv(definition.name) else {
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
