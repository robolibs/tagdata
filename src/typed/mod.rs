mod codecs;

use std::{fmt, marker::PhantomData};

use crate::{Bucket, Data, Error};

pub use codecs::*;

/// Converts typed keys to and from their ordered byte representation.
pub trait KeyCodec<K> {
    /// Whether encoded byte order is the same as `K`'s desired order.
    const ORDER_PRESERVING: bool;

    fn encode_key(&self, key: &K) -> Result<Vec<u8>, CodecError>;
    fn decode_key(&self, bytes: &[u8]) -> Result<K, CodecError>;
}

/// Converts typed values to and from bytes.
pub trait ValueCodec<V> {
    fn encode_value(&self, value: &V) -> Result<Vec<u8>, CodecError>;
    fn decode_value(&self, bytes: &[u8]) -> Result<V, CodecError>;
}

/// Combines independent key and value codecs for [`TypedBucket`].
#[derive(Clone, Copy, Debug, Default)]
pub struct TypedCodec<KC, VC> {
    pub key: KC,
    pub value: VC,
}

impl<KC, VC> TypedCodec<KC, VC> {
    pub const fn new(key: KC, value: VC) -> Self {
        Self { key, value }
    }
}

impl<K, KC: KeyCodec<K>, VC> KeyCodec<K> for TypedCodec<KC, VC> {
    const ORDER_PRESERVING: bool = KC::ORDER_PRESERVING;

    fn encode_key(&self, key: &K) -> Result<Vec<u8>, CodecError> {
        self.key.encode_key(key)
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<K, CodecError> {
        self.key.decode_key(bytes)
    }
}

impl<V, KC, VC: ValueCodec<V>> ValueCodec<V> for TypedCodec<KC, VC> {
    fn encode_value(&self, value: &V) -> Result<Vec<u8>, CodecError> {
        self.value.encode_value(value)
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<V, CodecError> {
        self.value.decode_value(bytes)
    }
}

/// A typed view over a raw bucket. Raw and typed views share one transaction.
pub struct TypedBucket<'b, 'tx, K, V, C> {
    raw: Bucket<'b, 'tx>,
    codec: C,
    marker: PhantomData<(K, V)>,
}

impl<'b, 'tx, K, V, C> TypedBucket<'b, 'tx, K, V, C>
where
    C: KeyCodec<K> + ValueCodec<V>,
{
    pub fn new(raw: Bucket<'b, 'tx>, codec: C) -> Self {
        Self {
            raw,
            codec,
            marker: PhantomData,
        }
    }

    pub fn raw(&self) -> &Bucket<'b, 'tx> {
        &self.raw
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, CodecError> {
        let key = self.codec.encode_key(key)?;
        self.raw
            .get_kv(key)?
            .map(|pair| self.codec.decode_value(pair.value()))
            .transpose()
    }

    pub fn put(&self, key: &K, value: &V) -> Result<Option<V>, CodecError> {
        let key = self.codec.encode_key(key)?;
        let value = self.codec.encode_value(value)?;
        self.raw
            .put(key, value)?
            .map(|pair| self.codec.decode_value(pair.value()))
            .transpose()
    }

    pub fn delete(&self, key: &K) -> Result<V, CodecError> {
        let key = self.codec.encode_key(key)?;
        let previous = self.raw.delete(key)?;
        self.codec.decode_value(previous.value())
    }

    pub fn entries(&self) -> Result<Vec<(K, V)>, CodecError> {
        self.raw
            .kv_pairs()
            .map(|pair| {
                let pair = pair?;
                Ok((
                    self.codec.decode_key(pair.key())?,
                    self.codec.decode_value(pair.value())?,
                ))
            })
            .collect()
    }

    /// Returns an inclusive typed range when the key codec preserves ordering.
    pub fn range_inclusive(&self, start: &K, end: &K) -> Result<Vec<(K, V)>, CodecError> {
        if !C::ORDER_PRESERVING {
            return Err(CodecError::OrderingRequired);
        }
        let start = self.codec.encode_key(start)?;
        let end = self.codec.encode_key(end)?;
        self.raw
            .range(start.as_slice()..=end.as_slice())
            .filter_map(|data| match data {
                Ok(Data::KeyValue(pair)) => Some(Ok(pair)),
                Ok(Data::Bucket(_)) => None,
                Err(error) => Some(Err(error)),
            })
            .map(|pair| {
                let pair = pair?;
                Ok((
                    self.codec.decode_key(pair.key())?,
                    self.codec.decode_value(pair.value())?,
                ))
            })
            .collect()
    }
}

/// Structured storage and codec failures from typed access.
#[derive(Debug)]
pub enum CodecError {
    Storage(Error),
    Encode(String),
    Decode(String),
    OrderingRequired,
    InvalidDefinition,
    SchemaMissing {
        collection: &'static str,
    },
    SchemaMismatch {
        collection: &'static str,
        expected_id: &'static str,
        expected_version: u32,
    },
}

impl From<Error> for CodecError {
    fn from(value: Error) -> Self {
        Self::Storage(value)
    }
}

impl From<CodecError> for crate::TransactionError<CodecError> {
    fn from(value: CodecError) -> Self {
        match value {
            CodecError::Storage(error) => Self::Storage(error),
            application => Self::Application(application),
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "storage error: {error}"),
            Self::Encode(error) => write!(f, "encode error: {error}"),
            Self::Decode(error) => write!(f, "decode error: {error}"),
            Self::OrderingRequired => write!(f, "key codec does not preserve byte ordering"),
            Self::InvalidDefinition => write!(f, "invalid collection definition"),
            Self::SchemaMissing { collection } => {
                write!(f, "collection {collection:?} has no schema metadata")
            }
            Self::SchemaMismatch {
                collection,
                expected_id,
                expected_version,
            } => write!(
                f,
                "collection {collection:?} does not match schema {expected_id:?} version {expected_version}"
            ),
        }
    }
}

impl std::error::Error for CodecError {}
