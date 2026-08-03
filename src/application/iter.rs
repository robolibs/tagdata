use std::marker::PhantomData;

use crate::{CodecError, Cursor, Data, KeyCodec, ValueCodec};

/// Lazy, allocation-bounded typed traversal of a collection.
pub struct CollectionIter<'b, 'tx, K, V, C> {
    pub(crate) cursor: Cursor<'b, 'tx>,
    pub(crate) codec: C,
    pub(crate) remaining: Option<usize>,
    pub(crate) prefix: Option<Vec<u8>>,
    pub(crate) end_exclusive: Option<Vec<u8>>,
    pub(crate) marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C> Iterator for CollectionIter<'_, '_, K, V, C>
where
    C: KeyCodec<K> + ValueCodec<V>,
{
    type Item = Result<(K, V), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == Some(0) {
            return None;
        }
        loop {
            let Data::KeyValue(pair) = self.cursor.next()? else {
                continue;
            };
            if let Some(prefix) = &self.prefix
                && !pair.key().starts_with(prefix)
            {
                return None;
            }
            if let Some(end) = &self.end_exclusive
                && pair.key() >= end.as_slice()
            {
                return None;
            }
            if let Some(remaining) = &mut self.remaining {
                *remaining -= 1;
            }
            return Some((|| {
                Ok((
                    self.codec.decode_key(pair.key())?,
                    self.codec.decode_value(pair.value())?,
                ))
            })());
        }
    }
}

impl<'b, 'tx, K, V, C> CollectionIter<'b, 'tx, K, V, C> {
    /// Bounds the number of decoded records produced by this scan.
    pub fn take_records(mut self, limit: usize) -> Self {
        self.remaining = Some(limit);
        self
    }
}
