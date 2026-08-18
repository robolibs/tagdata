use std::marker::PhantomData;

use crate::{Bucket, CodecError, Cursor, Data, Error, KeyCodec, ValueCodec};

/// Opaque exclusive continuation point for a collection page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageToken(pub(crate) Vec<u8>);

impl PageToken {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// One bounded page and the token needed to resume after it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanPage<K, V> {
    pub items: Vec<(K, V)>,
    pub next: Option<PageToken>,
}

/// Lazy, allocation-bounded typed traversal of a collection.
pub struct CollectionIter<'b, 'tx, K, V, C> {
    pub(crate) cursor: Cursor<'b, 'tx>,
    pub(crate) expirations: Result<Option<Bucket<'b, 'tx>>, Error>,
    pub(crate) codec: C,
    pub(crate) remaining: Option<usize>,
    pub(crate) prefix: Option<Vec<u8>>,
    pub(crate) lower_bound: Option<(Vec<u8>, bool)>,
    pub(crate) upper_bound: Option<(Vec<u8>, bool)>,
    pub(crate) reverse: bool,
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
            let next = if self.reverse {
                self.cursor.previous()
            } else {
                self.cursor.next().transpose()
            };
            let next = match next {
                Ok(next) => next?,
                Err(error) => {
                    self.remaining = Some(0);
                    return Some(Err(error.into()));
                }
            };
            let Data::KeyValue(pair) = next else {
                continue;
            };
            if let Some(prefix) = &self.prefix
                && !pair.key().starts_with(prefix)
            {
                return None;
            }
            if self.reverse {
                if let Some((start, inclusive)) = &self.lower_bound
                    && (pair.key() < start.as_slice()
                        || (!inclusive && pair.key() == start.as_slice()))
                {
                    return None;
                }
            } else if let Some((end, inclusive)) = &self.upper_bound
                && (pair.key() > end.as_slice() || (!inclusive && pair.key() == end.as_slice()))
            {
                return None;
            }
            let live = match &self.expirations {
                Ok(Some(expirations)) => match expirations.key_is_live(pair.key()) {
                    Ok(live) => live,
                    Err(error) => return Some(Err(error.into())),
                },
                Ok(None) => true,
                Err(_) => {
                    let Err(error) = std::mem::replace(&mut self.expirations, Ok(None)) else {
                        unreachable!()
                    };
                    self.remaining = Some(0);
                    return Some(Err(error.into()));
                }
            };
            if !live {
                continue;
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
