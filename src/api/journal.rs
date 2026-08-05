use std::marker::PhantomData;

use crate::{
    Bucket, Change, ChangeOperation, ChangeSet, DB, Error, Result, WatchFilter,
    changes::{ChangePath, JOURNAL_BUCKET},
    tx::TxInner,
};

const CONFIG_KEY: &[u8] = b"config";
const TX_PREFIX: u8 = b't';
const CHECKPOINT_PREFIX: u8 = b'c';
const MAGIC: &[u8; 4] = b"ISJR";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalConfig {
    /// Maximum complete transactions retained. Checkpoints do not pin history.
    pub max_transactions: u64,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            max_transactions: 10_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalGap {
    pub requested_after: u64,
    pub oldest_available: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JournalReplay {
    pub transactions: Vec<ChangeSet>,
    pub gap: Option<JournalGap>,
    pub oldest_available: Option<u64>,
    pub newest_available: Option<u64>,
}

impl DB {
    /// Enables the durable journal. Records contain metadata only, never values.
    pub fn enable_journal(&self, config: JournalConfig) -> Result<()> {
        if config.max_transactions == 0 {
            return Err(invalid("journal retention must be non-zero"));
        }
        self.update(|tx| {
            tx.get_or_create_bucket(JOURNAL_BUCKET)?
                .put(CONFIG_KEY, config.max_transactions.to_be_bytes())?;
            Ok(())
        })
    }

    /// Replays complete transactions strictly after `transaction_id`.
    pub fn replay_journal(
        &self,
        transaction_id: u64,
        limit: usize,
        filter: Option<&WatchFilter>,
    ) -> Result<JournalReplay> {
        self.view(|tx| {
            let journal = match tx.get_bucket(JOURNAL_BUCKET) {
                Ok(bucket) => bucket,
                Err(Error::BucketMissing) => return Ok(JournalReplay::default()),
                Err(error) => return Err(error),
            };
            let records = journal
                .kv_pairs()
                .filter_map(|pair| decode_tx_key(pair.key()).map(|id| (id, pair.value().to_vec())))
                .collect::<Vec<_>>();
            let oldest_available = records.first().map(|record| record.0);
            let newest_available = records.last().map(|record| record.0);
            let gap = oldest_available
                .filter(|oldest| transaction_id.saturating_add(1) < *oldest)
                .map(|oldest_available| JournalGap {
                    requested_after: transaction_id,
                    oldest_available,
                });
            let mut transactions = Vec::new();
            for (id, bytes) in records {
                if id <= transaction_id || transactions.len() == limit {
                    continue;
                }
                let mut set = decode_change_set(id, &bytes)?;
                if let Some(filter) = filter {
                    set.changes.retain(|change| filter.matches(change));
                }
                transactions.push(set);
            }
            Ok(JournalReplay {
                transactions,
                gap,
                oldest_available,
                newest_available,
            })
        })
    }

    /// Stores a consumer checkpoint without preventing retention or gap detection.
    pub fn checkpoint_journal(&self, consumer: impl AsRef<[u8]>, tx_id: u64) -> Result<()> {
        let key = checkpoint_key(consumer.as_ref())?;
        self.update(|tx| {
            tx.get_bucket(JOURNAL_BUCKET)?
                .put(key, tx_id.to_be_bytes())?;
            Ok(())
        })
    }

    pub fn journal_checkpoint(&self, consumer: impl AsRef<[u8]>) -> Result<Option<u64>> {
        let key = checkpoint_key(consumer.as_ref())?;
        self.view(|tx| match tx.get_bucket(JOURNAL_BUCKET) {
            Ok(bucket) => bucket
                .get_kv(key)
                .map(|pair| decode_u64(pair.value()))
                .transpose(),
            Err(Error::BucketMissing) => Ok(None),
            Err(error) => Err(error),
        })
    }
}

pub(crate) fn persist(tx: &mut TxInner<'_>, changes: &ChangeSet) -> Result<()> {
    let inner = match tx.root.borrow_mut().get_bucket(JOURNAL_BUCKET) {
        Ok(inner) => inner,
        Err(Error::BucketMissing) => return Ok(()),
        Err(error) => return Err(error),
    };
    let journal = Bucket {
        inner,
        freelist: tx.freelist.clone(),
        writable: true,
        path: ChangePath::root(JOURNAL_BUCKET),
        changes: tx.changes.clone(),
        _phantom: PhantomData,
    };
    let retention = journal
        .get_kv(CONFIG_KEY)
        .ok_or_else(|| invalid("journal configuration is missing"))
        .and_then(|pair| decode_u64(pair.value()))?;
    journal.put(tx_key(changes.transaction_id), encode_change_set(changes)?)?;
    let mut keys = journal
        .kv_pairs()
        .filter_map(|pair| decode_tx_key(pair.key()).map(|_| pair.key().to_vec()))
        .collect::<Vec<_>>();
    let remove = keys.len().saturating_sub(retention as usize);
    for key in keys.drain(..remove) {
        journal.delete(key)?;
    }
    Ok(())
}

fn tx_key(tx_id: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = TX_PREFIX;
    key[1..].copy_from_slice(&tx_id.to_be_bytes());
    key
}

fn decode_tx_key(key: &[u8]) -> Option<u64> {
    (key.len() == 9 && key[0] == TX_PREFIX)
        .then(|| u64::from_be_bytes(key[1..].try_into().unwrap()))
}

fn checkpoint_key(consumer: &[u8]) -> Result<Vec<u8>> {
    if consumer.is_empty() || consumer.len() > 255 {
        return Err(invalid("journal consumer name must contain 1..=255 bytes"));
    }
    let mut key = Vec::with_capacity(1 + consumer.len());
    key.push(CHECKPOINT_PREFIX);
    key.extend_from_slice(consumer);
    Ok(key)
}

fn encode_change_set(set: &ChangeSet) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(u8::from(set.truncated));
    push_u32(&mut out, set.changes.len())?;
    for change in &set.changes {
        out.push(encode_operation(&change.operation));
        push_u16(&mut out, change.bucket_path.len())?;
        for part in &change.bucket_path {
            push_bytes(&mut out, part)?;
        }
        push_bytes(&mut out, &change.key)?;
    }
    Ok(out)
}

fn decode_change_set(transaction_id: u64, bytes: &[u8]) -> Result<ChangeSet> {
    let mut input = Input::new(bytes);
    if input.take(4)? != MAGIC {
        return Err(invalid("journal record magic is invalid"));
    }
    let truncated = input.byte()? != 0;
    let count = input.u32()? as usize;
    let mut changes = Vec::with_capacity(count);
    for _ in 0..count {
        let operation = decode_operation(input.byte()?)?;
        let path_count = input.u16()? as usize;
        let mut bucket_path = Vec::with_capacity(path_count);
        for _ in 0..path_count {
            bucket_path.push(input.bytes()?);
        }
        changes.push(Change {
            bucket_path,
            key: input.bytes()?,
            operation,
        });
    }
    if !input.remaining().is_empty() {
        return Err(invalid("journal record has trailing bytes"));
    }
    Ok(ChangeSet {
        transaction_id,
        changes,
        truncated,
    })
}

fn encode_operation(operation: &ChangeOperation) -> u8 {
    match operation {
        ChangeOperation::Put => 1,
        ChangeOperation::Delete => 2,
        ChangeOperation::BucketCreate => 3,
        ChangeOperation::BucketDelete => 4,
        ChangeOperation::Expire => 5,
    }
}

fn decode_operation(value: u8) -> Result<ChangeOperation> {
    match value {
        1 => Ok(ChangeOperation::Put),
        2 => Ok(ChangeOperation::Delete),
        3 => Ok(ChangeOperation::BucketCreate),
        4 => Ok(ChangeOperation::BucketDelete),
        5 => Ok(ChangeOperation::Expire),
        _ => Err(invalid("journal operation is invalid")),
    }
}

fn push_u16(out: &mut Vec<u8>, value: usize) -> Result<()> {
    out.extend_from_slice(
        &u16::try_from(value)
            .map_err(|_| invalid("journal path is too deep"))?
            .to_be_bytes(),
    );
    Ok(())
}

fn push_u32(out: &mut Vec<u8>, value: usize) -> Result<()> {
    out.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| invalid("journal record is too large"))?
            .to_be_bytes(),
    );
    Ok(())
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    push_u32(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn decode_u64(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        invalid("journal integer must contain eight bytes")
    })?))
}

struct Input<'a> {
    remaining: &'a [u8],
}

impl<'a> Input<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn remaining(&self) -> &'a [u8] {
        self.remaining
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        if self.remaining.len() < count {
            return Err(invalid("journal record is truncated"));
        }
        let (value, remaining) = self.remaining.split_at(count);
        self.remaining = remaining;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let length = self.u32()? as usize;
        Ok(self.take(length)?.to_vec())
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidDB(message.into())
}
