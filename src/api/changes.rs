use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Mutex, mpsc},
};

use crate::{DB, Result};

const MAX_CHANGES: usize = 4096;
const MAX_CHANGE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const TTL_BUCKET: &[u8] = b"\0inspace.ttl.v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChangeOperation {
    Put,
    Delete,
    BucketCreate,
    BucketDelete,
    Expire,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Change {
    pub bucket_path: Vec<Vec<u8>>,
    pub key: Vec<u8>,
    pub operation: ChangeOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeSet {
    pub transaction_id: u64,
    pub changes: Vec<Change>,
    /// True when the transaction exceeded the fixed change-memory budget.
    pub truncated: bool,
}

pub struct WatchSubscription {
    receiver: mpsc::Receiver<ChangeSet>,
}

impl WatchSubscription {
    pub fn recv(&self) -> std::result::Result<ChangeSet, mpsc::RecvError> {
        self.receiver.recv()
    }

    pub fn try_recv(&self) -> std::result::Result<ChangeSet, mpsc::TryRecvError> {
        self.receiver.try_recv()
    }
}

pub(crate) struct WatchHub {
    senders: Mutex<Vec<mpsc::SyncSender<ChangeSet>>>,
}

impl WatchHub {
    pub(crate) fn new() -> Self {
        Self {
            senders: Mutex::new(Vec::new()),
        }
    }

    fn subscribe(&self, capacity: usize) -> Result<WatchSubscription> {
        let (sender, receiver) = mpsc::sync_channel(capacity.max(1));
        self.senders.lock()?.push(sender);
        Ok(WatchSubscription { receiver })
    }

    pub(crate) fn publish(&self, changes: ChangeSet) {
        let Ok(mut senders) = self.senders.lock() else {
            return;
        };
        senders.retain(|sender| sender.try_send(changes.clone()).is_ok());
    }
}

pub(crate) struct ChangeTracker {
    enabled: bool,
    bytes: usize,
    changes: Vec<Change>,
    truncated: bool,
}

impl ChangeTracker {
    pub(crate) fn shared(enabled: bool) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            enabled,
            bytes: 0,
            changes: Vec::new(),
            truncated: false,
        }))
    }

    pub(crate) fn record(&mut self, path: &[Vec<u8>], key: &[u8], operation: ChangeOperation) {
        if !self.enabled
            || key == TTL_BUCKET
            || path.iter().any(|part| part.as_slice() == TTL_BUCKET)
        {
            return;
        }
        let bytes = path.iter().map(Vec::len).sum::<usize>() + key.len();
        if self.changes.len() == MAX_CHANGES || self.bytes.saturating_add(bytes) > MAX_CHANGE_BYTES
        {
            self.truncated = true;
            return;
        }
        self.bytes += bytes;
        self.changes.push(Change {
            bucket_path: path.to_vec(),
            key: key.to_vec(),
            operation,
        });
    }

    pub(crate) fn finish(&mut self, transaction_id: u64) -> ChangeSet {
        ChangeSet {
            transaction_id,
            changes: std::mem::take(&mut self.changes),
            truncated: self.truncated,
        }
    }
}

impl DB {
    /// Subscribes to best-effort, process-local committed change sets.
    ///
    /// A slow consumer is disconnected when its bounded queue fills. There is
    /// no durable replay; use transaction IDs to detect application-level gaps.
    pub fn watch(&self, capacity: usize) -> Result<WatchSubscription> {
        self.inner.watches.subscribe(capacity)
    }
}
