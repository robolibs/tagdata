#[cfg(feature = "changefeed")]
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex, mpsc},
};

#[cfg(feature = "changefeed")]
use crate::{DB, Result};

#[cfg(feature = "changefeed")]
const MAX_CHANGES: usize = 4096;
#[cfg(feature = "changefeed")]
const MAX_CHANGE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const TTL_BUCKET: &[u8] = b"\0tagdata.ttl.lookup";
pub(crate) const TTL_DEADLINES_BUCKET: &[u8] = b"\0tagdata.ttl.deadlines";
#[cfg(feature = "changefeed")]
pub(crate) const JOURNAL_BUCKET: &[u8] = b"\0tagdata.journal";

#[cfg(feature = "changefeed")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChangeOperation {
    Put,
    Delete,
    BucketCreate,
    BucketDelete,
    Expire,
}

#[cfg(feature = "changefeed")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Change {
    pub bucket_path: Vec<Vec<u8>>,
    pub key: Vec<u8>,
    pub operation: ChangeOperation,
}

#[cfg(feature = "changefeed")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeSet {
    pub transaction_id: u64,
    pub changes: Vec<Change>,
    /// True when the transaction exceeded the fixed change-memory budget.
    pub truncated: bool,
}

#[cfg(feature = "changefeed")]
pub struct WatchSubscription {
    receiver: mpsc::Receiver<ChangeSet>,
    terminal: Arc<Mutex<Option<WatchError>>>,
}

#[cfg(feature = "changefeed")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchError {
    Empty,
    Overflow,
    Disconnected,
}

/// Best-effort process-local watch selection.
#[cfg(feature = "changefeed")]
#[derive(Clone, Debug, Default)]
pub struct WatchFilter {
    bucket_path: Option<Vec<Vec<u8>>>,
    key_prefix: Option<Vec<u8>>,
    key_range: Option<(Vec<u8>, Vec<u8>)>,
    operations: Vec<ChangeOperation>,
}

#[cfg(feature = "changefeed")]
impl WatchFilter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a filter whose subscription begins with commits after subscribe.
    pub fn from_now() -> Self {
        Self::default()
    }

    pub fn collection(mut self, name: impl AsRef<[u8]>) -> Self {
        self.bucket_path = Some(vec![name.as_ref().to_vec()]);
        self
    }

    pub fn bucket_path<I, B>(mut self, path: I) -> Self
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.bucket_path = Some(
            path.into_iter()
                .map(|part| part.as_ref().to_vec())
                .collect(),
        );
        self
    }

    pub fn prefix(mut self, prefix: impl AsRef<[u8]>) -> Self {
        self.key_prefix = Some(prefix.as_ref().to_vec());
        self
    }

    pub fn range(
        mut self,
        start_inclusive: impl AsRef<[u8]>,
        end_exclusive: impl AsRef<[u8]>,
    ) -> Self {
        self.key_range = Some((
            start_inclusive.as_ref().to_vec(),
            end_exclusive.as_ref().to_vec(),
        ));
        self
    }

    pub fn operations(mut self, operations: impl IntoIterator<Item = ChangeOperation>) -> Self {
        self.operations = operations.into_iter().collect();
        self
    }

    pub(crate) fn matches(&self, change: &Change) -> bool {
        self.bucket_path
            .as_ref()
            .is_none_or(|path| path == &change.bucket_path)
            && self
                .key_prefix
                .as_ref()
                .is_none_or(|prefix| change.key.starts_with(prefix))
            && self
                .key_range
                .as_ref()
                .is_none_or(|(start, end)| change.key >= *start && change.key < *end)
            && (self.operations.is_empty() || self.operations.contains(&change.operation))
    }
}

#[cfg(feature = "changefeed")]
struct WatchSender {
    sender: mpsc::SyncSender<ChangeSet>,
    filter: WatchFilter,
    terminal: Arc<Mutex<Option<WatchError>>>,
}

#[cfg(feature = "changefeed")]
impl WatchSubscription {
    pub fn recv(&self) -> std::result::Result<ChangeSet, mpsc::RecvError> {
        self.receiver.recv()
    }

    pub fn try_recv(&self) -> std::result::Result<ChangeSet, mpsc::TryRecvError> {
        self.receiver.try_recv()
    }

    pub fn recv_event(&self) -> std::result::Result<ChangeSet, WatchError> {
        self.receiver.recv().map_err(|_| self.terminal_error())
    }

    pub fn try_recv_event(&self) -> std::result::Result<ChangeSet, WatchError> {
        self.receiver.try_recv().map_err(|error| match error {
            mpsc::TryRecvError::Empty => WatchError::Empty,
            mpsc::TryRecvError::Disconnected => self.terminal_error(),
        })
    }

    fn terminal_error(&self) -> WatchError {
        self.terminal
            .lock()
            .ok()
            .and_then(|reason| *reason)
            .unwrap_or(WatchError::Disconnected)
    }
}

#[cfg(feature = "changefeed")]
pub(crate) struct WatchHub {
    senders: Mutex<Vec<WatchSender>>,
}

#[cfg(feature = "changefeed")]
impl WatchHub {
    pub(crate) fn new() -> Self {
        Self {
            senders: Mutex::new(Vec::new()),
        }
    }

    fn subscribe(&self, capacity: usize, filter: WatchFilter) -> Result<WatchSubscription> {
        let (sender, receiver) = mpsc::sync_channel(capacity.max(1));
        let terminal = Arc::new(Mutex::new(None));
        self.senders.lock()?.push(WatchSender {
            sender,
            filter,
            terminal: terminal.clone(),
        });
        Ok(WatchSubscription { receiver, terminal })
    }

    pub(crate) fn publish(&self, changes: ChangeSet) {
        let Ok(mut senders) = self.senders.lock() else {
            return;
        };
        senders.retain(|subscription| {
            let selected = changes
                .changes
                .iter()
                .filter(|change| subscription.filter.matches(change))
                .cloned()
                .collect::<Vec<_>>();
            if selected.is_empty() && !changes.truncated {
                return true;
            }
            match subscription.sender.try_send(ChangeSet {
                transaction_id: changes.transaction_id,
                changes: selected,
                truncated: changes.truncated,
            }) {
                Ok(()) => true,
                Err(mpsc::TrySendError::Full(_)) => {
                    if let Ok(mut terminal) = subscription.terminal.lock() {
                        *terminal = Some(WatchError::Overflow);
                    }
                    false
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    if let Ok(mut terminal) = subscription.terminal.lock() {
                        *terminal = Some(WatchError::Disconnected);
                    }
                    false
                }
            }
        });
    }
}

#[derive(Clone)]
pub(crate) struct SharedChangeTracker {
    #[cfg(feature = "changefeed")]
    inner: Rc<RefCell<ChangeTracker>>,
}

#[derive(Clone, Default)]
pub(crate) struct ChangePath {
    #[cfg(feature = "changefeed")]
    inner: Vec<Vec<u8>>,
}

impl ChangePath {
    pub(crate) fn root(key: &[u8]) -> Self {
        #[cfg(feature = "changefeed")]
        {
            Self {
                inner: vec![key.to_vec()],
            }
        }
        #[cfg(not(feature = "changefeed"))]
        {
            let _ = key;
            Self {}
        }
    }

    pub(crate) fn child(&self, key: &[u8]) -> Self {
        #[cfg(feature = "changefeed")]
        {
            let mut inner = self.inner.clone();
            inner.push(key.to_vec());
            Self { inner }
        }
        #[cfg(not(feature = "changefeed"))]
        {
            let _ = key;
            Self {}
        }
    }

    #[cfg(feature = "changefeed")]
    fn as_slice(&self) -> &[Vec<u8>] {
        &self.inner
    }

    #[cfg(feature = "changefeed")]
    pub(crate) fn leaf(&self) -> &[u8] {
        self.inner.last().map_or(&[], Vec::as_slice)
    }
}

impl SharedChangeTracker {
    pub(crate) fn new(enabled: bool) -> Self {
        #[cfg(feature = "changefeed")]
        {
            Self {
                inner: Rc::new(RefCell::new(ChangeTracker {
                    enabled,
                    bytes: 0,
                    changes: Vec::new(),
                    truncated: false,
                })),
            }
        }
        #[cfg(not(feature = "changefeed"))]
        {
            let _ = enabled;
            Self {}
        }
    }

    #[cfg(feature = "changefeed")]
    pub(crate) fn record(&self, path: &ChangePath, key: &[u8], operation: ChangeOperation) {
        self.inner
            .borrow_mut()
            .record(path.as_slice(), key, operation);
    }

    #[cfg(feature = "changefeed")]
    pub(crate) fn finish(&self, transaction_id: u64) -> ChangeSet {
        self.inner.borrow_mut().finish(transaction_id)
    }

    #[cfg(feature = "changefeed")]
    pub(crate) fn snapshot(&self, transaction_id: u64) -> ChangeSet {
        self.inner.borrow().snapshot(transaction_id)
    }
}

#[cfg(feature = "changefeed")]
struct ChangeTracker {
    enabled: bool,
    bytes: usize,
    changes: Vec<Change>,
    truncated: bool,
}

#[cfg(feature = "changefeed")]
impl ChangeTracker {
    pub(crate) fn record(&mut self, path: &[Vec<u8>], key: &[u8], operation: ChangeOperation) {
        if !self.enabled
            || key == TTL_BUCKET
            || key == TTL_DEADLINES_BUCKET
            || key == JOURNAL_BUCKET
            || path.iter().any(|part| {
                part.as_slice() == TTL_BUCKET
                    || part.as_slice() == TTL_DEADLINES_BUCKET
                    || part.as_slice() == JOURNAL_BUCKET
            })
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

    pub(crate) fn snapshot(&self, transaction_id: u64) -> ChangeSet {
        ChangeSet {
            transaction_id,
            changes: self.changes.clone(),
            truncated: self.truncated,
        }
    }
}

#[cfg(feature = "changefeed")]
impl DB {
    /// Subscribes to best-effort, process-local committed change sets.
    ///
    /// A slow consumer is disconnected when its bounded queue fills. There is
    /// no durable replay; use transaction IDs to detect application-level gaps.
    pub fn watch(&self, capacity: usize) -> Result<WatchSubscription> {
        self.inner
            .watches
            .subscribe(capacity, WatchFilter::default())
    }

    /// Subscribes to committed changes selected by `filter`.
    pub fn watch_filtered(
        &self,
        capacity: usize,
        filter: WatchFilter,
    ) -> Result<WatchSubscription> {
        self.inner.watches.subscribe(capacity, filter)
    }
}
