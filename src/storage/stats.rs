/// A point-in-time snapshot of database storage and transaction state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    pub file_bytes: u64,
    pub page_size: u64,
    pub allocated_pages: u64,
    pub free_pages: u64,
    pub pending_pages: u64,
    pub reader_pinned_pages: u64,
    pub current_tx_id: u64,
    pub active_readers: u64,
    pub oldest_reader_tx_id: Option<u64>,
    pub committed_transactions: u64,
    pub bytes_written: u64,
}
