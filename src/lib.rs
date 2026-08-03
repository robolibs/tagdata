//! `inspace` is a compact, transactional, memory-mapped key/value store.
//!
//! Reads borrow values directly from the mapping and allocate nothing. A database
//! permits concurrent readers or one writer. Writes are appended as checksummed
//! transactions and become visible only after the transaction is durable.

#![forbid(unsafe_op_in_unsafe_fn)]

mod db;
mod error;
mod format;

pub use db::{Bucket, Database, ReadTransaction, WriteTransaction};
pub use error::{Error, Result};
