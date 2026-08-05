//! # Just Another Memory Mapped Database
//!
//! Tagdata is an embedded, single-file database that stores key/value pairs as bytes.
//!
//! It started life as a Rust port of [Ben Johnson's](https://twitter.com/benbjohnson) [BoltDB](https://github.com/boltdb/bolt),
//! which was inspired by [Howard Chu's](https://twitter.com/hyc_symas) [LMDB](http://symas.com/mdb/),
//! so please check out both of these awesome projects!
//!
//! Tagdata offers
//! [ACID](https://en.wikipedia.org/wiki/ACID) compliance,
//! [serializable](https://en.wikipedia.org/wiki/Serializability) and
//! [isolated](https://en.wikipedia.org/wiki/Isolation_(database_systems)) transactions,
//! with multiple lock-free readers and a single concurrent writer. The data is organized in a
//! [single level](https://en.wikipedia.org/wiki/Single-level_store) [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)
//! so random and sequential reads are very fast. The underlying file is [memory mapped](https://en.wikipedia.org/wiki/Memory-mapped_file), so reads require no additional memory allocation.
//!
//! Tagdata has a small exported API. It stores data in collections called [`Buckets`](struct.Bucket.html),
//! and each bucket can contain unique keys mapped to byte values or nested buckets.
//! There are also more examples in the docs, be sure to check out
//! * Using a [`Cursor`] to iterate over the data in a bucket
//! * How to create and use multiple [`Tx`]s
//! * Nested [`Buckets`](struct.Bucket.html)
//! * [`OpenOptions`](struct.OpenOptions.html) to provide parameters for opening a [`DB`](struct.DB.html)
//!
//! # Examples
//!
//! ## Simple put and get
//! ```no_run
//! use tagdata::{DB, Data, Error};
//!
//! fn main() -> Result<(), Error> {
//! {
//!     // open a new database file
//!     let db = DB::open("my-database.db")?;
//!
//!     // open a writable transaction so we can make changes
//!     let tx = db.tx(true)?;
//!
//!     // create a bucket to store a map of first names to last names
//!     let mut names_bucket = tx.create_bucket("names")?;
//!     names_bucket.put("Kanan", "Jarrus")?;
//!     names_bucket.put("Ezra", "Bridger")?;
//!
//!     // commit the changes so they are saved to disk
//!     tx.commit()?;
//! }
//! {
//!     // open the existing database file
//!     let db = DB::open("my-database.db")?;
//!     // open a read-only transaction to get the data
//!     let tx = db.tx(false)?;
//!     // get the bucket we created in the last transaction
//!     let names_bucket = tx.get_bucket("names")?;
//!     // get the key / value pair we inserted into the bucket
//!     if let Some(data) = names_bucket.get("Kanan") {
//!         assert_eq!(data.kv().value(), b"Jarrus");
//!     }
//! }
//!     Ok(())
//! }
//! ```
//!
//! ## Storing structs
//! ```no_run
//! use tagdata::{DB, Data, Error};
//! use serde::{Deserialize, Serialize};
//! // use rmps crate to serialize structs using the MessagePack format
//! use rmp_serde::{Deserializer, Serializer};
//!
//! #[derive(Debug, PartialEq, Deserialize, Serialize)]
//! struct User {
//!     username: String,
//!     password: String,
//! }
//!
//! fn main() -> Result<(), Error> {
//!     let user = User{
//!         username: "my-user".to_string(),
//!         password: "my-password".to_string(),
//!     };
//! {
//!     // open a new database file and start a writable transaction
//!     let db = DB::open("my-database.db")?;
//!     let mut tx = db.tx(true)?;
//!
//!     // create a bucket to store users
//!     let mut users_bucket = tx.create_bucket("users")?;
//!
//!     // serialize struct to bytes and store in bucket
//!     let user_bytes = rmp_serde::to_vec(&user).unwrap();
//!     users_bucket.put("user1", user_bytes)?;
//!
//!     // commit the changes so they are saved to disk
//!     tx.commit()?;
//! }
//! {
//!     // open the existing database file
//!     let db = DB::open("my-database.db")?;
//!     // open a read-only transaction to get the data
//!     let tx = db.tx(false)?;
//!     // get the bucket we created in the last transaction
//!     let users_bucket = tx.get_bucket("users")?;
//!     // get the key / value pair we inserted into the bucket
//!     if let Some(data) = users_bucket.get(b"user1") {
//!         // deserialize into a user struct
//!         let db_user: User = rmp_serde::from_slice(data.kv().value()).unwrap();
//!         assert_eq!(db_user, user);
//!     }
//! }
//!     Ok(())
//! }
//!

mod api;
mod storage;
mod support;
mod tree;

#[cfg(feature = "changefeed")]
pub(crate) use api::journal;
pub(crate) use api::merge;
pub(crate) use api::ttl;
pub(crate) use api::{bucket, changes, cursor, data, scoped, tx};
pub(crate) use storage::format;
#[cfg(feature = "operator")]
pub(crate) use storage::operator;
pub(crate) use storage::verification;
pub(crate) use storage::{coordination, db, freelist, meta, page, stats};
pub(crate) use support::{bytes, errors};
pub(crate) use tree::{node, page_node};

#[cfg(feature = "typed")]
mod typed;

#[cfg(feature = "typed")]
mod application;

pub use bucket::{AtomicResult, Bucket};
#[cfg(feature = "changefeed")]
pub use changes::{Change, ChangeOperation, ChangeSet, WatchError, WatchFilter, WatchSubscription};
pub use cursor::{Buckets, Cursor, KVPairs, ToBuckets, ToKVPairs};
pub use data::*;
pub use db::{DB, FORMAT_VERSION, OpenOptions, WriteVerification};
pub use errors::*;
pub use format::FormatInfo;
#[cfg(feature = "changefeed")]
pub use journal::{JournalConfig, JournalGap, JournalReplay};
pub use merge::{MergeConflictPolicy, MergeOptions, MergeReport};
#[cfg(feature = "operator")]
pub use operator::{SalvageManifest, SkippedRecord};
pub use scoped::TransactionError;
pub use stats::Stats;
pub use ttl::TtlWriteResult;
pub use tx::Tx;
pub use verification::{VerifyIssue, VerifyReport};

#[cfg(feature = "typed")]
pub use typed::*;

#[cfg(feature = "typed")]
pub use application::*;

pub use crate::bytes::ToBytes;

#[cfg(test)]
///
/// ```
/// let a: u64 = 0_i32;
/// ```
///
mod testutil {
    use std::io::Write;

    use bytes::{BufMut, Bytes, BytesMut};
    use rand::{Rng, distributions::Alphanumeric};

    pub struct RandomFile {
        pub path: std::path::PathBuf,
    }

    impl RandomFile {
        pub fn new() -> RandomFile {
            loop {
                let filename: String = std::str::from_utf8(
                    rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(30)
                        .collect::<Vec<u8>>()
                        .as_slice(),
                )
                .unwrap()
                .into();
                let path = std::env::temp_dir().join(filename);
                if !path.exists() {
                    return RandomFile { path };
                }
            }
        }
    }

    impl AsRef<std::path::Path> for RandomFile {
        fn as_ref(&self) -> &std::path::Path {
            self.path.as_ref()
        }
    }

    impl Drop for RandomFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let mut sidecar = self.path.as_os_str().to_owned();
            sidecar.push(".tagdata");
            let _ = std::fs::remove_dir_all(std::path::PathBuf::from(sidecar));
        }
    }

    pub fn rand_bytes(size: usize) -> Bytes {
        let buf = BytesMut::new();
        let mut w = buf.writer();
        for byte in rand::thread_rng().sample_iter(&Alphanumeric).take(size) {
            let _ = write!(&mut w, "{}", byte);
            // let _ = w.write(&[byte]);
        }

        w.into_inner().freeze()
    }
}
