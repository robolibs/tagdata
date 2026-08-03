# inspace

`inspace` is an embedded, single-file, memory-mapped key/value database for Rust.

See [acknowledgments](ACKOLEGMENT.md) for prior work that informed the project.

## Engine

- **ACID transactions:** serializable and isolated transactions with explicit
  commit and automatic rollback on drop.
- **Concurrent access:** multiple snapshot readers and one writer across threads
  and processes.
- **Memory-mapped reads:** values are read directly from the mapped database file.
- **B+ tree storage:** efficient random lookups and ordered sequential access.
- **Nested buckets:** byte-key/byte-value namespaces can form arbitrary trees.
- **Ordered traversal:** cursors, key/value iterators, bucket iterators, and ranges.
- **Space reuse:** freed pages are tracked and reused by later transactions.
- **Configurable opening:** page size, initial allocation, mmap population, strict
  checks, and direct writes are available through `OpenOptions`.
- **Read-only handles:** existing databases can be opened from read-only files
  and shared safely by multiple reader processes.
- **Runtime statistics:** file, page, freelist, transaction, and reader state is
  available through `DB::stats()`.
- **Operational snapshots:** validated backups and compact copies can be written
  without stopping concurrent writers.
- **Checksummed format:** format-v2 databases authenticate every persisted page
  and overflow block and expose full offline verification.

```rust
use inspace::{DB, Error};

fn main() -> Result<(), Error> {
    let db = DB::open("my.db")?;

    let tx = db.tx(true)?;
    let names = tx.create_bucket("names")?;
    names.put("Kanan", "Jarrus")?;
    names.put("Ezra", "Bridger")?;
    tx.commit()?;

    let tx = db.tx(false)?;
    let names = tx.get_bucket("names")?;
    assert!(names
        .get_kv(b"Kanan")
        .is_some_and(|pair| pair.value() == b"Jarrus"));
    Ok(())
}
```

New code can use `read_tx()`, `write_tx()`, and nonblocking `try_write_tx()`
instead of the compatibility `tx(bool)` method. `view` and `update` scope a
synchronous closure; `update` commits only when the closure returns `Ok` and
rolls back on errors or panics. Transactions are synchronous and must not cross
an async suspension point.

Writable buckets provide serializable `put_if_absent`, `compare_exchange`, and
`delete_if_value` operations. Their owned `AtomicResult` reports whether the
mutation applied plus the observed and resulting values. For
`compare_exchange`, an expected value of `None` matches a missing key. A missing
key is a conflict for `delete_if_value`.

## Typed codecs

The raw byte API remains the default and adds no serialization dependency.
Enable `typed` for `KeyCodec`, `ValueCodec`, and `TypedBucket<K, V, C>`:

```toml
inspace = { version = "0.1", features = ["typed"] }
```

The built-in unsigned, sign-bit-adjusted signed, UTF-8 string, byte-vector, and
compound-string key codecs preserve lexicographic ordering. Typed ranges reject
codecs that do not declare ordering preservation. Decoding returns owned values;
only the raw API claims mmap-backed zero-copy reads. `serde-codec` additionally
enables the opt-in MessagePack value codec.

Codec selection is part of an application's schema. Store a schema/version key
in the containing raw bucket (or use versioned bucket names), migrate values in
a write transaction, and never change a live bucket's codec without rewriting
all entries. Raw and typed views may coexist when they follow the same schema.

## Change watches and TTL

`DB::watch(capacity)` receives ordered, process-local `ChangeSet` values after a
transaction is durably committed. Sets preserve transaction boundaries and IDs
and contain bucket paths, keys, and operation types—not values. Delivery is
best-effort with no durable replay or cross-process transport. Commit never
waits for a watcher; a subscriber whose bounded queue fills is disconnected.
Large transactions cap tracking at 4,096 changes or 4 MiB and set `truncated`.

`Bucket::put_with_ttl` persists a Unix-millisecond expiration in a reserved
nested index. `get_live` applies the current wall clock, while `get_live_at`
accepts an explicit time. Cleanup is deliberately lazy and bounded through
`purge_expired`; no runtime or background thread is required. Raw `get` ignores
TTL and expired bytes remain visible to raw access until cleanup. Wall-clock
jumps affect expiry, and ordinary `put` does not clear an existing TTL—call
`clear_ttl` when making a key persistent. Backup and compaction preserve TTL
indexes. See `docs/decisions/0002-changes-ttl-watches.md` for delivery and time
semantics.

## Storage layout

The format uses fixed-size pages:

```text
meta pages | freelist | leaf and branch pages
```

Write transactions update a copy-on-write B+ tree and publish a new meta page
when committed. Large nodes can span multiple pages, and the freelist makes
released pages available to future writes.

Commits use two durability barriers: changed data and freelist pages are synced
before the alternate meta page is published, then the meta page is synced before
the commit returns. After an interrupted commit, reopening selects either the
complete previous snapshot or the complete newly published snapshot.

Use `OpenOptions::new().read_only()` for a handle that never creates, resizes, or
writes the database. A read-only handle rejects writable transactions.

Writable handles coordinate through a sibling `.inspace` directory. Reader
registrations are removed automatically, including stale registrations left by
terminated processes. Keep that directory beside the database while it is live.

Use `DB::backup_to` for an atomically published snapshot, `DB::backup_writer` to
stream a snapshot, and `DB::compact_to` to rewrite only live data into a smaller
file. Maintenance operations never replace the source database.

New databases use format version 2. Each allocated page block ends with a
SHA3-256 checksum covering its header and payload. Page bounds, element counts,
offsets, overflow spans, tree ordering, and reachability are checked by
`DB::verify()`. `OpenOptions::verify_on_open(true)` performs that full walk while
opening. Normal commits checksum only dirty blocks; reads retain the mmap-backed
zero-copy path, so full verification remains an explicit policy choice.

Format-version-1 files remain readable and writable. They do not gain checksums
in place. Use `compact_to` (or `backup_to`) to produce a validated version-2
copy, then switch files using the deployment's own guarded replacement process.

Offline verification uses a read-only handle:

```sh
cargo run --example verify -- data.db
# Supply the original page size when it differs from the host default:
cargo run --example verify -- data.db 8192
```

`make benchmark` reports full-verification time, v3 long-reader/write-churn
growth, deadline-index TTL cleanup latency, and compares the selected
SHA3-256 checksum with FNV-1a-64 over the same file. SHA3-256 is used on disk for
substantially stronger corruption detection; the benchmark keeps that cost
visible rather than silently choosing the faster non-cryptographic hash.

## Commands

```sh
make build
make run
make test
make benchmark
make verify
```
