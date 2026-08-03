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

`make benchmark` reports full-verification time and compares the selected
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
