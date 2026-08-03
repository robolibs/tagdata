# inspace

`inspace` is an embedded, single-file, memory-mapped key/value database for Rust.

See [acknowledgments](ACKOLEGMENT.md) for prior work that informed the project.

## Current engine

- **Zero-copy point reads:** values are borrowed directly from a read-only mmap.
- **Concurrent readers, single writer:** read transactions share the map; a write
  transaction takes exclusive access only until its durable commit completes.
- **Atomic callback transactions:** an error from the callback writes nothing.
- **Crash recovery:** every commit has a transaction ID, payload checksum, and
  validated footer. A torn final commit is discarded when the database reopens.
- **Fast lookup:** an in-memory, collision-safe hash index points into the mmap.
- **Nested buckets:** byte-key/byte-value namespaces can form arbitrary trees.
- **Ordered traversal:** cursors, key/value iterators, bucket iterators, and ranges.
- **Single-process ownership:** an exclusive file lock prevents unsafe concurrent opens.
- **Small dependency surface:** the storage engine only depends on `memmap2`.

```rust
use inspace::{Database, Error};

fn main() -> Result<(), Error> {
    let db = Database::open("my.db")?;

    db.update(|tx| {
        tx.create_bucket("names")?;
        tx.put("names", "Kanan", "Jarrus")?;
        tx.put("names", "Ezra", "Bridger")
    })?;

    db.view(|tx| {
        let names = tx.bucket(b"names")?;
        assert_eq!(
            names.get_kv(b"Kanan").map(|pair| pair.value()),
            Some(&b"Jarrus"[..])
        );
        Ok(())
    })
}
```

## Storage layout

The first format version is an append-only transaction log:

```text
file header
transaction header | records... | transaction footer
transaction header | records... | transaction footer
...
```

The writer appends a complete checksummed transaction, calls `sync_data`, then
maps the new file and publishes its records to the index. Readers keep the shared
read lock for their callback, so their borrowed values cannot outlive or race a
map replacement. Opening a database scans committed transactions and truncates
only an incomplete tail.

## Commands

```sh
make build
make run
make test
make benchmark
make verify
```

## Scope and roadmap

This is a functional first engine. Before a stable 1.0, `inspace` still needs:

1. page-oriented B+ trees for ordered scans and bounded startup time;
2. copy-on-write pages plus dual meta pages for large-database commits;
3. freelist management and online/offline compaction;
4. explicit read-only open mode and configurable mapping/page behavior;
5. fuzzing, crash-injection tests, format compatibility tests, and comparative
   database benchmarks.

The on-disk format is currently experimental. Do not use this version for the
only copy of critical data, and do not open one file from multiple processes.
