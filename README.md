# inspace

`inspace` is an embedded, single-file, memory-mapped key/value database for Rust.

See [acknowledgments](ACKOLEGMENT.md) for prior work that informed the project.

## Current engine

- **Zero-copy point reads:** values are borrowed directly from a read-only mmap.
- **Concurrent readers, single writer:** read transactions share the map; a write
  transaction takes exclusive access only until its durable commit completes.
- **Atomic callback transactions:** an error from the callback writes nothing.
- **Crash recovery:** checksummed tree pages are published through alternating
  meta pages; an invalid newest meta page falls back to the prior commit.
- **Fast lookup:** an in-memory, collision-safe hash index points into the mmap.
- **Nested buckets:** byte-key/byte-value namespaces can form arbitrary trees.
- **Ordered traversal:** cursors, key/value iterators, bucket iterators, and ranges.
- **Single-process ownership:** an exclusive file lock prevents unsafe concurrent opens.
- **Small dependency surface:** storage uses `memmap2` and `fs4`.

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

The current format uses fixed-size copy-on-write pages:

```text
meta page A | meta page B
leaf and branch pages for commit 0
leaf and branch pages for commit 1
leaf and branch pages for commit 2
```

The writer builds and syncs new tree pages before publishing their root through
one checksummed meta page. The other meta page retains the previous root. Nodes
may span multiple pages for large values. Readers borrow values from the mapped
leaf pages while holding a shared transaction lock.

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

1. incremental tree updates instead of rebuilding all live records per commit;
2. snapshot readers that remain active while a writer publishes new pages;
3. freelist management and online/offline compaction;
4. explicit read-only open mode and configurable mapping/page behavior;
5. fuzzing, crash-injection tests, format compatibility tests, and comparative
   database benchmarks.

The on-disk format is currently experimental. Do not use this version for the
only copy of critical data, and do not open one file from multiple processes.
