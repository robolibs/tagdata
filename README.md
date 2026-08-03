# inspace

`inspace` is an embedded, single-file, memory-mapped key/value database for Rust.

See [acknowledgments](ACKOLEGMENT.md) for prior work that informed the project.

## Engine

- **ACID transactions:** serializable and isolated transactions with explicit
  commit and automatic rollback on drop.
- **Concurrent access:** multiple lock-free readers and one concurrent writer.
- **Memory-mapped reads:** values are read directly from the mapped database file.
- **B+ tree storage:** efficient random lookups and ordered sequential access.
- **Nested buckets:** byte-key/byte-value namespaces can form arbitrary trees.
- **Ordered traversal:** cursors, key/value iterators, bucket iterators, and ranges.
- **Space reuse:** freed pages are tracked and reused by later transactions.
- **Configurable opening:** page size, initial allocation, mmap population, strict
  checks, and direct writes are available through `OpenOptions`.

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

## Commands

```sh
make build
make run
make test
make benchmark
make verify
```
