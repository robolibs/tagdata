# Multi-process coordination

## Status

Accepted.

## Required behavior

Inspace permits many snapshot readers and one writer across processes. Existing
readers may continue while a writer publishes a new snapshot. A page visible to
any registered reader cannot be reused.

## Coordination layout

For `data.db`, writable handles use a `data.db.inspace` directory:

```text
gate.lock
readers/reader-<transaction>-<pid>-<nonce>
```

The gate is held shared while a reader refreshes its map, selects a metadata
generation, and registers. A writer holds it exclusively from before refreshing
through metadata publication. The gate therefore prevents a reader from
selecting an old generation after the writer has decided which pages are safe to
reuse.

Each active reader holds a shared lock on its uniquely created reader file. A
writer probes those files with a nonblocking exclusive lock. An unlocked entry
belongs to a terminated process and is removed. Correctness depends on the lock,
not the PID, so PID reuse is harmless.

The database file itself is locked exclusively for a writable transaction. This
serializes writers and makes genuine read-only handles, which hold a lifetime
shared database-file lock, safely block writers. Ordinary readers on writable
handles use the registry rather than a database-file lock and therefore coexist
with a writer.

## Mapping and reclamation

Every transaction refreshes the mmap when the file length changes and selects
metadata only while protected by the gate. Existing transactions retain their
previous map through `Arc<Mmap>`.

Freelist generations are not stored in the current file format. When any
external reader exists, a writer conservatively defers every reusable page before
allocation. This can retain more space than necessary but prevents a new writer
process from reusing pages visible to an old reader. A future format may persist
freelist generations to reduce that retention.

## Recovery

Kernel locks are released when a process exits. Reader files may remain after a
crash, but the next writer detects and removes unlocked entries. Missing
coordination directories are recreated when no process is using them. Manual
removal while the database is live is unsupported.

## Platform behavior

Coordination uses the cross-platform whole-file locking contract provided by the
existing locking dependency. Process-level tests run on Linux, macOS, and Windows
through the verification matrix.
