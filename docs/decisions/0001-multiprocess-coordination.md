# Multi-process coordination

## Status

Accepted.

## Required behavior

Tagdata permits many snapshot readers and one writer across processes. Existing
readers may continue while a writer publishes a new snapshot. A page visible to
any registered reader cannot be reused.

## Coordination layout

Coordination uses open-file-description byte-range locks on the database file.
The reserved lock range starts beyond the maximum data-file size, and locking it
does not extend or modify the file.

The gate is held shared while a reader refreshes its map, selects a metadata
generation, and registers. A writer holds it exclusively from before refreshing
through metadata publication. Each reader owns an exclusive token lock and a
payload lock whose offset and length encode its exact transaction ID.

The database file itself is locked exclusively for a writable transaction. This
serializes writers and makes read-only handles, which hold a lifetime shared
database-file lock, block writers. Ordinary readers hold their registration for
the transaction lifetime and coexist with a writer.

## Mapping and reclamation

Every transaction refreshes the mmap when the file length changes and selects
metadata only while protected by the gate. Existing transactions retain their
previous map through `Arc<Mmap>`.

The writer enumerates active token locks, decodes each transaction payload, and
releases pages older than the exact oldest reader. Closing a transaction or
terminating its process releases both locks in the kernel.

## Recovery

No persistent coordination state exists. Kernel locks disappear immediately
when their owning descriptor closes or process exits, including abnormal exits.

## Platform behavior

The storage engine requires open-file-description lock support. The current
backend covers Linux, Android, macOS, iOS, tvOS, visionOS, and watchOS.
