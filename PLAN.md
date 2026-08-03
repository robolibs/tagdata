# Inspace Development Plan

## Goal

Build Inspace into a durable, observable, multi-process, memory-mapped database
without weakening its compact byte-oriented API or zero-copy read path.

## Constraints

- Preserve the existing on-disk format until a versioned migration is available.
- Keep safe durability as the default.
- Support many readers and one writer, including across processes.
- Never reclaim a page that is visible to an active snapshot.
- Keep the synchronous engine runtime-independent.
- Keep optional conveniences out of the storage core where possible.
- Keep every source and test file below 800 lines.
- Every phase must pass `make verify` before it is considered complete.

## Execution order

| Phase | Work | Status | Depends on |
|---|---|---|---|
| 0 | Crash-proof commits and failure injection | DONE | Current engine |
| 1 | Statistics and diagnostics | DONE | Phase 0 |
| 2 | Genuine read-only opening | DONE | Phase 0 |
| 3 | Multi-process readers and one writer | DONE | Phases 1-2 |
| 4 | Snapshot backup and offline compaction | DONE | Phase 3 |
| 5 | Versioned pages and checksums | DONE | Phase 0 |
| 6 | Transaction ergonomics and atomic operations | DONE | Phase 0 |
| 7 | Optional typed codec layer | DONE | Phase 6 |
| 8 | Change tracking, TTL, and watches | DONE | Phases 3 and 6 |

Phases 5 and 6 may run after Phase 3 has a settled coordination design. Phase 8
must remain deferred until commit change tracking and delivery semantics are
specified.

## Phase 0: Crash-proof commits

### Problem

The commit path writes data and freelist pages, writes the alternate metadata
page, and then performs one final sync. A crash during that operation can leave
durable metadata referring to incomplete data pages.

### Work

1. Define the durability contract in storage documentation.
2. Split commit publication into ordered stages:
   - write dirty data and freelist pages;
   - flush and sync those pages;
   - write the alternate metadata page;
   - flush and sync the metadata publication.
3. Keep the safe two-barrier protocol as the default.
4. Add a `Durability` option only after benchmarks establish useful alternatives.
5. Add internal failpoints at every commit stage.
6. Add child-process tests that terminate a writer at each failpoint, reopen the
   database, and verify that either the old or new transaction is completely
   visible—never a mixture.
7. Test file growth, freelist publication, large overflow pages, and nested
   buckets under interrupted commits.

### Completion criteria

- Every commit stage has deterministic failure coverage.
- Reopening after each injected failure passes the full database check.
- The default mode uses a data barrier before metadata publication.
- Durability behavior is documented without overstating weaker modes.
- `make verify` passes.

## Phase 1: Statistics and diagnostics

### Public API

Add a non-exhaustive `Stats` snapshot returned by `DB::stats()`.

Initial fields:

- file size in bytes;
- page size and allocated page count;
- free and pending page counts;
- current transaction ID;
- active reader count;
- oldest reader transaction ID;
- pages or bytes pinned by readers when calculable;
- committed transaction count and bytes written since open.

Do not include recursive key counts in the first version because calculating
them requires walking the tree.

### Completion criteria

- Reading statistics cannot block for an unbounded duration.
- Statistics are documented as point-in-time values.
- Tests cover empty, populated, churned, and reader-pinned databases.
- The benchmark example reports storage and transaction statistics.
- `make verify` passes.

## Phase 2: Genuine read-only opening

### Public API

Prefer an explicit capability over a boolean:

```rust
let db = OpenOptions::new().read_only().open("data.db")?;
```

### Work

1. Open existing files without write permissions.
2. Use a read-only memory map.
3. Use shared locking appropriate to each supported platform.
4. Reject writable transactions immediately with a dedicated error.
5. Never create or resize a file in read-only mode.
6. Support read-only filesystems and permission-restricted files.
7. Add process-level tests proving that multiple read-only handles can coexist.

Read-only opening is a safe milestone, not the final multi-process design. It
may initially block writers if that is required for correctness.

### Completion criteria

- Multiple processes can open the same database read-only.
- Writable transactions cannot be created from a read-only handle.
- Opening succeeds when the file is not writable.
- Behavior is tested on Linux, macOS, and Windows.
- `make verify` passes.

## Phase 3: Multi-process readers and one writer

### Required semantics

- Multiple reader processes may hold stable snapshots concurrently.
- Only one write transaction may commit at a time across all processes.
- A writer may publish a new snapshot while older readers continue using theirs.
- Pages visible to any active reader must not be reused.
- Processes must detect file growth and remap safely.
- Crashed processes must not permanently block writes or page reclamation.

### Design milestone

Before implementation, write a short design decision covering:

- the cross-process writer lock;
- reader registration storage;
- snapshot transaction IDs;
- stale-reader detection and PID-reuse protection;
- lock-file or reserved-page layout;
- remapping and metadata-generation detection;
- platform-specific advisory-lock behavior;
- recovery when the coordination state is missing or damaged.

A likely design uses a sidecar coordination file containing a writer lock and
reader slots. Removing the lifetime-exclusive database lock without shared
reader tracking is explicitly forbidden because it would permit unsafe page
reuse.

### Tests

- Concurrent readers in separate processes.
- Competing writers in separate processes.
- Writer publication while an old reader remains active.
- Reader and writer crashes while holding coordination state.
- Stale reader cleanup.
- Database growth and remapping in other processes.
- Rapid open/close cycles and PID reuse simulation.
- Cross-platform locking behavior in CI.

### Completion criteria

- The required semantics above are demonstrated by process-level tests.
- No correctness guarantee depends only on process-local mutexes or reader lists.
- Killing any participating process cannot corrupt the database.
- `make verify` passes on the supported platform matrix.

## Phase 4: Snapshot backup and offline compaction

### Public API

```rust
db.backup_to("backup.db")?;
db.backup_writer(writer)?;
db.compact_to("compact.db")?;
```

### Backup requirements

- Capture one stable transaction snapshot.
- Never copy a changing file as an uncoordinated byte stream.
- Sync the destination before reporting success.
- Validate the resulting database before returning success.
- Document writer blocking, temporary space, and snapshot lifetime.

### Compaction requirements

- Copy only live buckets and key/value pairs into a fresh database.
- Preserve bucket nesting and sequence counters.
- Permit a different page size when explicitly requested.
- Validate the destination.
- Keep atomic replacement separate and guarded because rename and durability
  behavior vary by platform.

### Completion criteria

- Backups remain consistent during concurrent writes.
- Compaction reduces a churned database to approximately its live-data size.
- Interrupted backup or compaction never damages the source database.
- `make verify` passes.

## Phase 5: Versioned pages and checksums

### Work

1. Define a new format version rather than silently changing page headers.
2. Add checksums for every persisted page or overflow block.
3. Validate page ranges, sizes, counts, and overflow spans before unsafe access.
4. Return structured corruption errors instead of panicking where possible.
5. Expose a supported `DB::verify()` operation.
6. Add an offline verification command or example.
7. Provide either read compatibility with the current format or an explicit
   migration through backup/compaction.
8. Benchmark checksum algorithms and verification policies.

### Completion criteria

- Single-bit corruption in metadata, branches, leaves, freelists, keys, and
  values is detected reliably.
- Corrupt offsets cannot cause out-of-bounds mmap interpretation.
- Existing databases have a documented migration path.
- `make verify` passes.

## Phase 6: Transaction ergonomics and atomic operations

### Named transactions

Add clear alternatives to `tx(bool)`:

```rust
db.read_tx()?;
db.write_tx()?;
db.try_write_tx()?;
```

Keep `tx(bool)` temporarily for compatibility and deprecate it only after the
named API is stable.

### Scoped helpers

Consider synchronous closure helpers:

- `DB::view` for read-only work;
- `DB::update` for writable work that commits only when the closure returns
  `Ok`;
- automatic rollback on errors and panics through transaction drop.

Do not allow a transaction to cross an asynchronous suspension point. An
optional adapter may run one complete synchronous closure on a blocking worker,
but the storage core must not depend on an async runtime.

### Atomic bucket operations

- `put_if_absent`;
- `compare_exchange`;
- `delete_if_value`.

Specify missing-key behavior and return both the observed and updated values in
a form that respects transaction lifetimes.

### Completion criteria

- Boolean transaction inversion is unnecessary in new code.
- Nonblocking writer acquisition is available.
- Atomic operations are serializable and covered for success and conflict cases.
- Existing public behavior remains compatible.
- `make verify` passes.

## Phase 7: Optional typed codec layer

Keep the raw byte API primary. Add optional abstractions:

- `KeyCodec` with documented ordering preservation;
- `ValueCodec`;
- `TypedBucket<K, V, C>`;
- structured encode/decode errors;
- optional serialization adapters behind Cargo features.

Typed range iteration must only be offered when the key encoding preserves the
desired byte ordering. Owned decoding is acceptable; do not claim zero-copy for
codecs that allocate.

### Completion criteria

- Raw buckets require no serialization dependencies.
- Typed and raw access can coexist safely.
- Schema and codec versioning are documented.
- Ordering tests cover signed numbers, unsigned numbers, strings, and compound
  keys for every provided key codec.
- `make verify` passes with default and all features.

## Phase 8: Change tracking, TTL, and watches

### Change-set foundation

First define a transaction change set containing ordered bucket paths, keys,
operation types, and commit transaction IDs. Decide whether values are included
and how memory use is bounded.

### Watches

Specify before implementation:

- process-local versus cross-process delivery;
- durable replay versus best-effort notification;
- ordering and transaction boundaries;
- slow-consumer backpressure;
- overflow and disconnect behavior.

### TTL

Specify before implementation:

- wall-clock behavior and clock jumps;
- persistent expiry indexes;
- lazy versus background cleanup;
- snapshot visibility of expired entries;
- interaction with backup and compaction.

TTL and watches must not be implemented as unrelated hooks. Both should build on
the same committed change-set and transaction-ID foundation.

## Deferred ideas

The following are intentionally not near-term priorities:

- **Transparent compression:** it removes zero-copy access for compressed values
  and should be opt-in at the bucket or codec layer.
- **Encryption at rest:** mmap access, key management, page authentication, and
  recovery require a separate threat model and format design.
- **Native async transactions:** transactions hold synchronous locks and borrowed
  mmap data; making them await-safe would encourage long-lived locks.
- **Savepoints:** useful, but they require reversible allocation, freelist, and
  tree mutation state and should follow the transaction change-set design.

## Global verification gate

Every phase must run:

```sh
make verify
make run
```

Additional project invariants:

```sh
find src tests -type f -exec wc -l {} +
git diff --check
```

No source or test file may exceed 800 lines. New unsafe code requires a documented
safety invariant and focused corruption/bounds tests.

---

## Roadmap extension: application-facing API and operational maturity

The storage engine is now feature-complete for the original phases. The next
work should make Inspace pleasant and difficult to misuse when embedded across
many different applications. The raw byte API remains supported and zero-copy;
new typed and convenience APIs must be additive rather than hiding storage
costs or transaction boundaries.

| Phase | Work | Status | Depends on |
|---|---|---|---|
| 9 | Reusable application API | DONE | Phases 6-8 |
| 10 | Portability, contention, and capacity controls | DONE | Phase 9 |
| 11 | Operator CLI, diagnostics, and salvage | DONE | Phases 4-5 and 10 |
| 12 | Scalable TTL and precise page reclamation | DONE | Phases 3 and 8 |
| 13 | Optional durable change journal | TODO | Phases 8 and 12 |
| 14 | Publication and compatibility contract | TODO | Phases 9-13 |

## Phase 9: Reusable application API

### Design principles

1. Keep transactions explicit; convenience methods must not turn every operation
   into an invisible transaction.
2. Represent read and write capabilities with separate types so invalid writes
   fail at compile time rather than at runtime.
3. Use familiar ordered-map vocabulary: `get`, `contains_key`, `insert`,
   `remove`, `entry`, `range`, `prefix`, `first`, and `last`.
4. Keep raw values borrowed from the mmap. Typed codecs may return owned values,
   but must not claim zero-copy behavior when decoding allocates.
5. Make the common path short without removing access to buckets, nested paths,
   cursors, atomic operations, TTL, watches, or raw bytes.
6. Do not add a native async transaction. An application may run one complete
   synchronous closure on its own blocking worker.

### Named collection definitions

Add reusable definitions that applications can declare once and use everywhere:

```rust,ignore
const USERS: CollectionDef<UserId, User, UserCodec> =
    CollectionDef::new("users", UserCodec).schema(1);

db.read(|tx| {
    let users = tx.collection(USERS)?;
    users.get(&user_id)
})?;

db.write(|tx| {
    let mut users = tx.collection_mut(USERS)?;
    users.insert(&user_id, &user)?;
    Ok(())
})?;
```

Requirements:

- A definition contains a stable bucket path, key/value codecs, schema identity,
  and open policy.
- Definitions are cheap to copy and contain no transaction state.
- `ReadCollection` exposes only reads; `WriteCollection` adds mutation methods.
- Opening with an incompatible persisted schema or codec returns a structured
  error before decoding user data.
- Raw and typed collection handles can coexist in one transaction.
- Nested paths use a reusable `BucketPath`/`CollectionPath` type instead of
  repeatedly opening each path component by hand.

### Map-like access

Add these operations consistently to raw and typed collection handles:

- `get`, `get_owned`, `contains_key`, and `multi_get`;
- `insert`/`put`, returning the previous value;
- `remove`/`delete`, returning `Ok(None)` for a missing key;
- `first`, `last`, `pop_first`, and `pop_last`;
- `len`, `is_empty`, and `clear` with documented traversal cost;
- `delete_range` and `delete_prefix` with explicit write-transaction semantics;
- `insert_many` and `remove_many`, with all changes committed atomically by the
  enclosing transaction.

Keep existing method names for compatibility. New aliases should converge on
standard Rust collection terminology, and deprecation should wait until the new
surface has been used by real applications.

### Entry and update API

Provide an `Entry` API for single-lookup manipulation:

```rust,ignore
users
    .entry(user_id)?
    .and_modify(|user| user.login_count += 1)?
    .or_insert(default_user)?;
```

The entry surface should cover:

- occupied and vacant matching;
- `or_insert`, `or_insert_with`, `and_modify`, `replace`, and `remove`;
- compare/exchange conflicts as expected outcomes, separate from storage errors;
- numeric `fetch_add` helpers only for codecs that define an unambiguous numeric
  representation;
- TTL write options on the same mutation so value and expiration cannot drift.

For typed values, `and_modify` decodes once and encodes once. For raw values,
provide a closure over the current bytes without promising in-place mmap writes.

### Transaction closures and application errors

Generalize scoped helpers so application errors can abort without being forced
into `inspace::Error`:

```rust,ignore
let result: Result<User, TransactionError<MyError>> = db.write(|tx| {
    // Storage failures and domain failures remain distinguishable.
});
```

Requirements:

- distinguish storage/commit failures, expected conflicts, and application
  errors with structured enums;
- roll back on every closure error and panic;
- preserve the current panic-safe mutex behavior;
- add deadline-aware `write_tx_timeout(Duration)` and `write_timeout` helpers;
- keep `try_write_tx()` for immediate nonblocking acquisition.

### Streaming traversal

Typed traversal must match the raw API's bounded-memory behavior:

- lazy `iter`, `keys`, `values`, `range`, and `prefix` iterators;
- forward and reverse traversal;
- seek, inclusive/exclusive bounds, `take`, and resumable pagination tokens;
- iterator items return `Result` so one decode failure identifies the failing key
  without collecting the entire bucket;
- existing collecting helpers remain as wrappers over streaming iterators.

### Batches and multi-collection work

Transactions already provide atomicity across buckets. Add a convenience batch
surface without introducing a second transaction model:

```rust,ignore
db.write(|tx| {
    tx.batch()
        .insert(USERS, id, user)
        .remove(SESSIONS, session_id)
        .apply()?;
    Ok(())
})?;
```

Support ordered bulk loading as a separate optimized path. It must validate key
ordering, preserve normal commit durability, and fall back to ordinary insertion
when the preconditions are not met.

### Filtered watches

Build filters over the existing committed change-set foundation:

- watch one collection or nested path;
- optional key prefix/range and operation filters;
- explicit `from_now` semantics for the current best-effort watch;
- preserve transaction boundaries when only some changes match;
- make disconnect/overflow observable through a dedicated watch error.

Durable replay belongs to Phase 13 and must not be implied by this API.

### API research basis

The design should combine proven patterns rather than clone one database API:

- [redb](https://docs.rs/redb/latest/redb/struct.Database.html)'s reusable typed
  table definitions and transaction-scoped table handles;
- [sled](https://docs.rs/sled/latest/sled/struct.Tree.html)'s `BTreeMap`-like
  trees, batches, range/prefix scans, and explicit compare-and-swap outcomes;
- [RocksDB](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview)'s
  consistent `MultiGet`, atomic `WriteBatch`, snapshots, and reversible iterators;
- [heed](https://docs.rs/heed/latest/heed/)'s typed codecs and forward/reverse
  range iterators over an mmap database.

### Phase 9 completion criteria

- A representative application can declare collections once and perform common
  CRUD without repeating bucket names or codec plumbing.
- Read handles cannot call mutation methods at compile time.
- Domain errors can abort scoped writes without losing their original type.
- Typed scans are lazy and bounded-memory.
- Entry, batch, range, prefix, reverse, TTL, and filtered-watch behavior have
  integration tests for raw and typed collections.
- Property tests compare randomized operations with `BTreeMap` behavior.
- Compile-fail tests cover capability and lifetime misuse.
- Existing raw API tests continue to pass unchanged.
- `make verify` and `make run` pass.

## Phase 10: Portability, contention, and capacity controls

- Auto-detect persisted page size and format version when opening an existing
  database; retain an explicit forensic override and fail closed on ambiguity.
- Expose read-only `FormatInfo` inspection without mapping the full database.
- Add writer acquisition deadlines and a structured timeout/contention error.
- Add `max_file_bytes` and configurable growth increments. Reject a commit before
  publication when its required extent exceeds policy.
- Test corrupted bootstrap metadata, cross-system page sizes, lock timeouts, and
  capacity rejection during file growth.

## Phase 11: Operator CLI, diagnostics, and salvage

- Ship a separate lightweight operator binary with `info`, `stats`, `verify`,
  `backup`, `compact`, and `migrate` commands plus stable JSON output.
- Replace `verify() -> Result<()>` internally with structured corruption reports
  containing page ID, offset, page kind, failed invariant, and bucket path when
  recoverable.
- Add conservative copy-out salvage into a new destination with a manifest of
  skipped records/pages. Never perform automatic in-place repair.
- Separate fast physical snapshot backup from logical compaction/migration.
- Package the operator CLI in releases instead of the demonstration example.

## Phase 12: Scalable TTL and precise page reclamation

- Replace key-ordered TTL cleanup with a versioned deadline-ordered index so
  cleanup stops at the first unexpired record.
- Add database-level bounded expiry cleanup across nested collections.
- Persist freelist retirement generations in a new format version and reclaim
  only pages older than the oldest registered reader.
- Preserve conservative reclamation for older formats and damaged coordination
  state.
- Benchmark long-lived readers, write churn, TTL-heavy workloads, file growth,
  and cleanup latency before enabling the new format by default.

## Phase 13: Optional durable change journal

- Keep the default process-local watch lightweight and best-effort.
- Add an opt-in journal that records complete transaction boundaries atomically
  with the user transaction and replays from a transaction ID.
- Specify retention, acknowledgements/checkpoints, filtering, truncation, gap
  detection, and whether values may ever be included.
- Support cross-process consumers without making commit latency unbounded.
- Use the journal as the foundation for optional incremental backup, secondary
  indexing, and replication adapters.

## Phase 14: Publication and compatibility contract

- Define MSRV, SemVer policy, supported platforms, and on-disk format support
  windows.
- Add frozen database fixtures produced by each released format version.
- Test open/read/write/verify/migrate against frozen fixtures in CI.
- Complete crate metadata and add `cargo package --locked` plus MSRV CI.
- Publish only after the Phase 9 API has been exercised by multiple real
  applications; prefer prereleases until that surface settles.

## Still deferred

Transparent compression, encryption at rest, native async transactions, and
savepoints remain deferred. They should not delay the reusable application API,
operational tooling, or storage-scale work above.
