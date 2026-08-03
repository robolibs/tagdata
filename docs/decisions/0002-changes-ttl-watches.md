# Change sets, TTL, and watches

## Decision

Every writable transaction owns an ordered change tracker. Successful durable
publication assigns the metadata transaction ID to one `ChangeSet` and then
notifies watchers. Aborted transactions publish nothing. Entries contain bucket
path, key, and operation but not values, keeping secrets and large values out of
notification memory. Tracking stops at 4,096 entries or 4 MiB of path/key bytes
and marks the set `truncated`.

Watches are process-local, best-effort notifications. Each subscription uses a
bounded queue. Commit never blocks for a consumer: a full or disconnected queue
removes that subscriber. There is no durable replay and no cross-process
delivery. Consumers that require completeness must use transaction IDs to
detect gaps and rescan application state. A change set preserves transaction
boundaries and operation order.

TTL uses a reserved nested bucket named `\0inspace.ttl.v1` as a persistent
key-to-expiration index. Expirations are unsigned Unix-epoch milliseconds.
There is no background thread: reads can use `get_live`/`get_live_at`, while a
writer calls bounded `purge_expired`. Cleanup emits the same committed change
foundation with `Expire` operations. Backup and compaction naturally copy the
index alongside its containing bucket.

## Time and snapshot semantics

The persisted deadline is wall-clock time, so backward clock jumps extend and
forward jumps shorten apparent lifetime. `get_live_at` exists for deterministic
policy and tests. The transaction supplies a stable data/index snapshot, while
the caller supplies the comparison time. Raw `get` deliberately ignores TTL and
remains a zero-copy storage primitive. Expired bytes remain physically present
until a successful cleanup transaction.

## Consequences

- The storage core remains synchronous and runtime-independent.
- Watch delivery cannot delay commit durability.
- Cross-process consumers need an application-owned durable log if replay is
  required.
- The reserved TTL bucket is visible through raw traversal and must not be used
  by applications.
- Normal `put` does not implicitly clear TTL metadata; call `clear_ttl` when
  converting a TTL-managed key to a persistent one.
