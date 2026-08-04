# Inspace Read-Performance Merge Plan

## Goal

Keep only the two read optimizations with the clearest value and smallest
correctness cost:

1. allocation-free point reads;
2. direct typed cursor scans.

Do not merge the freelist-free read-transaction or shared reader-registration
experiments. Preserve the current on-disk format, commit protocol, checksums,
reader coordination, and write durability.

## Current experiment

The full experiment exists on `perf/read-path-speedups`.

| Commit | Work | Decision |
|---|---|---|
| `2c9690b` | Expanded read benchmark matrix | KEEP |
| `e1ba452` | Typed benchmark feature gate | KEEP |
| `935b233` | Allocation-free point lookup | KEEP |
| `d776eba` | Freelist-free read transactions | DROP |
| `3d298fb` | Direct typed cursor decoding | KEEP |
| `f1d8bf7` | Shared reader registrations | DROP |

Because the commits are interleaved, create a clean branch from `main` and
cherry-pick only the KEEP commits in the order shown. Do not rewrite or merge
the full experiment branch.

The local `.gitignore` change belongs to the user and must remain untouched.

## Constraints

- Keep one current on-disk format.
- Do not weaken dirty-page checksums or the two-barrier commit protocol.
- Retain per-read-transaction freelist validation.
- Retain one reader registration per transaction.
- Preserve multi-process snapshot and reclamation behavior.
- Preserve raw and typed API behavior except for the documented iterator detail.
- Keep every source and test file below 800 lines.
- Use Makefile targets for building, testing, formatting, and benchmarking.
- Keep `plans/` local and Git-ignored.

## Phase 1: Establish the benchmark baseline

Retain the expanded comparison harness and typed-scan benchmark.

The comparison matrix must measure:

- hot point reads inside one reused transaction;
- one lookup per transaction;
- overlapping read snapshots;
- ordered full scans;
- reopen plus point reads;
- 8, 128, and 4096-byte values;
- batched writes as a regression signal.

The typed benchmark must compare direct cursor decoding with the former path
that fetched every cursor result through another B+tree lookup.

Run the baseline and candidate on the same host with:

```sh
INSPACE_BENCH_ITEMS=100000 \
INSPACE_BENCH_READS=500000 \
INSPACE_BENCH_SHORT_READS=500 \
INSPACE_BENCH_REOPEN_READS=10000 \
INSPACE_BENCH_SAMPLES=9 \
make benchmark-compare

INSPACE_BENCH_ITEMS=100000 \
INSPACE_BENCH_SAMPLES=9 \
make benchmark-typed
```

## Phase 2: Allocation-free point reads

### Implementation

- Keep the full path-producing search for cursors and mutations.
- Use a leaf-only traversal for ordinary `Bucket::get` and `get_kv` calls.
- Do not allocate a `Vec` or populate mutation-only parent state during point
  reads.
- Let later write operations perform their own full traversal when required.

### Required tests

- Existing and missing keys below, between, and above stored keys.
- A deep tree built with small pages.
- Agreement between point reads, seeks, forward iteration, and reverse
  iteration.
- Writable transactions can still read and subsequently mutate the same tree.

### Acceptance gate

- Hot point-read throughput improves by at least 5% for all three value sizes.
- No raw scan regression exceeds 3% across repeated medians.
- Public APIs and returned bytes remain unchanged.

Expected measured gain from the experiment: approximately 7-11% over the old
Inspace implementation, leaving Inspace approximately 7-11% faster than jammdb
for hot point reads in one reused transaction on the test host.

## Phase 3: Direct typed cursor scans

### Implementation

- Resolve the reserved TTL lookup bucket once when constructing an iterator.
- Decode the key and value already returned by the cursor.
- Never fetch the main record again through `get_live`.
- Continue checking the current wall clock for every TTL-bearing record.
- Preserve prefix, range, reverse, pagination, and record-limit behavior.
- Raw scans must continue exposing stored records regardless of TTL.

### Documented iterator detail

An iterator captures whether its TTL lookup bucket exists when the iterator is
created. Creating the first TTL entry later in the same writable transaction is
not reflected in that already-created iterator. Normal read transactions cannot
change TTL state and are unaffected.

### Required tests

- No TTL metadata.
- Mixed live and expired records.
- Forward and reverse traversal.
- Prefix, range, pagination, and bounded scans.
- Raw visibility still includes expired stored records.
- Corrupt TTL metadata produces a structured iterator error.

### Acceptance gate

- Typed full-scan throughput improves by at least 10x.
- TTL visibility remains correct in ordinary read transactions.
- No persisted TTL layout or public codec API changes.

Expected measured gain from the experiment: approximately 19.2x for a typed
full scan of 100,000 records.

## Phase 4: Explicitly remove the rejected experiments

### Restore read-transaction freelist validation

- Read transactions clone the current freelist as before.
- Read transactions validate the persisted freelist block as before.
- `num_freelist_pages` remains initialized for every transaction.

This intentionally gives up approximately 34% throughput for one-lookup read
transactions. Those transactions remain much slower than jammdb either way, so
the reduced corruption detection is not worth retaining.

### Restore one registration per reader

- Remove the process-local weak-registration map.
- Every read transaction owns and removes its own locked reader file.
- Keep stale-reader cleanup entirely kernel-lock based.

This intentionally gives up the experimental gain for large groups of
overlapping same-generation readers. The simpler coordination boundary is more
valuable unless a real application demonstrates that workload.

## Phase 5: Final validation

Run:

```sh
make fmt
make verify
INSPACE_BENCH_ITEMS=100000 \
INSPACE_BENCH_READS=500000 \
INSPACE_BENCH_SHORT_READS=500 \
INSPACE_BENCH_REOPEN_READS=10000 \
INSPACE_BENCH_SAMPLES=9 \
make benchmark-compare
INSPACE_BENCH_ITEMS=100000 INSPACE_BENCH_SAMPLES=9 make benchmark-typed
```

Confirm:

- every source and test file is below 800 lines;
- `git diff --check` is clean;
- only the four KEEP commits or equivalent changes are present;
- `.gitignore` remains the user's uncommitted change;
- README benchmark claims match the measured workloads;
- no storage-format, durability, checksum, or coordination guarantee changed.

## Merge decision

Merge only when the point-read and typed-scan acceptance gates pass on the same
host and the full verification lane is green. Otherwise discard the curated
branch and retain `perf/read-path-speedups` only as an experimental record.
