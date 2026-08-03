# Compatibility and publication policy

## Language and platforms

The minimum supported Rust version (MSRV) is **1.85.0**, the first release that
supports Rust 2024 edition. Raising the MSRV requires a minor release while the
crate is below 1.0 and must be called out in the changelog.

The supported platform matrix is Linux, macOS, and Windows on the architectures
provided by their current GitHub-hosted runners. Linux and macOS release
artifacts cover x86-64 and AArch64. Other targets may work, but are not part of
the compatibility promise until added to CI.

## API and SemVer

The public Rust API follows Cargo SemVer rules. Before 1.0, breaking API changes
require a minor release; after 1.0 they require a major release. Patch releases
must remain source-compatible and must not change durable behavior silently.
New public structs that may grow use `#[non_exhaustive]` where construction by
literal would otherwise freeze their fields.

The operator CLI's documented commands and JSON field names follow the same
policy. Human-readable output may gain detail in a compatible release.

## On-disk formats

- Formats 1 and 2 remain readable, writable, verifiable, and migratable.
- Format 2 is the default and authenticates page blocks with SHA3-256.
- Format 3 is opt-in and persists page retirement generations.
- A released format remains supported for at least two subsequent minor
  releases and at least 12 months after a successor becomes the default,
  whichever is longer.
- Removing write support requires a major release. Read and migration support
  should be retained whenever technically safe.
- Opening never upgrades a file in place. Logical compaction/migration writes a
  new destination; deployment controls replacement of the source.

Frozen fixtures under `tests/fixtures` are generated for each released format.
CI opens, reads, writes, verifies, and migrates copies of every fixture.

## Publication gate

`make package` must pass with the lockfile, along with `make verify`, the MSRV
job, and the supported-platform matrix. Releases should remain prereleases until
the reusable Phase 9 API has been exercised by multiple real applications. A
release checklist must name those applications; examples and synthetic tests do
not satisfy this gate.
