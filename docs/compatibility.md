# Compatibility and publication policy

## Language and platforms

The minimum supported Rust version (MSRV) is **1.89.0**, which provides the
standard-library file locking used by Inspace. Raising the MSRV requires a minor release while the
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

## On-disk format

The project has one current on-disk format. It authenticates page blocks with
SHA3-256 and persists page retirement generations. New databases always use it;
there is no format-selection API and no legacy read, write, or migration path.
Opening rejects any other format marker.

Before the first stable release, the format may change without compatibility
code because this project has no deployed database compatibility commitment.
After a stable release, any future format policy must be designed explicitly
rather than pre-emptively carrying unused legacy branches now.

The frozen fixture at `tests/fixtures/current.db` is regenerated whenever the
current layout intentionally changes. CI opens, reads, writes, and verifies it.

## Publication gate

`make package` must pass with the lockfile, along with `make verify`, the MSRV
job, and the supported-platform matrix. Releases should remain prereleases until
the reusable Phase 9 API has been exercised by multiple real applications. A
release checklist must name those applications; examples and synthetic tests do
not satisfy this gate.
