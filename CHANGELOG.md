# Changelog

## [0.1.1] - 2026-08-05

### <!-- 0 -->⛰️  Features

- Verify writes before metadata publication
- Add optional durable change journal
- Persist page retirement generations
- Add scalable ttl cleanup
- Add operator cli verification and salvage
- Detect formats and enforce capacity limits
- Complete reusable application api
- Add map aliases and ordered bulk loading
- Add reverse scans pagination and watch filters
- Add typed entries batches and write options
- Add reusable typed collection definitions
- Add typed transaction errors and writer deadlines
- Add change watches and ttl
- Add optional typed codecs
- Improve transaction ergonomics
- Add versioned page checksums
- Add backup and compaction
- Support multi-process transactions
- Add read-only handles and database stats
- Make commits crash consistent
- Activate dual-meta copy-on-write storage
- Build checksummed copy-on-write tree pages
- Persist bucket sequences and validate state
- Add nested buckets and ordered traversal
- Establish transactional mmap foundation

### <!-- 1 -->🐛 Bug Fixes

- Prevent checksum benchmark elimination

### <!-- 2 -->🚜 Refactor

- Keep only the current storage format
- Organize source modules by responsibility
- Replace engine with proven page implementation

### <!-- 3 -->📚 Documentation

- Record curated performance results
- Focus read performance merge plan
- Align readme with current format policy
- Mark compatibility phase complete
- Mark durable journal phase complete
- Mark scalable storage phase complete
- Mark operator phase complete
- Mark portability phase complete
- Mark application api phase complete
- Extend roadmap with reusable api
- Add database feature roadmap

### <!-- 4 -->⚡ Performance

- Reduce default memory and artifact footprint
- Decode typed scans directly from cursors
- Remove point lookup path allocation

### <!-- 6 -->🧪 Testing

- Restore complete engine coverage

### <!-- 7 -->⚙️ Miscellaneous Tasks

- Automated changelog
- Project rename
- Project rename

### Bench

- Expand read performance coverage
- Compare inspace with jammdb
- Measure reclamation and ttl cleanup

### Build

- Gate typed benchmark example
- Define publication compatibility contract

