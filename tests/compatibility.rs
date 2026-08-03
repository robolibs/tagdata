use inspace::{DB, Error, FormatInfo, OpenOptions};

mod common;

#[test]
fn frozen_formats_open_read_write_verify_and_migrate() -> Result<(), Error> {
    for expected_version in 1..=3_u32 {
        let fixture = format!("tests/fixtures/format-v{expected_version}.db");
        assert_eq!(FormatInfo::inspect(&fixture)?.version, expected_version);
        let source = OpenOptions::new().read_only().open(&fixture)?;
        source.verify()?;
        source.view(|tx| {
            let root = tx.get_bucket("fixture")?;
            assert_eq!(root.get_kv("message").unwrap().value(), b"frozen");
            assert_eq!(
                root.get_bucket("nested")?.get_kv("key").unwrap().value(),
                b"value"
            );
            Ok(())
        })?;
        drop(source);

        let writable = common::RandomFile::new();
        std::fs::copy(&fixture, &writable.path)?;
        let db = DB::open(&writable)?;
        db.update(|tx| {
            tx.get_bucket("fixture")?.put("written", "compatible")?;
            Ok(())
        })?;
        db.verify()?;

        let migrated = common::RandomFile::new();
        db.compact_to(&migrated.path)?;
        let migrated = DB::open(&migrated)?;
        migrated.verify()?;
        assert_eq!(
            migrated.view(|tx| Ok(tx
                .get_bucket("fixture")?
                .get_kv("written")
                .unwrap()
                .value()
                .to_vec()))?,
            b"compatible"
        );
    }
    Ok(())
}
