use inspace::{DB, Error, FORMAT_VERSION, FormatInfo, OpenOptions};

mod common;

#[test]
fn frozen_current_format_opens_reads_writes_and_verifies() -> Result<(), Error> {
    let fixture = "tests/fixtures/current.db";
    assert_eq!(FormatInfo::inspect(fixture)?.version, FORMAT_VERSION);
    let source = OpenOptions::new().read_only().open(fixture)?;
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
    std::fs::copy(fixture, &writable.path)?;
    let db = DB::open(&writable)?;
    db.update(|tx| {
        tx.get_bucket("fixture")?.put("written", "compatible")?;
        Ok(())
    })?;
    db.verify()?;
    db.view(|tx| {
        assert_eq!(
            tx.get_bucket("fixture")?.get_kv("written").unwrap().value(),
            b"compatible"
        );
        Ok(())
    })
}
