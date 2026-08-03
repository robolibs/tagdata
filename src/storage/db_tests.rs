use super::*;
use crate::testutil::RandomFile;

#[test]
fn test_open_options() {
    assert_ne!(get_page_size(), 5000);
    let random_file = RandomFile::new();
    {
        let db = OpenOptions::new()
            .pagesize(5000)
            .num_pages(100)
            .open(&random_file)
            .unwrap();
        assert_eq!(db.pagesize(), 5000);
    }
    {
        let metadata = random_file.path.metadata().unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 500_000);
    }
    {
        let db = OpenOptions::new()
            .pagesize(5000)
            .num_pages(100)
            .open(&random_file)
            .unwrap();
        assert_eq!(db.pagesize(), 5000);
    }
}

#[test]
#[should_panic]
fn test_open_options_min_pages() {
    OpenOptions::new().num_pages(3);
}

#[test]
#[should_panic]
fn test_open_options_min_pagesize() {
    OpenOptions::new().pagesize(1000);
}

#[test]
fn test_different_pagesizes_are_detected() {
    assert_ne!(get_page_size(), 5000);
    let random_file = RandomFile::new();
    {
        let db = OpenOptions::new()
            .pagesize(5000)
            .num_pages(100)
            .open(&random_file)
            .unwrap();
        assert_eq!(db.pagesize(), 5000);
    }
    assert_eq!(DB::open(&random_file).unwrap().pagesize(), 5000);
}

#[test]
fn opens_and_migrates_version_one_databases() -> Result<()> {
    let source = RandomFile::new();
    let destination = RandomFile::new();
    drop(init_file_version(&source.path, 4096, 32, false, 1)?);

    let db = OpenOptions::new().pagesize(4096).open(&source)?;
    assert_eq!(db.inner.meta()?.version, 1);
    let tx = db.tx(true)?;
    tx.create_bucket("legacy")?.put("key", "value")?;
    tx.commit()?;
    db.verify()?;

    db.compact_to(&destination.path)?;
    let migrated = OpenOptions::new().pagesize(4096).open(&destination)?;
    assert_eq!(migrated.inner.meta()?.version, DEFAULT_FORMAT_VERSION);
    assert_eq!(
        migrated
            .tx(false)?
            .get_bucket("legacy")?
            .get_kv("key")
            .unwrap()
            .value(),
        b"value"
    );
    migrated.verify()
}
