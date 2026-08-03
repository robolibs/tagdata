use super::*;
use crate::testutil::RandomFile;
use std::io::{Read, Seek, SeekFrom};

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
        assert_eq!(db.inner.meta().unwrap().version, FORMAT_VERSION);
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
fn rejects_every_non_current_format_marker() -> Result<()> {
    let random_file = RandomFile::new();
    drop(OpenOptions::new().pagesize(4096).open(&random_file)?);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&random_file.path)?;
    for id in 0..=1_u64 {
        let mut block = vec![0; 4096];
        file.seek(SeekFrom::Start(id * 4096))?;
        file.read_exact(&mut block)?;
        let page = unsafe { &mut *(&mut block[0] as *mut u8 as *mut Page) };
        page.meta_mut().version = FORMAT_VERSION - 1;
        page.meta_mut().hash = page.meta().hash_self();
        seal_block(&mut block)?;
        file.seek(SeekFrom::Start(id * 4096))?;
        file.write_all(&block)?;
    }
    file.sync_all()?;
    drop(file);

    assert!(FormatInfo::inspect(&random_file.path).is_err());
    assert!(DB::open(&random_file.path).is_err());
    Ok(())
}
