use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use inspace::{Data, Database, Error, Result};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct TestFile(PathBuf);

impl TestFile {
    fn new() -> Self {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "inspace-nested-test-{}-{id}.db",
            std::process::id()
        )))
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn nested_buckets_persist_and_iterate() -> Result<()> {
    let file = TestFile::new();
    {
        let db = Database::open(&file.0)?;
        db.update(|tx| {
            let mut root = tx.create_bucket("root")?;
            root.put("top", "value")?;
            let mut child = root.create_bucket("child")?;
            child.put("inside", "nested")?;
            let mut grandchild = child.create_bucket("grandchild")?;
            grandchild.put("deep", "data")
        })?;
    }

    let db = Database::open(&file.0)?;
    db.view(|tx| {
        let root = tx.bucket(b"root")?;
        assert!(matches!(root.get(b"child"), Some(Data::Bucket(_))));
        let children = root
            .buckets()
            .map(|(name, _)| name.name().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(children, vec![b"child".to_vec()]);

        let child = root.get_bucket(b"child")?;
        assert_eq!(child.get_kv(b"inside").unwrap().value(), b"nested");
        let grandchild = child.get_bucket(b"grandchild")?;
        assert_eq!(grandchild.get_kv(b"deep").unwrap().value(), b"data");

        let entries = root.cursor().collect::<Vec<_>>();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key(), b"child");
        assert_eq!(entries[1].key(), b"top");
        Ok(())
    })
}

#[test]
fn deleting_a_bucket_removes_its_subtree() -> Result<()> {
    let file = TestFile::new();
    let db = Database::open(&file.0)?;
    db.update(|tx| {
        let mut root = tx.create_bucket("root")?;
        let mut child = root.create_bucket("child")?;
        child.create_bucket("grandchild")?;
        Ok(())
    })?;
    db.update(|tx| {
        let mut root = tx.bucket("root")?;
        root.delete_bucket("child")
    })?;
    db.view(|tx| {
        let root = tx.bucket(b"root")?;
        assert!(matches!(
            root.get_bucket(b"child"),
            Err(Error::BucketNotFound)
        ));
        assert_eq!(root.buckets().count(), 0);
        Ok(())
    })
}

#[test]
fn keys_and_child_buckets_cannot_share_a_name() -> Result<()> {
    let file = TestFile::new();
    let db = Database::open(&file.0)?;
    db.update(|tx| {
        let mut root = tx.create_bucket("root")?;
        root.put("occupied", "value")?;
        assert!(matches!(
            root.create_bucket("occupied"),
            Err(Error::IncompatibleValue)
        ));
        root.create_bucket("child")?;
        assert!(matches!(
            root.put("child", "value"),
            Err(Error::IncompatibleValue)
        ));
        Ok(())
    })
}
