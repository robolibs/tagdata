use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use inspace::{Database, Error, Result};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct TestFile(PathBuf);

impl TestFile {
    fn new() -> Self {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!("inspace-test-{}-{id}.db", std::process::id())))
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn persists_zero_copy_values_across_reopen() -> Result<()> {
    let file = TestFile::new();
    {
        let db = Database::open(&file.0)?;
        db.update(|tx| {
            tx.create_bucket("users")?;
            tx.put("users", "one", [1, 2, 3, 4])?;
            tx.put("users", "two", [5, 6])
        })?;
        assert_eq!(db.transaction_id(), 1);
        db.view(|tx| {
            let users = tx.bucket(b"users")?;
            assert_eq!(
                users.get_kv(b"one").map(|pair| pair.value()),
                Some(&[1, 2, 3, 4][..])
            );
            assert_eq!(
                users.get_kv(b"two").map(|pair| pair.value()),
                Some(&[5, 6][..])
            );
            Ok(())
        })?;
    }
    let reopened = Database::open(&file.0)?;
    assert_eq!(reopened.transaction_id(), 1);
    reopened.check()?;
    reopened.view(|tx| {
        assert_eq!(
            tx.bucket(b"users")?.get_kv(b"one").map(|pair| pair.value()),
            Some(&[1, 2, 3, 4][..])
        );
        Ok(())
    })
}

#[test]
fn callback_error_rolls_back_without_io() -> Result<()> {
    let file = TestFile::new();
    let db = Database::open(&file.0)?;
    let result: Result<()> = db.update(|tx| {
        tx.create_bucket("temporary")?;
        Err(Error::TooLarge)
    });
    assert!(matches!(result, Err(Error::TooLarge)));
    assert_eq!(db.transaction_id(), 0);
    assert!(matches!(
        db.view(|tx| tx.bucket(b"temporary").map(|_| ())),
        Err(Error::BucketNotFound)
    ));
    Ok(())
}

#[test]
fn replaces_deletes_and_recreates_data() -> Result<()> {
    let file = TestFile::new();
    let db = Database::open(&file.0)?;
    db.update(|tx| {
        tx.create_bucket("items")?;
        tx.put("items", "key", "old")
    })?;
    db.update(|tx| {
        tx.put("items", "key", "new")?;
        assert!(matches!(
            tx.delete("items", "missing"),
            Err(Error::KeyValueMissing)
        ));
        Ok(())
    })?;
    db.view(|tx| {
        assert_eq!(
            tx.bucket(b"items")?.get_kv(b"key").map(|pair| pair.value()),
            Some(&b"new"[..])
        );
        Ok(())
    })?;
    db.update(|tx| tx.delete_bucket("items"))?;
    db.update(|tx| {
        tx.create_bucket("items")?;
        tx.put("items", "fresh", "value")
    })?;
    db.view(|tx| {
        let items = tx.bucket(b"items")?;
        assert_eq!(items.get(b"key"), None);
        assert_eq!(
            items.get_kv(b"fresh").map(|pair| pair.value()),
            Some(&b"value"[..])
        );
        Ok(())
    })
}

#[test]
fn discards_a_torn_trailing_transaction() -> Result<()> {
    let file = TestFile::new();
    {
        let db = Database::open(&file.0)?;
        db.update(|tx| {
            tx.create_bucket("safe")?;
            tx.put("safe", "key", "value")
        })?;
    }
    let valid_len = fs::metadata(&file.0)?.len();
    OpenOptions::new()
        .append(true)
        .open(&file.0)?
        .write_all(b"INSTXN01torn")?;
    assert!(fs::metadata(&file.0)?.len() > valid_len);

    let recovered = Database::open(&file.0)?;
    assert_eq!(fs::metadata(&file.0)?.len(), valid_len);
    recovered.view(|tx| {
        assert_eq!(
            tx.bucket(b"safe")?.get_kv(b"key").map(|pair| pair.value()),
            Some(&b"value"[..])
        );
        Ok(())
    })
}
