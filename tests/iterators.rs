use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use inspace::{Data, Database, Result, ToKVPairs};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct TestFile(PathBuf);

impl TestFile {
    fn new() -> Self {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "inspace-iterator-test-{}-{id}.db",
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
fn cursor_orders_entries_and_seeks() -> Result<()> {
    let file = TestFile::new();
    let db = Database::open(&file.0)?;
    db.update(|tx| {
        tx.create_bucket("numbers")?;
        for number in (0_u32..100).rev() {
            tx.put("numbers", number.to_be_bytes(), number.to_le_bytes())?;
        }
        Ok(())
    })?;

    db.view(|tx| {
        let bucket = tx.bucket(b"numbers")?;
        let keys = bucket
            .kv_pairs()
            .map(|pair| u32::from_be_bytes(pair.key().try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(keys, (0..100).collect::<Vec<_>>());

        let mut cursor = bucket.cursor();
        assert!(cursor.seek(42_u32.to_be_bytes()));
        assert_eq!(cursor.current().unwrap().kv().value(), 42_u32.to_le_bytes());
        assert_eq!(cursor.next().unwrap().key(), 42_u32.to_be_bytes());
        assert_eq!(cursor.next().unwrap().key(), 43_u32.to_be_bytes());

        assert!(!cursor.seek([0, 0, 0, 42, 1]));
        assert_eq!(cursor.next().unwrap().key(), 43_u32.to_be_bytes());

        let start = 20_u32.to_be_bytes();
        let end = 25_u32.to_be_bytes();
        let ranged = bucket
            .range(start.as_slice()..end.as_slice())
            .map(|entry| u32::from_be_bytes(entry.key().try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(ranged, vec![20, 21, 22, 23, 24]);
        Ok(())
    })
}

#[test]
fn data_and_filter_adapters_expose_pairs() -> Result<()> {
    let file = TestFile::new();
    let db = Database::open(&file.0)?;
    db.update(|tx| {
        tx.create_bucket("data")?;
        tx.put("data", "a", "one")?;
        tx.put("data", "b", "two")
    })?;

    db.view(|tx| {
        let bucket = tx.bucket(b"data")?;
        let data = bucket.get(b"a").unwrap();
        assert!(data.is_kv());
        assert_eq!(data.key(), b"a");
        assert_eq!(data.kv().kv(), (&b"a"[..], &b"one"[..]));

        let pairs = bucket.cursor().to_kv_pairs().collect::<Vec<_>>();
        assert_eq!(pairs.len(), 2);
        assert!(
            pairs
                .iter()
                .all(|pair| matches!(bucket.get(pair.key()), Some(Data::KeyValue(_))))
        );
        Ok(())
    })
}
