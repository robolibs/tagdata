use std::fs;
use std::time::Instant;

use inspace::{Database, Error};

const ITEMS: u64 = 100_000;

fn main() -> Result<(), Error> {
    let path = std::env::temp_dir().join("inspace-benchmark.db");
    let _ = fs::remove_file(&path);
    let db = Database::open(&path)?;

    let started = Instant::now();
    db.update(|tx| {
        tx.create_bucket("bench")?;
        for number in 0..ITEMS {
            tx.put("bench", number.to_le_bytes(), number.to_le_bytes())?;
        }
        Ok(())
    })?;
    let write_elapsed = started.elapsed();

    let started = Instant::now();
    db.view(|tx| {
        let bucket = tx.bucket(b"bench")?;
        for number in 0..ITEMS {
            assert_eq!(
                bucket.get_kv(number.to_le_bytes()).map(|pair| pair.value()),
                Some(&number.to_le_bytes()[..])
            );
        }
        Ok(())
    })?;
    let read_elapsed = started.elapsed();

    println!(
        "batch write: {:>10.0} ops/s ({write_elapsed:?})",
        ITEMS as f64 / write_elapsed.as_secs_f64()
    );
    println!(
        "mmap reads:  {:>10.0} ops/s ({read_elapsed:?})",
        ITEMS as f64 / read_elapsed.as_secs_f64()
    );
    println!("file size:   {} bytes", fs::metadata(&path)?.len());

    fs::remove_file(path)?;
    Ok(())
}
