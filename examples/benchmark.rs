use std::fs;
use std::time::Instant;

use inspace::{DB, Error};

const ITEMS: u64 = 100_000;

fn main() -> Result<(), Error> {
    let path = std::env::temp_dir().join("inspace-benchmark.db");
    let _ = fs::remove_file(&path);
    let db = DB::open(&path)?;

    let started = Instant::now();
    let tx = db.tx(true)?;
    let bucket = tx.create_bucket("bench")?;
    for number in 0..ITEMS {
        bucket.put(number.to_le_bytes(), number.to_le_bytes())?;
    }
    tx.commit()?;
    let write_elapsed = started.elapsed();

    let started = Instant::now();
    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("bench")?;
    for number in 0..ITEMS {
        assert!(
            bucket
                .get_kv(number.to_le_bytes())
                .is_some_and(|pair| pair.value() == number.to_le_bytes())
        );
    }
    let read_elapsed = started.elapsed();

    drop(bucket);
    drop(tx);

    println!(
        "batch write: {:>10.0} ops/s ({write_elapsed:?})",
        ITEMS as f64 / write_elapsed.as_secs_f64()
    );
    println!(
        "mmap reads:  {:>10.0} ops/s ({read_elapsed:?})",
        ITEMS as f64 / read_elapsed.as_secs_f64()
    );
    let stats = db.stats()?;
    println!("file size:   {} bytes", stats.file_bytes);
    println!("pages:       {} allocated", stats.allocated_pages);
    println!("written:     {} bytes", stats.bytes_written);

    fs::remove_file(path)?;
    Ok(())
}
