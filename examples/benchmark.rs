use std::time::{Duration, Instant, UNIX_EPOCH};
use std::{fs, hash::Hasher, hint::black_box};

use fnv::FnvHasher;
use sha3::{Digest, Sha3_256};
use tagdata::{DB, Error, OpenOptions};

const ITEMS: u64 = 100_000;

fn main() -> Result<(), Error> {
    let path = std::env::temp_dir().join("tagdata-benchmark.db");
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

    let started = Instant::now();
    db.verify()?;
    println!("full verify: {:#?}", started.elapsed());

    let bytes = fs::read(&path)?;
    let started = Instant::now();
    black_box(Sha3_256::digest(black_box(&bytes)));
    println!("SHA3-256:    {:#?}", started.elapsed());
    let started = Instant::now();
    let mut fnv = FnvHasher::default();
    fnv.write(black_box(&bytes));
    black_box(fnv.finish());
    println!("FNV-1a-64:   {:#?}", started.elapsed());

    benchmark_reclamation()?;

    drop(db);
    fs::remove_file(path)?;
    let _ = fs::remove_dir_all(std::env::temp_dir().join("tagdata-benchmark.db.tagdata"));
    Ok(())
}

fn benchmark_reclamation() -> Result<(), Error> {
    const CHURN_KEYS: u64 = 2_000;
    const ROUNDS: u8 = 4;
    let path = std::env::temp_dir().join("tagdata-reclamation-benchmark.db");
    let _ = fs::remove_file(&path);
    let db = OpenOptions::new()
        .num_pages(4)
        .growth_increment(4096)
        .open(&path)?;
    db.update(|tx| {
        let values = tx.create_bucket("churn")?;
        let ttl = tx.create_bucket("ttl")?;
        for key in 0..CHURN_KEYS {
            values.put(key.to_be_bytes(), vec![0_u8; 1024])?;
            let deadline = UNIX_EPOCH
                + if key % 2 == 0 {
                    Duration::from_secs(10)
                } else {
                    Duration::from_secs(100)
                };
            ttl.put_with_ttl(key.to_be_bytes(), [1], deadline)?;
        }
        Ok(())
    })?;

    let reader = db.read_tx()?;
    let before = db.stats()?.file_bytes;
    let started = Instant::now();
    for round in 1..=ROUNDS {
        db.update(|tx| {
            let values = tx.get_bucket("churn")?;
            for key in 0..CHURN_KEYS {
                values.put(key.to_be_bytes(), vec![round; 1024])?;
            }
            Ok(())
        })?;
    }
    let churn_elapsed = started.elapsed();
    let pinned = db.stats()?;
    assert!(
        reader
            .get_bucket("churn")?
            .get_kv(0_u64.to_be_bytes())
            .is_some()
    );
    drop(reader);

    let started = Instant::now();
    let removed = db.purge_expired(UNIX_EPOCH + Duration::from_secs(20), CHURN_KEYS as usize)?;
    let cleanup_elapsed = started.elapsed();
    let after = db.stats()?;
    println!("\nreclamation/TTL benchmark");
    println!(
        "write churn:  {:>10.0} ops/s ({churn_elapsed:?})",
        (CHURN_KEYS * u64::from(ROUNDS)) as f64 / churn_elapsed.as_secs_f64()
    );
    println!("long reader:  {} pending pages", pinned.pending_pages);
    println!("file growth:  {} bytes", pinned.file_bytes - before);
    println!(
        "TTL cleanup:  {removed} records in {cleanup_elapsed:?} ({:.0} records/s)",
        removed as f64 / cleanup_elapsed.as_secs_f64()
    );
    println!("post-cleanup: {} bytes", after.file_bytes);

    drop(db);
    fs::remove_file(&path)?;
    let _ = fs::remove_dir_all(path.with_extension("db.tagdata"));
    Ok(())
}
