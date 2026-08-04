use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

use inspace::{Error, OpenOptions, WriteVerification};

fn main() -> Result<(), Error> {
    let items = env_u64("INSPACE_BENCH_ITEMS", 10_000);
    let samples = env_u64("INSPACE_BENCH_SAMPLES", 5) as usize;
    let mut update_counts = vec![1, 100, items];
    update_counts.sort_unstable();
    update_counts.dedup();

    println!("write verification benchmark: {items} records, {samples} samples");
    for updates in update_counts {
        let [standard, readback, full] = measure(items, updates, samples)?;
        println!("\n{updates} updated records per commit");
        report("Standard", standard, standard);
        report("ReadBack", readback, standard);
        report("Full", full, standard);
    }
    Ok(())
}

fn measure(items: u64, updates: u64, samples: usize) -> Result<[Duration; 3], Error> {
    let policies = [
        WriteVerification::Standard,
        WriteVerification::ReadBack,
        WriteVerification::Full,
    ];
    let mut timings = [Vec::new(), Vec::new(), Vec::new()];
    for sample in 0..samples {
        for step in 0..policies.len() {
            let policy_index = (sample + step) % policies.len();
            let policy = policies[policy_index];
            let path = std::env::temp_dir().join(format!(
                "inspace-write-verification-{}-{policy:?}-{updates}-{sample}.db",
                std::process::id()
            ));
            cleanup(&path);
            let db = OpenOptions::new()
                .write_verification(WriteVerification::Standard)
                .open(&path)?;
            db.update(|tx| {
                let bucket = tx.create_bucket("bench")?;
                for key in 0..items {
                    bucket.put(key.to_be_bytes(), vec![0_u8; 128])?;
                }
                Ok(())
            })?;
            drop(db);

            let db = OpenOptions::new().write_verification(policy).open(&path)?;
            let started = Instant::now();
            db.update(|tx| {
                let bucket = tx.get_bucket("bench")?;
                for key in 0..updates {
                    bucket.put(key.to_be_bytes(), vec![1_u8; 128])?;
                }
                Ok(())
            })?;
            timings[policy_index].push(started.elapsed());
            drop(db);
            cleanup(&path);
        }
    }
    for values in &mut timings {
        values.sort_unstable();
    }
    Ok(timings.map(|values| values[values.len() / 2]))
}

fn report(label: &str, elapsed: Duration, baseline: Duration) {
    println!(
        "  {label:<8} {elapsed:>10.2?}  ({:.2}x Standard)",
        elapsed.as_secs_f64() / baseline.as_secs_f64()
    );
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn cleanup(path: &Path) {
    let _ = fs::remove_file(path);
    let mut sidecar = path.as_os_str().to_owned();
    sidecar.push(".inspace");
    let _ = fs::remove_dir_all(sidecar);
}
