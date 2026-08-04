use std::{
    env,
    error::Error,
    fs,
    hint::black_box,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use inspace::DB as Inspace;
use jammdb::DB as Jammdb;

const DEFAULT_ITEMS: u64 = 100_000;
const DEFAULT_READS: u64 = 500_000;
const DEFAULT_SHORT_READS: u64 = 1_000;
const DEFAULT_REOPEN_READS: u64 = 10_000;
const DEFAULT_SAMPLES: usize = 3;
const DEFAULT_VALUE_BYTES: &[usize] = &[8, 128, 4096];

#[derive(Clone, Copy)]
struct Sample {
    write: Duration,
    hot_read: Duration,
    short_read: Duration,
    overlapping_read: Duration,
    scan: Duration,
    reopen_read: Duration,
    file_bytes: u64,
}

#[derive(Clone, Copy)]
struct Workload {
    items: u64,
    short_reads: u64,
    reopen_reads: u64,
    value_bytes: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let items = setting("INSPACE_BENCH_ITEMS", DEFAULT_ITEMS)?;
    let reads = setting("INSPACE_BENCH_READS", DEFAULT_READS)?;
    let short_reads = setting("INSPACE_BENCH_SHORT_READS", DEFAULT_SHORT_READS)?;
    let reopen_reads = setting("INSPACE_BENCH_REOPEN_READS", DEFAULT_REOPEN_READS)?;
    let samples = setting("INSPACE_BENCH_SAMPLES", DEFAULT_SAMPLES)?;
    let value_sizes = value_sizes()?;
    if items == 0 || reads == 0 || short_reads == 0 || reopen_reads == 0 || samples == 0 {
        return Err("benchmark settings must be non-zero".into());
    }

    let read_keys = shuffled_keys(items, reads as usize);
    let short_keys = shuffled_keys(items, short_reads as usize);
    let reopen_keys = shuffled_keys(items, reopen_reads as usize);
    let root = env::temp_dir().join(format!("inspace-jammdb-bench-{}", std::process::id()));

    println!("Inspace vs jammdb 0.11.0");
    println!(
        "items: {items}, hot reads: {reads}, short reads: {short_reads}, reopen reads: {reopen_reads}, samples: {samples}"
    );
    println!("Each sample uses a fresh database; engine order alternates.");

    for value_bytes in value_sizes {
        let workload = Workload {
            items,
            short_reads,
            reopen_reads,
            value_bytes,
        };
        let mut inspace = Vec::with_capacity(samples);
        let mut jammdb = Vec::with_capacity(samples);

        println!("\n=== value size: {value_bytes} bytes ===");
        for sample in 0..samples {
            if sample % 2 == 0 {
                inspace.push(run_inspace(
                    &root,
                    sample,
                    workload,
                    &read_keys,
                    &short_keys,
                    &reopen_keys,
                )?);
                jammdb.push(run_jammdb(
                    &root,
                    sample,
                    workload,
                    &read_keys,
                    &short_keys,
                    &reopen_keys,
                )?);
            } else {
                jammdb.push(run_jammdb(
                    &root,
                    sample,
                    workload,
                    &read_keys,
                    &short_keys,
                    &reopen_keys,
                )?);
                inspace.push(run_inspace(
                    &root,
                    sample,
                    workload,
                    &read_keys,
                    &short_keys,
                    &reopen_keys,
                )?);
            }
            println!("sample {}/{} complete", sample + 1, samples);
        }

        print_summary(
            median_sample(inspace),
            median_sample(jammdb),
            workload,
            reads,
        );
    }

    let _ = fs::remove_dir_all(root);
    Ok(())
}

fn run_inspace(
    root: &Path,
    sample: usize,
    workload: Workload,
    read_keys: &[u64],
    short_keys: &[u64],
    reopen_keys: &[u64],
) -> Result<Sample, Box<dyn Error>> {
    let path = root.join(format!("inspace-{}-{sample}.db", workload.value_bytes));
    prepare(&path)?;
    let (write, hot_read, short_read, overlapping_read, scan, file_bytes) = {
        let db = Inspace::open(&path)?;
        let value = vec![0xA5; workload.value_bytes];

        let started = Instant::now();
        let tx = db.write_tx()?;
        let bucket = tx.create_bucket("bench")?;
        for key in 0..workload.items {
            bucket.put(key.to_be_bytes(), value.as_slice())?;
        }
        tx.commit()?;
        let write = started.elapsed();

        let hot_read = {
            let started = Instant::now();
            let tx = db.read_tx()?;
            let bucket = tx.get_bucket("bench")?;
            for &key in read_keys {
                let data = bucket.get(key.to_be_bytes()).ok_or("Inspace key missing")?;
                black_box(data.kv().key());
                black_box(data.kv().value());
            }
            started.elapsed()
        };

        let started = Instant::now();
        for &key in short_keys {
            {
                let tx = db.read_tx()?;
                let bucket = tx.get_bucket("bench")?;
                let data = bucket.get(key.to_be_bytes()).ok_or("Inspace key missing")?;
                black_box(data.kv().key());
                black_box(data.kv().value());
            }
        }
        let short_read = started.elapsed();

        let started = Instant::now();
        let mut transactions = Vec::with_capacity(short_keys.len());
        for &key in short_keys {
            let tx = db.read_tx()?;
            {
                let bucket = tx.get_bucket("bench")?;
                let data = bucket.get(key.to_be_bytes()).ok_or("Inspace key missing")?;
                black_box(data.kv().value());
            }
            transactions.push(tx);
        }
        drop(transactions);
        let overlapping_read = started.elapsed();

        let scan = {
            let started = Instant::now();
            let tx = db.read_tx()?;
            let bucket = tx.get_bucket("bench")?;
            let mut visited = 0_u64;
            for pair in bucket.kv_pairs() {
                black_box(pair.key());
                black_box(pair.value());
                visited += 1;
            }
            if visited != workload.items {
                return Err(format!("Inspace scan visited {visited} items").into());
            }
            started.elapsed()
        };

        let file_bytes = fs::metadata(&path)?.len();
        (
            write,
            hot_read,
            short_read,
            overlapping_read,
            scan,
            file_bytes,
        )
    };
    let started = Instant::now();
    let db = Inspace::open(&path)?;
    let tx = db.read_tx()?;
    let bucket = tx.get_bucket("bench")?;
    for &key in reopen_keys {
        let data = bucket.get(key.to_be_bytes()).ok_or("Inspace key missing")?;
        black_box(data.kv().key());
        black_box(data.kv().value());
    }
    let reopen_read = started.elapsed();

    drop(bucket);
    drop(tx);
    drop(db);
    cleanup(&path);
    Ok(Sample {
        write,
        hot_read,
        short_read,
        overlapping_read,
        scan,
        reopen_read,
        file_bytes,
    })
}

fn run_jammdb(
    root: &Path,
    sample: usize,
    workload: Workload,
    read_keys: &[u64],
    short_keys: &[u64],
    reopen_keys: &[u64],
) -> Result<Sample, Box<dyn Error>> {
    let path = root.join(format!("jammdb-{}-{sample}.db", workload.value_bytes));
    prepare(&path)?;
    let (write, hot_read, short_read, overlapping_read, scan, file_bytes) = {
        let db = Jammdb::open(&path)?;
        let value = vec![0xA5; workload.value_bytes];

        let started = Instant::now();
        let tx = db.tx(true)?;
        let bucket = tx.create_bucket("bench")?;
        for key in 0..workload.items {
            bucket.put(key.to_be_bytes(), value.as_slice())?;
        }
        tx.commit()?;
        let write = started.elapsed();

        let hot_read = {
            let started = Instant::now();
            let tx = db.tx(false)?;
            let bucket = tx.get_bucket("bench")?;
            for &key in read_keys {
                let data = bucket.get(key.to_be_bytes()).ok_or("jammdb key missing")?;
                black_box(data.kv().key());
                black_box(data.kv().value());
            }
            started.elapsed()
        };

        let started = Instant::now();
        for &key in short_keys {
            {
                let tx = db.tx(false)?;
                let bucket = tx.get_bucket("bench")?;
                let data = bucket.get(key.to_be_bytes()).ok_or("jammdb key missing")?;
                black_box(data.kv().key());
                black_box(data.kv().value());
            }
        }
        let short_read = started.elapsed();

        let started = Instant::now();
        let mut transactions = Vec::with_capacity(short_keys.len());
        for &key in short_keys {
            let tx = db.tx(false)?;
            {
                let bucket = tx.get_bucket("bench")?;
                let data = bucket.get(key.to_be_bytes()).ok_or("jammdb key missing")?;
                black_box(data.kv().value());
            }
            transactions.push(tx);
        }
        drop(transactions);
        let overlapping_read = started.elapsed();

        let scan = {
            let started = Instant::now();
            let tx = db.tx(false)?;
            let bucket = tx.get_bucket("bench")?;
            let mut visited = 0_u64;
            for pair in bucket.kv_pairs() {
                black_box(pair.key());
                black_box(pair.value());
                visited += 1;
            }
            if visited != workload.items {
                return Err(format!("jammdb scan visited {visited} items").into());
            }
            started.elapsed()
        };

        let file_bytes = fs::metadata(&path)?.len();
        (
            write,
            hot_read,
            short_read,
            overlapping_read,
            scan,
            file_bytes,
        )
    };
    let started = Instant::now();
    let db = Jammdb::open(&path)?;
    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("bench")?;
    for &key in reopen_keys {
        let data = bucket.get(key.to_be_bytes()).ok_or("jammdb key missing")?;
        black_box(data.kv().key());
        black_box(data.kv().value());
    }
    let reopen_read = started.elapsed();

    drop(bucket);
    drop(tx);
    drop(db);
    cleanup(&path);
    Ok(Sample {
        write,
        hot_read,
        short_read,
        overlapping_read,
        scan,
        reopen_read,
        file_bytes,
    })
}

fn shuffled_keys(items: u64, reads: usize) -> Vec<u64> {
    let mut state = 0xD1B5_4A32_D192_ED03_u64;
    (0..reads)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % items
        })
        .collect()
}

fn setting<T>(name: &str, default: T) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn value_sizes() -> Result<Vec<usize>, Box<dyn Error>> {
    match env::var("INSPACE_BENCH_VALUE_BYTES") {
        Ok(value) => {
            let sizes = value
                .split(',')
                .map(str::trim)
                .map(str::parse)
                .collect::<Result<Vec<usize>, _>>()?;
            if sizes.is_empty() || sizes.contains(&0) {
                return Err("value sizes must be non-zero".into());
            }
            Ok(sizes)
        }
        Err(env::VarError::NotPresent) => Ok(DEFAULT_VALUE_BYTES.to_vec()),
        Err(error) => Err(error.into()),
    }
}

fn prepare(path: &Path) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    cleanup(path);
    Ok(())
}

fn cleanup(path: &Path) {
    let _ = fs::remove_file(path);
    let mut sidecar = PathBuf::from(path);
    let extension = path
        .extension()
        .map(|extension| extension.to_string_lossy().into_owned())
        .unwrap_or_default();
    sidecar.set_extension(format!("{extension}.inspace"));
    let _ = fs::remove_dir_all(sidecar);
}

fn median_sample(samples: Vec<Sample>) -> Sample {
    Sample {
        write: median(&samples, |sample| sample.write),
        hot_read: median(&samples, |sample| sample.hot_read),
        short_read: median(&samples, |sample| sample.short_read),
        overlapping_read: median(&samples, |sample| sample.overlapping_read),
        scan: median(&samples, |sample| sample.scan),
        reopen_read: median(&samples, |sample| sample.reopen_read),
        file_bytes: median_u64(&samples, |sample| sample.file_bytes),
    }
}

fn median(samples: &[Sample], field: impl Fn(&Sample) -> Duration) -> Duration {
    let mut values: Vec<_> = samples.iter().map(field).collect();
    values.sort_unstable();
    values[values.len() / 2]
}

fn median_u64(samples: &[Sample], field: impl Fn(&Sample) -> u64) -> u64 {
    let mut values: Vec<_> = samples.iter().map(field).collect();
    values.sort_unstable();
    values[values.len() / 2]
}

fn print_summary(inspace: Sample, jammdb: Sample, workload: Workload, hot_reads: u64) {
    print_results(
        "batched writes",
        workload.items,
        inspace.write,
        jammdb.write,
    );
    print_results(
        "hot point reads",
        hot_reads,
        inspace.hot_read,
        jammdb.hot_read,
    );
    print_results(
        "one lookup per read transaction",
        workload.short_reads,
        inspace.short_read,
        jammdb.short_read,
    );
    print_results(
        "overlapping read snapshots",
        workload.short_reads,
        inspace.overlapping_read,
        jammdb.overlapping_read,
    );
    print_results(
        "ordered full scan",
        workload.items,
        inspace.scan,
        jammdb.scan,
    );
    print_results(
        "reopen plus point reads",
        workload.reopen_reads,
        inspace.reopen_read,
        jammdb.reopen_read,
    );
    println!("\nmedian file size");
    println!("  Inspace {:>12} bytes", inspace.file_bytes);
    println!("  jammdb  {:>12} bytes", jammdb.file_bytes);
    println!(
        "  ratio   {:>12.2}x",
        inspace.file_bytes as f64 / jammdb.file_bytes as f64
    );
}

fn print_results(label: &str, operations: u64, inspace: Duration, jammdb: Duration) {
    let inspace_rate = operations as f64 / inspace.as_secs_f64();
    let jammdb_rate = operations as f64 / jammdb.as_secs_f64();
    println!("\n{label}");
    println!("  Inspace {inspace_rate:>12.0} ops/s ({inspace:?})");
    println!("  jammdb  {jammdb_rate:>12.0} ops/s ({jammdb:?})");
    println!("  ratio   {:>12.2}x", inspace_rate / jammdb_rate);
}
