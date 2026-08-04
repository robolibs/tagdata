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
const DEFAULT_SAMPLES: usize = 3;
const VALUE_BYTES: usize = 128;

#[derive(Clone, Copy)]
struct Sample {
    write: Duration,
    read: Duration,
    file_bytes: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let items = setting("INSPACE_BENCH_ITEMS", DEFAULT_ITEMS)?;
    let reads = setting("INSPACE_BENCH_READS", DEFAULT_READS)?;
    let samples = setting("INSPACE_BENCH_SAMPLES", DEFAULT_SAMPLES)?;
    if items == 0 || reads == 0 || samples == 0 {
        return Err("benchmark settings must be non-zero".into());
    }

    let read_keys = shuffled_keys(items, reads as usize);
    let root = env::temp_dir().join(format!("inspace-jammdb-bench-{}", std::process::id()));
    let mut inspace = Vec::with_capacity(samples);
    let mut jammdb = Vec::with_capacity(samples);

    println!("Inspace vs jammdb 0.11.0");
    println!("items: {items}, reads: {reads}, samples: {samples}, value: {VALUE_BYTES} bytes");
    println!("Each sample uses a fresh database; engine order alternates.\n");

    for sample in 0..samples {
        if sample % 2 == 0 {
            inspace.push(run_inspace(&root, sample, items, &read_keys)?);
            jammdb.push(run_jammdb(&root, sample, items, &read_keys)?);
        } else {
            jammdb.push(run_jammdb(&root, sample, items, &read_keys)?);
            inspace.push(run_inspace(&root, sample, items, &read_keys)?);
        }
        println!("sample {}/{} complete", sample + 1, samples);
    }

    let inspace = median_sample(inspace);
    let jammdb = median_sample(jammdb);
    print_results("batched writes", items, inspace.write, jammdb.write);
    print_results("hot point reads", reads, inspace.read, jammdb.read);
    println!("\nmedian file size");
    println!("  Inspace {:>12} bytes", inspace.file_bytes);
    println!("  jammdb  {:>12} bytes", jammdb.file_bytes);
    println!(
        "  ratio   {:>12.2}x",
        inspace.file_bytes as f64 / jammdb.file_bytes as f64
    );

    let _ = fs::remove_dir_all(root);
    Ok(())
}

fn run_inspace(
    root: &Path,
    sample: usize,
    items: u64,
    read_keys: &[u64],
) -> Result<Sample, Box<dyn Error>> {
    let path = root.join(format!("inspace-{sample}.db"));
    prepare(&path)?;
    let db = Inspace::open(&path)?;
    let value = [0xA5; VALUE_BYTES];

    let started = Instant::now();
    let tx = db.write_tx()?;
    let bucket = tx.create_bucket("bench")?;
    for key in 0..items {
        bucket.put(key.to_be_bytes(), &value[..])?;
    }
    tx.commit()?;
    let write = started.elapsed();

    let started = Instant::now();
    let tx = db.read_tx()?;
    let bucket = tx.get_bucket("bench")?;
    for &key in read_keys {
        let data = bucket.get(key.to_be_bytes()).ok_or("Inspace key missing")?;
        black_box(data.kv().value());
    }
    let read = started.elapsed();
    let file_bytes = fs::metadata(&path)?.len();

    drop(bucket);
    drop(tx);
    drop(db);
    cleanup(&path);
    Ok(Sample {
        write,
        read,
        file_bytes,
    })
}

fn run_jammdb(
    root: &Path,
    sample: usize,
    items: u64,
    read_keys: &[u64],
) -> Result<Sample, Box<dyn Error>> {
    let path = root.join(format!("jammdb-{sample}.db"));
    prepare(&path)?;
    let db = Jammdb::open(&path)?;
    let value = [0xA5; VALUE_BYTES];

    let started = Instant::now();
    let tx = db.tx(true)?;
    let bucket = tx.create_bucket("bench")?;
    for key in 0..items {
        bucket.put(key.to_be_bytes(), &value[..])?;
    }
    tx.commit()?;
    let write = started.elapsed();

    let started = Instant::now();
    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("bench")?;
    for &key in read_keys {
        let data = bucket.get(key.to_be_bytes()).ok_or("jammdb key missing")?;
        black_box(data.kv().value());
    }
    let read = started.elapsed();
    let file_bytes = fs::metadata(&path)?.len();

    drop(bucket);
    drop(tx);
    drop(db);
    cleanup(&path);
    Ok(Sample {
        write,
        read,
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

fn median_sample(mut samples: Vec<Sample>) -> Sample {
    samples.sort_by_key(|sample| sample.write);
    let write = samples[samples.len() / 2].write;
    samples.sort_by_key(|sample| sample.read);
    let read = samples[samples.len() / 2].read;
    samples.sort_by_key(|sample| sample.file_bytes);
    let file_bytes = samples[samples.len() / 2].file_bytes;
    Sample {
        write,
        read,
        file_bytes,
    }
}

fn print_results(label: &str, operations: u64, inspace: Duration, jammdb: Duration) {
    let inspace_rate = operations as f64 / inspace.as_secs_f64();
    let jammdb_rate = operations as f64 / jammdb.as_secs_f64();
    println!("\n{label}");
    println!("  Inspace {inspace_rate:>12.0} ops/s ({inspace:?})");
    println!("  jammdb  {jammdb_rate:>12.0} ops/s ({jammdb:?})");
    println!("  ratio   {:>12.2}x", inspace_rate / jammdb_rate);
}
