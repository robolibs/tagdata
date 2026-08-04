use std::{
    env,
    error::Error,
    fs,
    hint::black_box,
    time::{Duration, Instant},
};

use inspace::{CollectionDef, DB, KeyCodec, TypedCodec, U64Codec, ValueCodec};

const DEFAULT_ITEMS: u64 = 100_000;
const DEFAULT_SAMPLES: usize = 9;
const RECORDS: CollectionDef<u64, u64, TypedCodec<U64Codec, U64Codec>> =
    CollectionDef::new("records", TypedCodec::new(U64Codec, U64Codec));

fn main() -> Result<(), Box<dyn Error>> {
    let items = setting("INSPACE_BENCH_ITEMS", DEFAULT_ITEMS)?;
    let samples = setting("INSPACE_BENCH_SAMPLES", DEFAULT_SAMPLES)?;
    if items == 0 || samples == 0 {
        return Err("benchmark settings must be non-zero".into());
    }

    let root = env::temp_dir().join(format!("inspace-typed-bench-{}", std::process::id()));
    let path = root.join("typed.db");
    fs::create_dir_all(&root)?;
    let _ = fs::remove_file(&path);
    let _ = fs::remove_dir_all(format!("{}.inspace", path.display()));
    let db = DB::open(&path)?;
    let tx = db.write_tx()?;
    let records = tx.collection_mut(RECORDS)?;
    records.insert_ordered((0..items).map(|key| (key, key.rotate_left(17))))?;
    tx.commit()?;

    let mut direct = Vec::with_capacity(samples);
    let mut legacy = Vec::with_capacity(samples);
    for sample in 0..samples {
        let tx = db.read_tx()?;
        let records = tx.collection(RECORDS)?;
        let raw = tx.get_bucket("records")?;
        if sample % 2 == 0 {
            direct.push(time_direct(&records, items)?);
            legacy.push(time_legacy(&raw, items)?);
        } else {
            legacy.push(time_legacy(&raw, items)?);
            direct.push(time_direct(&records, items)?);
        }
    }

    let direct = median(direct);
    let legacy = median(legacy);
    println!("Inspace typed full scan");
    println!("items: {items}, samples: {samples}");
    print_result("direct cursor decode", items, direct);
    print_result("legacy repeated lookup", items, legacy);
    println!(
        "  speedup {:>16.2}x",
        legacy.as_secs_f64() / direct.as_secs_f64()
    );

    drop(db);
    let _ = fs::remove_file(path);
    let _ = fs::remove_dir_all(root);
    Ok(())
}

fn time_direct<C>(
    records: &inspace::ReadCollection<'_, '_, u64, u64, C>,
    expected: u64,
) -> Result<Duration, Box<dyn Error>>
where
    C: KeyCodec<u64> + ValueCodec<u64> + Clone,
{
    let started = Instant::now();
    let mut visited = 0;
    for record in records.iter() {
        black_box(record?);
        visited += 1;
    }
    if visited != expected {
        return Err(format!("direct scan visited {visited} records").into());
    }
    Ok(started.elapsed())
}

fn time_legacy(raw: &inspace::Bucket<'_, '_>, expected: u64) -> Result<Duration, Box<dyn Error>> {
    let codec = TypedCodec::new(U64Codec, U64Codec);
    let started = Instant::now();
    let mut visited = 0;
    for pair in raw.kv_pairs() {
        let Some(live) = raw.get_live(pair.key())? else {
            continue;
        };
        black_box(codec.decode_key(live.key())?);
        black_box(codec.decode_value(live.value())?);
        visited += 1;
    }
    if visited != expected {
        return Err(format!("legacy scan visited {visited} records").into());
    }
    Ok(started.elapsed())
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

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn print_result(label: &str, items: u64, duration: Duration) {
    let rate = items as f64 / duration.as_secs_f64();
    println!("  {label:<24} {rate:>12.0} records/s ({duration:?})");
}
