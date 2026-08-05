use std::{
    env,
    error::Error,
    fs,
    hash::Hasher,
    path::Path,
    time::{Duration, Instant},
};

use fnv::FnvHasher;
use rand::{RngCore, SeedableRng, rngs::StdRng, seq::SliceRandom};
use tagdata::{DB, MergeOptions, OpenOptions};

const DEFAULT_ITEMS: u64 = 500_000;
const DEFAULT_VALUE_BYTES: usize = 32;
const DEFAULT_OVERLAP_PERCENT: f64 = 12.5;
const BUCKET: &str = "kv";

struct BuildResult {
    elapsed: Duration,
    fingerprint: u64,
    overlap_fingerprint: u64,
    file_bytes: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let items = setting("TAGDATA_MERGE_ITEMS", DEFAULT_ITEMS)?;
    let value_bytes = setting("TAGDATA_MERGE_VALUE_BYTES", DEFAULT_VALUE_BYTES)?;
    let overlap_percent = setting("TAGDATA_MERGE_OVERLAP_PERCENT", DEFAULT_OVERLAP_PERCENT)?;
    if items == 0 || value_bytes == 0 || !(0.0..100.0).contains(&overlap_percent) {
        return Err("merge benchmark settings must be non-zero".into());
    }
    let overlap = ((items as f64 * overlap_percent / 100.0).round() as u64).min(items);
    let unique_entries = items * 2 - overlap;

    let root = env::temp_dir().join(format!("tagdata-merge-{}", std::process::id()));
    prepare_root(&root)?;
    let first_path = root.join("first.db");
    let second_path = root.join("second.db");
    let merged_path = root.join("merged.db");

    println!("Tagdata KV merge benchmark");
    println!("entries per source: {items}");
    println!("overlapping keys: {overlap} ({overlap_percent:.2}%)");
    println!("unique merged entries: {unique_entries}");
    println!("value size: {value_bytes} bytes");

    let first = OpenOptions::new().open(&first_path)?;
    let first_keys: Vec<u64> = (0..items).collect();
    let first_build = populate(&first, first_keys, value_bytes, 0xA11C_E001, overlap)?;
    println!(
        "first database:  {:>10.0} entries/s ({:?}, {} bytes)",
        items as f64 / first_build.elapsed.as_secs_f64(),
        first_build.elapsed,
        first_build.file_bytes
    );

    let second = OpenOptions::new().open(&second_path)?;
    let second_keys: Vec<u64> = (0..overlap).chain(items..items + items - overlap).collect();
    let second_build = populate(&second, second_keys, value_bytes, 0xB22D_E002, 0)?;
    println!(
        "second database: {:>10.0} entries/s ({:?}, {} bytes)",
        items as f64 / second_build.elapsed.as_secs_f64(),
        second_build.elapsed,
        second_build.file_bytes
    );

    let merged = OpenOptions::new().open(&merged_path)?;
    let started = Instant::now();
    let first_report = merged.merge_from(&first, MergeOptions::new())?;
    let second_report = merged.merge_from(&second, MergeOptions::new())?;
    let merge_elapsed = started.elapsed();
    let first_copied = merged_keys(first_report);
    let second_copied = merged_keys(second_report);
    let copied = first_copied + second_copied;
    println!(
        "merge:           {:>10.0} entries/s ({merge_elapsed:?})",
        copied as f64 / merge_elapsed.as_secs_f64()
    );

    let expected_fingerprint =
        first_build.fingerprint ^ first_build.overlap_fingerprint ^ second_build.fingerprint;
    let started = Instant::now();
    let (merged_count, merged_fingerprint) = scan(&merged)?;
    merged.verify()?;
    let verify_elapsed = started.elapsed();
    assert_eq!(first_copied, items);
    assert_eq!(second_copied, items);
    assert_eq!(merged_count, unique_entries);
    assert_eq!(merged_fingerprint, expected_fingerprint);

    let merged_bytes = merged.stats()?.file_bytes;
    println!("verification:    {verify_elapsed:?}");
    println!("merged size:     {merged_bytes} bytes");
    println!("result:          {merged_count} verified entries");

    drop(merged);
    drop(second);
    drop(first);
    fs::remove_dir_all(root)?;
    Ok(())
}

fn populate(
    db: &DB,
    mut keys: Vec<u64>,
    value_bytes: usize,
    seed: u64,
    overlap: u64,
) -> Result<BuildResult, Box<dyn Error>> {
    let mut rng = StdRng::seed_from_u64(seed);
    keys.shuffle(&mut rng);

    let started = Instant::now();
    let tx = db.write_tx()?;
    let bucket = tx.create_bucket(BUCKET)?;
    let mut fingerprint = 0_u64;
    let mut overlap_fingerprint = 0_u64;
    for logical_key in keys {
        let key = permute_key(logical_key).to_be_bytes();
        let value = random_value(logical_key, value_bytes, seed);
        let record_fingerprint = record_fingerprint(&key, &value);
        fingerprint ^= record_fingerprint;
        if logical_key < overlap {
            overlap_fingerprint ^= record_fingerprint;
        }
        bucket.put(key, value)?;
    }
    tx.commit()?;

    Ok(BuildResult {
        elapsed: started.elapsed(),
        fingerprint,
        overlap_fingerprint,
        file_bytes: db.stats()?.file_bytes,
    })
}

fn merged_keys(report: tagdata::MergeReport) -> u64 {
    report.keys_inserted + report.keys_updated + report.keys_unchanged + report.entries_skipped
}

fn scan(db: &DB) -> Result<(u64, u64), Box<dyn Error>> {
    let tx = db.read_tx()?;
    let bucket = tx.get_bucket(BUCKET)?;
    let mut count = 0_u64;
    let mut fingerprint = 0_u64;
    for pair in bucket.kv_pairs() {
        fingerprint ^= record_fingerprint(pair.key(), pair.value());
        count += 1;
    }
    Ok((count, fingerprint))
}

fn record_fingerprint(key: &[u8], value: &[u8]) -> u64 {
    let mut hasher = FnvHasher::default();
    hasher.write(key);
    hasher.write(value);
    hasher.finish()
}

fn random_value(logical_key: u64, value_bytes: usize, seed: u64) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(seed ^ permute_key(logical_key));
    let mut value = vec![0_u8; value_bytes];
    rng.fill_bytes(&mut value);
    value
}

fn permute_key(value: u64) -> u64 {
    let mut value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
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

fn prepare_root(root: &Path) -> Result<(), Box<dyn Error>> {
    if root.exists() {
        fs::remove_dir_all(root)?;
    }
    fs::create_dir_all(root)?;
    Ok(())
}
