use std::{
    alloc::{GlobalAlloc, Layout, System},
    fs,
    sync::atomic::{AtomicUsize, Ordering},
};

use inspace::{Error, OpenOptions, WriteVerification};

struct CountingAllocator;

static CURRENT_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            if new_size >= layout.size() {
                record_allocation(new_size - layout.size());
            } else {
                CURRENT_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        resized
    }
}

fn main() -> Result<(), Error> {
    const ITEMS: u64 = 10_000;
    let path = std::env::temp_dir().join("inspace-memory-benchmark.db");
    cleanup(&path);
    let db = OpenOptions::new()
        .write_verification(WriteVerification::Standard)
        .open(&path)?;
    db.update(|tx| {
        let bucket = tx.create_bucket("bench")?;
        for key in 0..ITEMS {
            bucket.put(key.to_be_bytes(), [0_u8; 16])?;
        }
        Ok(())
    })?;

    drop(db);
    let baseline = begin_sample();
    let db = OpenOptions::new()
        .write_verification(WriteVerification::Standard)
        .open(&path)?;
    let (open_peak, open_allocations) = finish_sample(baseline);

    let baseline = begin_sample();
    db.update(|tx| {
        let bucket = tx.get_bucket("bench")?;
        for key in 0..ITEMS {
            bucket.put(key.to_be_bytes(), [1_u8; 16])?;
        }
        Ok(())
    })?;
    let (update_peak, update_allocations) = finish_sample(baseline);

    println!("reopen peak heap increase: {open_peak} bytes");
    println!("reopen allocation calls:   {open_allocations}");
    println!("update peak heap increase: {update_peak} bytes");
    println!("update allocation calls:   {update_allocations}");
    drop(db);
    cleanup(&path);
    Ok(())
}

fn begin_sample() -> usize {
    let baseline = CURRENT_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(baseline, Ordering::Relaxed);
    ALLOCATIONS.store(0, Ordering::Relaxed);
    baseline
}

fn finish_sample(baseline: usize) -> (usize, usize) {
    let peak = PEAK_BYTES.load(Ordering::Relaxed).saturating_sub(baseline);
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    (peak, allocations)
}

fn record_allocation(bytes: usize) {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    let current = CURRENT_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK_BYTES.fetch_max(current, Ordering::Relaxed);
}

fn cleanup(path: &std::path::Path) {
    let _ = fs::remove_file(path);
    let mut sidecar = path.as_os_str().to_owned();
    sidecar.push(".inspace");
    let _ = fs::remove_dir_all(sidecar);
}
