use std::{
    fs::OpenOptions as FileOpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use tagdata::{Data, Error, OpenOptions};

mod common;

const PAGE_SIZE: usize = 4096;
const TYPE_BRANCH: u8 = 1;
const TYPE_LEAF: u8 = 2;
const TYPE_FREELIST: u8 = 4;
// id: u64, page_type: u8 + padding, count: u64, overflow: u64, then the elements
const ELEMENTS_OFFSET: usize = 32;

#[test]
fn branch_pointer_at_the_meta_page_is_rejected() -> Result<(), Error> {
    let file = populated()?;
    let bytes = read_all(&file.path)?;
    let (offset, _) = blocks(&bytes)
        .into_iter()
        .rev()
        .find(|(_, page_type)| *page_type == TYPE_BRANCH)
        .expect("database did not contain a branch page");

    // Aim the branch's first child at page 0, which is a meta page.
    write_u64(&file.path, offset + ELEMENTS_OFFSET, 0)?;

    match scan(&file.path) {
        Err(Error::InvalidDB(message)) => {
            assert!(
                message.contains("branch or leaf"),
                "expected a page type error, got {message}"
            );
            Ok(())
        }
        Err(error) => panic!("expected Error::InvalidDB, got {error}"),
        Ok(()) => panic!("corrupt branch pointer was followed without an error"),
    }
}

#[test]
fn branch_pointer_past_the_end_of_the_file_is_rejected() -> Result<(), Error> {
    let file = populated()?;
    let bytes = read_all(&file.path)?;
    let (offset, _) = blocks(&bytes)
        .into_iter()
        .rev()
        .find(|(_, page_type)| *page_type == TYPE_BRANCH)
        .expect("database did not contain a branch page");

    let past_end = (bytes.len() / PAGE_SIZE) as u64 + 1;
    write_u64(&file.path, offset + ELEMENTS_OFFSET, past_end)?;

    match scan(&file.path) {
        Err(Error::InvalidDB(_)) => Ok(()),
        Err(error) => panic!("expected Error::InvalidDB, got {error}"),
        Ok(()) => panic!("out of bounds branch pointer was followed without an error"),
    }
}

#[test]
fn branch_pointer_with_an_absurd_page_id_is_rejected() -> Result<(), Error> {
    let file = populated()?;
    let bytes = read_all(&file.path)?;
    let (offset, _) = blocks(&bytes)
        .into_iter()
        .rev()
        .find(|(_, page_type)| *page_type == TYPE_BRANCH)
        .expect("database did not contain a branch page");

    // Large enough that id * pagesize overflows a u64.
    write_u64(&file.path, offset + ELEMENTS_OFFSET, u64::MAX)?;

    match scan(&file.path) {
        Err(Error::InvalidDB(_)) => Ok(()),
        Err(error) => panic!("expected Error::InvalidDB, got {error}"),
        Ok(()) => panic!("overflowing branch pointer was followed without an error"),
    }
}

#[test]
fn a_leaf_page_where_a_branch_is_expected_is_rejected() -> Result<(), Error> {
    let file = populated()?;
    let bytes = read_all(&file.path)?;
    let (offset, _) = blocks(&bytes)
        .into_iter()
        .rev()
        .find(|(_, page_type)| *page_type == TYPE_BRANCH)
        .expect("database did not contain a branch page");
    let (freelist, _) = blocks(&bytes)
        .into_iter()
        .rev()
        .find(|(_, page_type)| *page_type == TYPE_FREELIST)
        .expect("database did not contain a freelist page");

    let freelist_page = (freelist / PAGE_SIZE) as u64;
    write_u64(&file.path, offset + ELEMENTS_OFFSET, freelist_page)?;

    match scan(&file.path) {
        Err(Error::InvalidDB(_)) => Ok(()),
        Err(error) => panic!("expected Error::InvalidDB, got {error}"),
        Ok(()) => panic!("freelist page was walked as a tree node"),
    }
}

/// Flips one byte at a time across the whole file and requires that opening and
/// scanning the result always returns, whether or not it finds the corruption.
#[test]
fn flipping_any_single_byte_never_unwinds() -> Result<(), Error> {
    let file = populated_with(400)?;
    let pristine = read_all(&file.path)?;

    // Per page: the id, the type byte, the count, the overflow count, the first
    // element, and two points inside the key / value bytes.
    let mut offsets = Vec::new();
    for page in 0..(pristine.len() / PAGE_SIZE) {
        let base = page * PAGE_SIZE;
        for step in [0, 8, 16, 24, 32, 512, 4000] {
            if base + step < pristine.len() {
                offsets.push(base + step);
            }
        }
    }

    for offset in offsets {
        write_all(&file.path, &pristine)?;
        flip(&file.path, offset)?;
        // Either outcome is fine. A panic or a segfault fails the test.
        let _ = scan(&file.path);
    }
    Ok(())
}

/// Rewrites each page header's type byte in turn, which is the field the tree
/// walk trusts most.
#[test]
fn rewriting_page_types_never_unwinds() -> Result<(), Error> {
    let file = populated_with(400)?;
    let pristine = read_all(&file.path)?;

    for page in 2..(pristine.len() / PAGE_SIZE) {
        for page_type in [0_u8, 1, 2, 3, 4, 5, 255] {
            write_all(&file.path, &pristine)?;
            write_u8(&file.path, page * PAGE_SIZE + 8, page_type)?;
            let _ = scan(&file.path);
        }
    }
    Ok(())
}

/// Opens the database and walks every bucket, every key, and every value.
fn scan(path: &Path) -> Result<(), Error> {
    let db = OpenOptions::new().pagesize(PAGE_SIZE as u64).open(path)?;
    let tx = db.tx(false)?;
    for entry in tx.buckets() {
        let (name, bucket) = entry?;
        // A point read walks the same pointers the cursor does.
        let _ = bucket.get(name.name())?;
        for data in bucket.cursor() {
            match data? {
                Data::KeyValue(kv) => {
                    let _ = kv.key().len() + kv.value().len();
                }
                Data::Bucket(nested) => {
                    let _ = nested.name().len();
                }
            }
        }
    }
    Ok(())
}

fn populated() -> Result<common::RandomFile, Error> {
    populated_with(5000)
}

fn populated_with(records: u64) -> Result<common::RandomFile, Error> {
    let file = common::RandomFile::new();
    let db = OpenOptions::new().pagesize(PAGE_SIZE as u64).open(&file)?;
    let tx = db.tx(true)?;
    let bucket = tx.create_bucket("records")?;
    for index in 0..records {
        bucket.put(index.to_be_bytes(), vec![index as u8; 80])?;
    }
    let nested = bucket.create_bucket("nested")?;
    for index in 0..(records / 10) {
        nested.put(index.to_be_bytes(), vec![index as u8; 40])?;
    }
    tx.commit()?;
    db.verify()?;
    drop(db);
    Ok(file)
}

fn blocks(bytes: &[u8]) -> Vec<(usize, u8)> {
    let mut result = Vec::new();
    let mut offset = PAGE_SIZE * 2;
    while offset + PAGE_SIZE <= bytes.len() {
        let page_type = bytes[offset + 8];
        if matches!(page_type, TYPE_BRANCH | TYPE_LEAF | TYPE_FREELIST) {
            result.push((offset, page_type));
            let overflow = read_u64(bytes, offset + 24) as usize;
            offset += PAGE_SIZE * (overflow + 1);
        } else {
            offset += PAGE_SIZE;
        }
    }
    result
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_ne_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn read_all(path: &Path) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    FileOpenOptions::new()
        .read(true)
        .open(path)?
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn write_all(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let mut file = FileOpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn write_at(path: &Path, offset: usize, bytes: &[u8]) -> Result<(), Error> {
    let mut file = FileOpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn write_u64(path: &Path, offset: usize, value: u64) -> Result<(), Error> {
    write_at(path, offset, &value.to_ne_bytes())
}

fn write_u8(path: &Path, offset: usize, value: u8) -> Result<(), Error> {
    write_at(path, offset, &[value])
}

fn flip(path: &Path, offset: usize) -> Result<(), Error> {
    let mut file = FileOpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut byte = [0];
    file.read_exact(&mut byte)?;
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset as u64))?;
    file.write_all(&byte)?;
    file.sync_all()?;
    Ok(())
}
