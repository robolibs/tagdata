use std::{
    fs::OpenOptions as FileOpenOptions,
    io::{Read, Seek, SeekFrom, Write},
};

use inspace::{Error, OpenOptions};

mod common;

const PAGE_SIZE: usize = 4096;
const TYPE_BRANCH: u8 = 1;
const TYPE_LEAF: u8 = 2;
const TYPE_FREELIST: u8 = 4;

#[test]
fn detects_metadata_corruption_without_panicking() -> Result<(), Error> {
    let file = populated()?;
    flip(&file.path, PAGE_SIZE - 33)?;
    flip(&file.path, (PAGE_SIZE * 2) - 33)?;
    assert_corrupt(&file)
}

#[test]
fn detects_branch_corruption() -> Result<(), Error> {
    corrupt_block(TYPE_BRANCH, |offset, _| offset + 40)
}

#[test]
fn detects_freelist_corruption() -> Result<(), Error> {
    corrupt_block(TYPE_FREELIST, |offset, _| offset + 40)
}

#[test]
fn detects_leaf_key_corruption() -> Result<(), Error> {
    corrupt_block(TYPE_LEAF, |offset, bytes| {
        let element = offset + 32;
        element + read_u64(bytes, element + 8) as usize
    })
}

#[test]
fn detects_leaf_value_corruption() -> Result<(), Error> {
    corrupt_block(TYPE_LEAF, |offset, bytes| {
        let element = offset + 32;
        element + read_u64(bytes, element + 8) as usize + read_u64(bytes, element + 16) as usize
    })
}

#[test]
fn verify_on_open_accepts_a_valid_database() -> Result<(), Error> {
    let file = populated()?;
    let db = OpenOptions::new()
        .pagesize(PAGE_SIZE as u64)
        .verify_on_open(true)
        .open(&file)?;
    db.verify()
}

fn corrupt_block(kind: u8, target: impl Fn(usize, &[u8]) -> usize) -> Result<(), Error> {
    let file = populated()?;
    let bytes = read_all(&file.path)?;
    let (offset, _) = blocks(&bytes)
        .into_iter()
        .rev()
        .find(|(_, page_type)| *page_type == kind)
        .unwrap_or_else(|| panic!("database did not contain page type {kind}"));
    flip(&file.path, target(offset, &bytes))?;
    assert_corrupt(&file)
}

fn populated() -> Result<common::RandomFile, Error> {
    let file = common::RandomFile::new();
    let db = OpenOptions::new().pagesize(PAGE_SIZE as u64).open(&file)?;
    let tx = db.tx(true)?;
    let bucket = tx.create_bucket("records")?;
    for index in 0..5000_u64 {
        bucket.put(index.to_be_bytes(), vec![index as u8; 80])?;
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

fn read_all(path: &std::path::Path) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    FileOpenOptions::new()
        .read(true)
        .open(path)?
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn flip(path: &std::path::Path, offset: usize) -> Result<(), Error> {
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

fn assert_corrupt(file: &common::RandomFile) -> Result<(), Error> {
    match OpenOptions::new()
        .pagesize(PAGE_SIZE as u64)
        .verify_on_open(true)
        .open(file)
    {
        Err(Error::InvalidDB(_)) => Ok(()),
        Err(error) => panic!("expected structured corruption error, got {error}"),
        Ok(_) => panic!("corrupt database passed verification"),
    }
}
