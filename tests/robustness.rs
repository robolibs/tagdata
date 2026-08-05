use tagdata::{DB, Error, OpenOptions};

mod common;

#[test]
fn dropped_write_transaction_rolls_back_new_bucket() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;

    {
        let tx = db.tx(true)?;
        let bucket = tx.create_bucket("temporary")?;
        bucket.put("key", "value")?;
    }

    let tx = db.tx(false)?;
    assert!(matches!(
        tx.get_bucket("temporary"),
        Err(Error::BucketMissing)
    ));
    db.check()
}

#[test]
fn dropped_write_transaction_restores_updates_and_deletes() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;

    {
        let tx = db.tx(true)?;
        let bucket = tx.create_bucket("data")?;
        bucket.put("stable", "original")?;
        bucket.put("kept", "present")?;
        tx.commit()?;
    }

    {
        let tx = db.tx(true)?;
        let bucket = tx.get_bucket("data")?;
        bucket.put("stable", "changed")?;
        bucket.delete("kept")?;
        bucket.put("temporary", "discarded")?;
    }

    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("data")?;
    assert_eq!(bucket.get_kv("stable").unwrap().value(), b"original");
    assert_eq!(bucket.get_kv("kept").unwrap().value(), b"present");
    assert!(bucket.get("temporary").is_none());
    Ok(())
}

#[test]
fn binary_and_empty_data_survive_reopen() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let binary_key = vec![0, 255, 1, 254, 0];
    let binary_value = vec![255, 0, 128, 64, 0, 255];

    {
        let db = DB::open(&file)?;
        let tx = db.tx(true)?;
        let bucket = tx.create_bucket("binary")?;
        bucket.put(binary_key.clone(), binary_value.clone())?;
        bucket.put([], [])?;
        tx.commit()?;
    }

    let db = DB::open(&file)?;
    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("binary")?;
    assert_eq!(
        bucket.get_kv(&binary_key).unwrap().value(),
        binary_value.as_slice()
    );
    assert_eq!(bucket.get_kv([]).unwrap().value(), b"");
    db.check()
}

#[test]
fn values_crossing_page_boundaries_survive_reopen() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let page_size: usize = 1024;
    let sizes = [page_size - 1, page_size, page_size + 1, page_size * 5 + 17];

    {
        let db = OpenOptions::new()
            .pagesize(page_size as u64)
            .strict_mode(true)
            .open(&file)?;
        let tx = db.tx(true)?;
        let bucket = tx.create_bucket("pages")?;
        for size in sizes {
            bucket.put(size.to_be_bytes(), vec![(size % 251) as u8; size])?;
        }
        tx.commit()?;
    }

    let db = OpenOptions::new()
        .pagesize(page_size as u64)
        .strict_mode(true)
        .open(&file)?;
    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("pages")?;
    for size in sizes {
        assert_eq!(
            bucket.get_kv(size.to_be_bytes()).unwrap().value(),
            vec![(size % 251) as u8; size]
        );
    }
    db.check()
}

#[test]
fn repeated_replace_and_delete_cycles_remain_valid() -> Result<(), Error> {
    let file = common::RandomFile::new();
    let db = OpenOptions::new().strict_mode(true).open(&file)?;

    for generation in 0..20_u64 {
        let tx = db.tx(true)?;
        let bucket = tx.get_or_create_bucket("cycles")?;
        for key in 0..200_u64 {
            bucket.put(
                key.to_be_bytes(),
                vec![(generation % 251) as u8; (key as usize % 127) + 1],
            )?;
        }
        for key in (0..200_u64).step_by(3) {
            bucket.delete(key.to_be_bytes())?;
        }
        tx.commit()?;
        db.check()?;
    }

    let tx = db.tx(false)?;
    let bucket = tx.get_bucket("cycles")?;
    for key in 0..200_u64 {
        if key % 3 == 0 {
            assert!(bucket.get(key.to_be_bytes()).is_none());
        } else {
            assert_eq!(
                bucket.get_kv(key.to_be_bytes()).unwrap().value(),
                vec![19; (key as usize % 127) + 1]
            );
        }
    }
    Ok(())
}

#[test]
fn nested_bucket_delete_is_persistent() -> Result<(), Error> {
    let file = common::RandomFile::new();

    {
        let db = DB::open(&file)?;
        let tx = db.tx(true)?;
        let root = tx.create_bucket("root")?;
        let nested = root.create_bucket("nested")?;
        nested.put("key", "value")?;
        tx.commit()?;
    }

    {
        let db = DB::open(&file)?;
        let tx = db.tx(true)?;
        let root = tx.get_bucket("root")?;
        root.delete_bucket("nested")?;
        tx.commit()?;
    }

    let db = DB::open(&file)?;
    let tx = db.tx(false)?;
    let root = tx.get_bucket("root")?;
    assert!(matches!(
        root.get_bucket("nested"),
        Err(Error::BucketMissing)
    ));
    db.check()
}
