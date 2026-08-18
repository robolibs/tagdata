use super::*;
use crate::{DB, testutil::RandomFile};

#[test]
fn bytes() {
    let meta = BucketMeta {
        root_page: 3,
        next_int: 1,
    };
    let bytes = meta.as_ref();
    assert_eq!(bytes, &[3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
}

macro_rules! deleted_bucket_test {
    	($($name:ident: ($expected_err:expr, $value:expr))*) => {
    	$(
    		#[test]
            #[should_panic(expected = $expected_err)]
    		fn $name() {
                let random_file = RandomFile::new();
                let db = DB::open(&random_file).unwrap();
                let tx = db.tx(true).unwrap();
                let b = tx.create_bucket("abc").unwrap();
                tx.delete_bucket("abc").unwrap();
                #[allow(clippy::redundant_closure_call)]
                $value(&b);
    		}
    	)*
    	}
    }

deleted_bucket_test! {
    deleted_bucket_put: ("Cannot put data into a deleted bucket.", |b: &Bucket| {
        let _ = b.put("a", "b");
    })
    deleted_bucket_get: ("Cannot get data from a deleted bucket.", |b: &Bucket| {
        let _ = b.get("a");
    })
    deleted_bucket_delete: ("Cannot delete data from a deleted bucket.", |b: &Bucket| {
        let _ = b.delete("a");
    })
    deleted_bucket_get_kv: ("Cannot get data from a deleted bucket.", |b: &Bucket| {
        let _ = b.get_kv("a");
    })
    deleted_bucket_get_bucket: ("Cannot get bucket from a deleted bucket.", |b: &Bucket| {
        let _ = b.get_bucket("a");
    })
    deleted_bucket_create_bucket: ("Cannot create bucket in a deleted bucket.", |b: &Bucket| {
        let _ = b.create_bucket("a");
    })
    deleted_bucket_get_or_create_bucket: ("Cannot get or create bucket from a deleted bucket.", |b: &Bucket| {
        let _ = b.get_or_create_bucket("a");
    })
    deleted_bucket_delete_bucket: ("Cannot delete bucket from a deleted bucket.", |b: &Bucket| {
        let _ = b.delete_bucket("a");
    })
    deleted_bucket_next_int: ("Cannot get next int from a deleted bucket.", |b: &Bucket| {
        b.next_int();
    })
    deleted_bucket_cursor: ("Cannot create cursor from a deleted bucket.", |b: &Bucket| {
        b.cursor();
    })
    deleted_bucket_buckets: ("Cannot create cursor from a deleted bucket.", |b: &Bucket| {
        let _ = b.buckets();
    })
    deleted_bucket_kv_pairs: ("Cannot create cursor from a deleted bucket.", |b: &Bucket| {
        let _ = b.kv_pairs();
    })
}

macro_rules! bucket_errors {
    	($($name:ident: ($rw: expr, $value:expr))*) => {
    	$(
    		#[test]
    		fn $name() -> Result<()> {
                let random_file = RandomFile::new();
                let db = DB::open(&random_file)?;
                {

                    let tx = db.tx(true)?;
                    tx.create_bucket("abc")?;
                    tx.commit()?;
                }
                let tx = db.tx($rw)?;
                let b = tx.get_bucket("abc")?;
                #[allow(clippy::redundant_closure_call)]
                $value(&b);
                Ok(())
    		}
    	)*
    	}
    }

bucket_errors! {
    ro_tx_put_data: (false, |b: &Bucket| {
        assert_eq!(b.put("abc", "def").expect_err("Expected a ReadOnlyTx error"), Error::ReadOnlyTx);
    })
    ro_tx_delete_data: (false, |b: &Bucket| {
        assert_eq!(b.delete("abc").expect_err("Expected a ReadOnlyTx error"), Error::ReadOnlyTx);
    })
    ro_tx_delete_bucket: (false, |b: &Bucket| {
        assert_eq!(b.delete_bucket("abc").expect_err("Expected a ReadOnlyTx error"), Error::ReadOnlyTx);
    })
    ro_tx_get_or_create_bucket: (false, |b: &Bucket| {
        match b.get_or_create_bucket("abc")  {
            Ok(_) => panic!("Expected a ReadOnlyTx error"),
            Err(e) => assert!(e == Error::ReadOnlyTx)
        }
    })
    ro_tx_create_bucket: (false, |b: &Bucket| {
        match b.create_bucket("abc")  {
            Ok(_) => panic!("Expected a ReadOnlyTx error"),
            Err(e) => assert!(e == Error::ReadOnlyTx)
        }
    })
    double_create_bucket: (true, |b: &Bucket| {
        b.create_bucket("abc").unwrap();
        match  b.create_bucket("abc") {
            Ok(_) => panic!("Expected a BucketExists error"),
            Err(e) => assert!(e == Error::BucketExists)
        }
    })
    kv_bucket_mismatch: (true, |b: &Bucket| {
        b.put("abc", "def").unwrap();
        match  b.get_bucket("abc") {
            Ok(_) => panic!("Expected a IncompatibleValue error"),
            Err(e) => assert!(e == Error::IncompatibleValue)
        }
        match  b.create_bucket("abc") {
            Ok(_) => panic!("Expected a IncompatibleValue error"),
            Err(e) => assert!(e == Error::IncompatibleValue)
        }
        match  b.get_or_create_bucket("abc") {
            Ok(_) => panic!("Expected a IncompatibleValue error"),
            Err(e) => assert!(e == Error::IncompatibleValue)
        }
        match  b.delete_bucket("abc") {
            Ok(_) => panic!("Expected a IncompatibleValue error"),
            Err(e) => assert!(e == Error::IncompatibleValue)
        }
    })
    bucket_kv_mismatch: (true, |b: &Bucket| {
        b.create_bucket("abc").unwrap();
        match b.put("abc", "def") {
            Ok(_) => panic!("Expected a IncompatibleValue error"),
            Err(e) => assert!(e == Error::IncompatibleValue)
        }
        match b.delete("abc") {
            Ok(_) => panic!("Expected a IncompatibleValue error"),
            Err(e) => assert!(e == Error::IncompatibleValue)
        }
        assert!(b.get_kv("abc").unwrap().is_none())
    })
}

#[test]
fn test_range() -> Result<()> {
    let random_file = RandomFile::new();
    let db = DB::open(&random_file)?;
    {
        let tx = db.tx(true)?;
        let b = tx.create_bucket("abc")?;
        b.put("a", "1")?;
        b.put("b", "2")?;
        b.put("c", "3")?;
        b.put("d", "4")?;
        b.put("e", "5")?;
        b.put("f", "6")?;
        tx.commit()?;
    }
    macro_rules! iter_test {
        ($range:expr, $keys:expr) => {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            let mut bucket_iter = b.range($range);
            for k in $keys {
                let k = k.as_bytes();
                let data = bucket_iter.next();
                assert!(data.is_some());
                assert!(data.unwrap()?.key() == k);
            }
            assert!(bucket_iter.next().is_none());
        };
    }
    let a = "a".as_bytes();
    let aa = "aa".as_bytes();
    let b = "b".as_bytes();
    let d = "d".as_bytes();
    let e = "e".as_bytes();

    iter_test!(a..e, ["a", "b", "c", "d"]);
    iter_test!(aa..e, ["b", "c", "d"]);
    iter_test!(b..e, ["b", "c", "d"]);
    iter_test!(a..=d, ["a", "b", "c", "d"]);
    iter_test!(b..=e, ["b", "c", "d", "e"]);
    iter_test!(b.., ["b", "c", "d", "e", "f"]);
    iter_test!(a.., ["a", "b", "c", "d", "e", "f"]);
    iter_test!(d..e, ["d"]);
    iter_test!(d..=e, ["d", "e"]);
    iter_test!(..=e, ["a", "b", "c", "d", "e"]);
    iter_test!(..e, ["a", "b", "c", "d"]);
    iter_test!(.., ["a", "b", "c", "d", "e", "f"]);

    Ok(())
}
