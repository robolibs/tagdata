use std::mem::size_of;

use super::*;
use crate::{
    db::{DB, OpenOptions},
    testutil::RandomFile,
};

#[test]
fn test_ro_txs() -> Result<()> {
    let random_file = RandomFile::new();
    let db = DB::open(&random_file)?;

    {
        let tx = db.tx(true)?;
        assert!(tx.create_bucket("abc").is_ok());
        tx.commit()?;
    }

    let tx = db.tx(false)?;
    assert!(tx.create_bucket("def").is_err());
    let b = tx.get_bucket("abc")?;
    assert_eq!(b.put("key", "value"), Err(Error::ReadOnlyTx));
    assert_eq!(b.delete("key"), Err(Error::ReadOnlyTx));
    assert_eq!(b.create_bucket("dev").err(), Some(Error::ReadOnlyTx));
    assert_eq!(tx.commit(), Err(Error::ReadOnlyTx));
    Ok(())
}

#[test]
fn test_concurrent_txs() -> Result<()> {
    let random_file = RandomFile::new();
    let db = OpenOptions::new()
        .pagesize(1024)
        .num_pages(10)
        .open(&random_file)?;
    {
        let tx = db.tx(false)?;
        assert!(!tx.writable());
        let tx = tx.inner.borrow_mut();
        assert_eq!(tx.pages.data.len(), 1024 * 10);
        assert!(!tx.lock.writable());
        {
            let open_ro_txs = tx.db.inner.open_ro_txs.lock().unwrap();
            assert_eq!(open_ro_txs.len(), 1);
            assert_eq!(open_ro_txs[0], tx.meta.tx_id);
        }
        {
            let tx = db.tx(true)?;
            assert!(tx.writable());
            {
                {
                    let inner = tx.inner.borrow_mut();
                    assert_eq!(inner.meta.tx_id, 1);
                    let freelist = inner.freelist.borrow();
                    assert_eq!(freelist.inner.pages(), Vec::<u64>::new());
                }
                let b = tx.create_bucket("abc")?;
                b.put("123", "456")?;
            }
            tx.commit()?;
        }
        {
            let tx = db.tx(true)?;
            assert!(tx.writable());
            {
                {
                    let inner = tx.inner.borrow_mut();
                    let freelist = inner.freelist.borrow();
                    assert_eq!(inner.meta.tx_id, 2);
                    assert_eq!(freelist.inner.pages(), vec![2, 3]);
                }
                let b = tx.get_bucket("abc")?;
                b.put("123", "456")?;
            }
            tx.commit()?;
        }
    }
    {
        let tx = db.tx(true)?;
        assert!(tx.writable());
        let inner = tx.inner.borrow_mut();
        let mut freelist = inner.freelist.borrow_mut();
        assert_eq!(freelist.inner.pages(), vec![2, 3, 4, 5, 6]);
        assert_eq!(freelist.meta.num_pages, 10);
        for id in 2..=6 {
            let page = freelist.allocate(size_of::<Page>() as u64)?;
            assert_eq!(page.id, id);
            assert_eq!(page.overflow, 0);
        }
        assert_eq!(freelist.meta.num_pages, 10);
        let page = freelist.allocate(size_of::<Page>() as u64)?;
        assert_eq!(page.id, 10);
        assert_eq!(page.overflow, 0);
        assert_eq!(freelist.meta.num_pages, 11);
        assert_eq!(freelist.inner.pages(), Vec::<u64>::new());
    }
    Ok(())
}
