use std::fs;

use tagdata::{DB, Error};

fn main() -> Result<(), Error> {
    let path = std::env::temp_dir().join("tagdata-example.db");
    let _ = fs::remove_file(&path);
    let db = DB::open(&path)?;

    let tx = db.tx(true)?;
    let names = tx.create_bucket("names")?;
    names.put("Kanan", "Jarrus")?;
    names.put("Ezra", "Bridger")?;
    tx.commit()?;

    let tx = db.tx(false)?;
    let names = tx.get_bucket("names")?;
    println!(
        "Kanan {}",
        String::from_utf8_lossy(names.get_kv(b"Kanan")?.unwrap().value())
    );
    drop(names);
    drop(tx);

    fs::remove_file(path)?;
    Ok(())
}
