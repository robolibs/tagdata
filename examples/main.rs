use std::fs;

use inspace::{Database, Error};

fn main() -> Result<(), Error> {
    let path = std::env::temp_dir().join("inspace-example.db");
    let _ = fs::remove_file(&path);
    let db = Database::open(&path)?;

    db.update(|tx| {
        tx.create_bucket("names")?;
        tx.put("names", "Kanan", "Jarrus")?;
        tx.put("names", "Ezra", "Bridger")
    })?;

    db.view(|tx| {
        let names = tx.bucket(b"names")?;
        println!(
            "Kanan {}",
            String::from_utf8_lossy(names.get(b"Kanan").unwrap())
        );
        Ok(())
    })?;

    fs::remove_file(path)?;
    Ok(())
}
