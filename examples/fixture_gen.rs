use std::path::Path;

use inspace::{Error, FORMAT_VERSION, OpenOptions};

fn main() -> Result<(), Error> {
    let directory = Path::new("tests/fixtures");
    std::fs::create_dir_all(directory)?;
    let path = directory.join("current.db");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(format!("{}.inspace", path.display()));
    let db = OpenOptions::new().pagesize(4096).num_pages(8).open(&path)?;
    db.update(|tx| {
        let root = tx.create_bucket("fixture")?;
        root.put("format", FORMAT_VERSION.to_be_bytes())?;
        root.put("message", "frozen")?;
        root.create_bucket("nested")?.put("key", "value")?;
        Ok(())
    })?;
    db.verify()?;
    drop(db);
    let _ = std::fs::remove_dir_all(format!("{}.inspace", path.display()));
    Ok(())
}
