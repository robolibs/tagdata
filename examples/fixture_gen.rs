use std::path::Path;

use inspace::{Error, OpenOptions};

fn main() -> Result<(), Error> {
    let directory = Path::new("tests/fixtures");
    std::fs::create_dir_all(directory)?;
    for version in 1..=3 {
        let path = directory.join(format!("format-v{version}.db"));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(format!("{}.inspace", path.display()));
        let db = OpenOptions::new()
            .pagesize(4096)
            .num_pages(8)
            .format_version(version)
            .open(&path)?;
        db.update(|tx| {
            let root = tx.create_bucket("fixture")?;
            root.put("format", version.to_be_bytes())?;
            root.put("message", "frozen")?;
            root.create_bucket("nested")?.put("key", "value")?;
            Ok(())
        })?;
        db.verify()?;
        drop(db);
        let _ = std::fs::remove_dir_all(format!("{}.inspace", path.display()));
    }
    Ok(())
}
