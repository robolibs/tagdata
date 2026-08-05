#![cfg(feature = "operator")]

use std::process::Command;

use tagdata::{DB, Error, FORMAT_VERSION};

mod common;

#[test]
fn operator_commands_emit_stable_json_and_create_outputs() -> Result<(), Error> {
    let source = common::RandomFile::new();
    let backup = common::RandomFile::new();
    let compact = common::RandomFile::new();
    let salvaged = common::RandomFile::new();
    let db = DB::open(&source)?;
    db.update(|tx| {
        tx.create_bucket("items")?.put("key", "value")?;
        Ok(())
    })?;
    drop(db);

    let info = command(["--json", "info", path(&source)])?;
    assert_eq!(info["version"], FORMAT_VERSION);
    assert!(info["page_size"].as_u64().unwrap() >= 1024);

    let stats = command(["--json", "stats", path(&source)])?;
    assert!(stats["allocated_pages"].as_u64().unwrap() >= 4);

    let verify = command(["--json", "verify", path(&source)])?;
    assert_eq!(verify["valid"], true);
    assert_eq!(verify["issues"], serde_json::json!([]));

    let result = command(["--json", "backup", path(&source), path(&backup)])?;
    assert_eq!(result["command"], "backup");
    DB::open(&backup)?.verify()?;

    command(["--json", "compact", path(&source), path(&compact), "2048"])?;
    assert_eq!(DB::open(&compact)?.pagesize(), 2048);
    let manifest = command(["--json", "salvage", path(&source), path(&salvaged)])?;
    assert_eq!(manifest["copied_records"], 1);
    DB::open(&salvaged)?.verify()
}

fn command<const N: usize>(arguments: [&str; N]) -> Result<serde_json::Value, Error> {
    let output = Command::new(env!("CARGO_BIN_EXE_tagdata"))
        .args(arguments)
        .output()?;
    assert!(
        output.status.success(),
        "operator failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout)
        .map_err(|error| Error::InvalidDB(format!("invalid operator JSON: {error}")))
}

fn path(file: &common::RandomFile) -> &str {
    file.path.to_str().expect("temporary path is UTF-8")
}
