#![cfg(feature = "test-hooks")]

use std::process::Command;

use inspace::{DB, Error};

mod common;

const STAGES: [(&str, Expected); 4] = [
    ("after-data-write", Expected::Old),
    ("after-data-sync", Expected::Old),
    ("after-meta-write", Expected::Either),
    ("after-meta-sync", Expected::New),
];

#[derive(Clone, Copy)]
enum Expected {
    Old,
    New,
    Either,
}

#[test]
fn interrupted_commits_recover_complete_snapshots() -> Result<(), Error> {
    for (stage, expected) in STAGES {
        let file = common::RandomFile::new();
        initialize(&file)?;

        let status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("crash_commit_child")
            .arg("--nocapture")
            .env("INSPACE_CRASH_DB", &file.path)
            .env("INSPACE_FAILPOINT", stage)
            .status()?;
        assert!(!status.success(), "failpoint {stage} did not terminate");

        let db = DB::open(&file)?;
        let tx = db.tx(false)?;
        let bucket = tx.get_bucket("state")?;
        let value = bucket.get_kv("value").unwrap().value().to_vec();
        let overflow = bucket.get_kv("overflow").unwrap().value().to_vec();
        let nested_bucket = bucket.get_bucket("nested")?;
        let nested = nested_bucket.get_kv("value").unwrap().value().to_vec();
        let observed = if value == b"old" {
            assert_eq!(overflow, vec![3; 64 * 1024], "stage {stage}");
            assert_eq!(nested, b"old", "stage {stage}");
            Expected::Old
        } else {
            assert_eq!(value, b"new", "stage {stage}");
            assert_eq!(overflow, vec![7; 256 * 1024], "stage {stage}");
            assert_eq!(nested, b"new", "stage {stage}");
            Expected::New
        };
        match expected {
            Expected::Old => assert!(matches!(observed, Expected::Old), "stage {stage}"),
            Expected::New => assert!(matches!(observed, Expected::New), "stage {stage}"),
            Expected::Either => {}
        }
        drop(bucket);
        drop(tx);
        db.check()?;
    }
    Ok(())
}

#[test]
fn crash_commit_child() -> Result<(), Error> {
    let Ok(path) = std::env::var("INSPACE_CRASH_DB") else {
        return Ok(());
    };

    let db = DB::open(path)?;
    let tx = db.tx(true)?;
    let bucket = tx.get_bucket("state")?;
    bucket.put("value", "new")?;
    bucket.put("overflow", vec![7; 256 * 1024])?;
    bucket.get_bucket("nested")?.put("value", "new")?;
    tx.commit()
}

fn initialize(file: &common::RandomFile) -> Result<(), Error> {
    let db = DB::open(file)?;
    let tx = db.tx(true)?;
    let bucket = tx.create_bucket("state")?;
    bucket.put("value", "old")?;
    bucket.put("overflow", vec![3; 64 * 1024])?;
    bucket.create_bucket("nested")?.put("value", "old")?;
    tx.commit()
}
