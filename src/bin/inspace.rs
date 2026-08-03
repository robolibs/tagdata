use std::{env, path::Path, process::ExitCode};

use inspace::{DB, FormatInfo, OpenOptions, SalvageManifest, Stats, VerifyReport};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err((json, message)) => {
            if json {
                eprintln!("{{\"ok\":false,\"error\":{}}}", string(&message));
            } else {
                eprintln!("error: {message}");
            }
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), (bool, String)> {
    let mut arguments = env::args().skip(1).collect::<Vec<_>>();
    let json = arguments
        .first()
        .is_some_and(|argument| argument == "--json");
    if json {
        arguments.remove(0);
    }
    let Some(command) = arguments.first().map(String::as_str) else {
        return Err((json, usage().into()));
    };
    let result = match command {
        "info" if arguments.len() == 2 => info(&arguments[1], json),
        "stats" if arguments.len() == 2 => stats(&arguments[1], json),
        "verify" if arguments.len() == 2 => verify(&arguments[1], json),
        "backup" if arguments.len() == 3 => backup(&arguments[1], &arguments[2], json),
        "compact" if (3..=4).contains(&arguments.len()) => compact(
            "compact",
            &arguments[1],
            &arguments[2],
            arguments.get(3),
            json,
        ),
        "migrate" if (3..=4).contains(&arguments.len()) => compact(
            "migrate",
            &arguments[1],
            &arguments[2],
            arguments.get(3),
            json,
        ),
        "salvage" if arguments.len() == 3 => salvage(&arguments[1], &arguments[2], json),
        _ => Err(usage().into()),
    };
    result.map_err(|message| (json, message))
}

fn info(path: &str, json: bool) -> Result<(), String> {
    let info = FormatInfo::inspect(path).map_err(|error| error.to_string())?;
    if json {
        println!(
            "{{\"file_bytes\":{},\"metadata_page\":{},\"page_size\":{},\"transaction_id\":{},\"version\":{}}}",
            info.file_bytes, info.metadata_page, info.page_size, info.transaction_id, info.version
        );
    } else {
        println!("page size:      {}", info.page_size);
        println!("format version: {}", info.version);
        println!("transaction:    {}", info.transaction_id);
        println!("file bytes:     {}", info.file_bytes);
    }
    Ok(())
}

fn stats(path: &str, json: bool) -> Result<(), String> {
    let db = OpenOptions::new()
        .read_only()
        .open(path)
        .map_err(|error| error.to_string())?;
    let stats = db.stats().map_err(|error| error.to_string())?;
    if json {
        println!("{}", stats_json(&stats));
    } else {
        println!("{stats:#?}");
    }
    Ok(())
}

fn verify(path: &str, json: bool) -> Result<(), String> {
    let db = OpenOptions::new()
        .read_only()
        .open(path)
        .map_err(|error| error.to_string())?;
    let report = db.verify_report().map_err(|error| error.to_string())?;
    if json {
        println!("{}", verify_json(&report));
    } else if report.valid {
        println!("database verified successfully");
    } else {
        for issue in &report.issues {
            println!("verification issue: {}", issue.invariant);
        }
    }
    if report.valid {
        Ok(())
    } else {
        Err("database verification failed".into())
    }
}

fn backup(source: &str, destination: &str, json: bool) -> Result<(), String> {
    let db = DB::open(source).map_err(|error| error.to_string())?;
    db.physical_backup_to(destination)
        .map_err(|error| error.to_string())?;
    success("backup", destination, json);
    Ok(())
}

fn compact(
    command: &str,
    source: &str,
    destination: &str,
    page_size: Option<&String>,
    json: bool,
) -> Result<(), String> {
    let db = OpenOptions::new()
        .read_only()
        .open(source)
        .map_err(|error| error.to_string())?;
    match page_size {
        Some(page_size) => db.compact_to_with_page_size(
            destination,
            page_size
                .parse()
                .map_err(|error| format!("invalid page size: {error}"))?,
        ),
        None => db.compact_to(destination),
    }
    .map_err(|error| error.to_string())?;
    success(command, destination, json);
    Ok(())
}

fn salvage(source: &str, destination: &str, json: bool) -> Result<(), String> {
    let db = OpenOptions::new()
        .read_only()
        .open(source)
        .map_err(|error| error.to_string())?;
    let manifest = db
        .salvage_to(destination)
        .map_err(|error| error.to_string())?;
    if json {
        println!("{}", salvage_json(&manifest));
    } else {
        println!("copied buckets: {}", manifest.copied_buckets);
        println!("copied records: {}", manifest.copied_records);
        println!("skipped pages:  {}", manifest.skipped_pages.len());
        println!("skipped records:{}", manifest.skipped_records.len());
    }
    Ok(())
}

fn success(command: &str, destination: &str, json: bool) {
    if json {
        println!(
            "{{\"command\":{},\"destination\":{},\"ok\":true}}",
            string(command),
            string(destination)
        );
    } else {
        println!("{command} written to {}", Path::new(destination).display());
    }
}

fn stats_json(stats: &Stats) -> String {
    format!(
        "{{\"active_readers\":{},\"allocated_pages\":{},\"bytes_written\":{},\"committed_transactions\":{},\"current_tx_id\":{},\"file_bytes\":{},\"free_pages\":{},\"oldest_reader_tx_id\":{},\"page_size\":{},\"pending_pages\":{},\"reader_pinned_pages\":{}}}",
        stats.active_readers,
        stats.allocated_pages,
        stats.bytes_written,
        stats.committed_transactions,
        stats.current_tx_id,
        stats.file_bytes,
        stats.free_pages,
        option_u64(stats.oldest_reader_tx_id),
        stats.page_size,
        stats.pending_pages,
        stats.reader_pinned_pages
    )
}

fn verify_json(report: &VerifyReport) -> String {
    let issues = report
        .issues
        .iter()
        .map(|issue| {
            format!(
                "{{\"bucket_path\":[],\"invariant\":{},\"offset\":{},\"page_id\":{},\"page_kind\":{}}}",
                string(&issue.invariant),
                option_u64(issue.offset),
                option_u64(issue.page_id),
                issue.page_kind.as_ref().map_or_else(|| "null".into(), |kind| string(kind))
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{\"issues\":[{issues}],\"valid\":{}}}", report.valid)
}

fn salvage_json(manifest: &SalvageManifest) -> String {
    let skipped_records = manifest
        .skipped_records
        .iter()
        .map(|record| {
            let path = record
                .bucket_path
                .iter()
                .map(|part| bytes(part))
                .collect::<Vec<_>>()
                .join(",");
            let key = record
                .key
                .as_ref()
                .map_or_else(|| "null".into(), |key| bytes(key));
            format!(
                "{{\"bucket_path\":[{path}],\"key\":{key},\"reason\":{}}}",
                string(&record.reason)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let skipped_pages = manifest
        .skipped_pages
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"copied_buckets\":{},\"copied_records\":{},\"skipped_pages\":[{skipped_pages}],\"skipped_records\":[{skipped_records}]}}",
        manifest.copied_buckets, manifest.copied_records,
    )
}

fn bytes(value: &[u8]) -> String {
    format!(
        "[{}]",
        value
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn option_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".into(), |value| value.to_string())
}

fn string(value: &str) -> String {
    let mut output = String::from("\"");
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                output.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => output.push(character),
        }
    }
    output.push('"');
    output
}

fn usage() -> &'static str {
    "usage: inspace [--json] <info|stats|verify> <database>\n       inspace [--json] <backup|salvage> <source> <destination>\n       inspace [--json] <compact|migrate> <source> <destination> [page-size]"
}
