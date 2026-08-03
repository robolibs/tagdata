use std::{env, process::ExitCode};

use inspace::OpenOptions;

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: cargo run --example verify -- <database> [page-size]");
        return ExitCode::from(2);
    };
    let page_size = match args.next() {
        Some(value) => match value.to_string_lossy().parse::<u64>() {
            Ok(value) => value,
            Err(error) => {
                eprintln!("invalid page size: {error}");
                return ExitCode::from(2);
            }
        },
        None => page_size::get() as u64,
    };

    match OpenOptions::new()
        .pagesize(page_size)
        .read_only()
        .verify_on_open(true)
        .open(path)
    {
        Ok(_) => {
            println!("database verified successfully");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("verification failed: {error}");
            ExitCode::FAILURE
        }
    }
}
