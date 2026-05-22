//! `lsm` — a thin CLI for manual smoke-testing the engine.
//!
//! Usage:
//!   lsm <dir> put <key> <value>
//!   lsm <dir> get <key>
//!   lsm <dir> delete <key>
//!   lsm <dir> flush

use std::process::ExitCode;

use lsm_kv::Db;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (dir, cmd) = match args.split_first() {
        Some((dir, rest)) if !rest.is_empty() => (dir, rest),
        _ => return Err(usage()),
    };

    let mut db = Db::open(dir).map_err(|e| e.to_string())?;
    match cmd[0].as_str() {
        "put" if cmd.len() == 3 => {
            db.put(cmd[1].as_bytes(), cmd[2].as_bytes())
                .map_err(|e| e.to_string())?;
            println!("OK");
        }
        "get" if cmd.len() == 2 => match db.get(cmd[1].as_bytes()).map_err(|e| e.to_string())? {
            Some(v) => println!("{}", String::from_utf8_lossy(&v)),
            None => {
                println!("(nil)");
                return Ok(());
            }
        },
        "delete" if cmd.len() == 2 => {
            db.delete(cmd[1].as_bytes()).map_err(|e| e.to_string())?;
            println!("OK");
        }
        "flush" if cmd.len() == 1 => {
            db.flush().map_err(|e| e.to_string())?;
            println!("OK ({} sstables)", db.sstable_count());
        }
        _ => return Err(usage()),
    }
    Ok(())
}

fn usage() -> String {
    "usage: lsm <dir> <put k v | get k | delete k | flush>".to_string()
}
