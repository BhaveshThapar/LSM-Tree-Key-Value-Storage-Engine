//! Crash-recovery integration test.
//!
//! A child process appends keys with `sync_wal = true` and prints the index of
//! every durably-acked write to stdout. The parent lets it run, then `SIGKILL`s
//! it mid-write, reopens the database, and asserts that *every* acked write
//! survived. A small MemTable threshold ensures flushes also happen mid-run, so
//! the test exercises crash safety of both the WAL and the flush path.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

use lsm_kv::{Db, Options};

const CHILD_ENV: &str = "LSM_CRASH_CHILD";
const DIR_ENV: &str = "LSM_CRASH_DIR";

fn opts() -> Options {
    Options {
        memtable_threshold: 16 * 1024,
        sync_wal: true,
    }
}

/// When spawned as the child, write forever and never return.
fn run_child_if_requested() {
    if std::env::var(CHILD_ENV).is_err() {
        return;
    }
    let dir = std::env::var(DIR_ENV).expect("child needs target dir");
    let mut db = Db::open_with(&dir, opts()).expect("child: open");
    let stdout = std::io::stdout();
    for i in 0u64.. {
        let key = format!("key{i:08}");
        let value = format!("value-for-{i}-padded-padded-padded");
        db.put(key.as_bytes(), value.as_bytes())
            .expect("child: put");
        // The WAL is synced, so this write is durable: ack it. The "ACK "
        // prefix lets the parent ignore interleaved test-harness output.
        use std::io::Write;
        let mut lock = stdout.lock();
        writeln!(lock, "ACK {i}").unwrap();
        lock.flush().unwrap();
    }
    unreachable!();
}

#[test]
fn acked_writes_survive_sigkill() {
    run_child_if_requested();

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    // Re-exec this test binary, running only this test, in child mode.
    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(exe)
        .args(["--exact", "acked_writes_survive_sigkill", "--nocapture"])
        .env(CHILD_ENV, "1")
        .env(DIR_ENV, &dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child");

    // Read acked write indices until we've seen enough to be interesting.
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut last_acked: i64 = -1;
    let mut line = String::new();
    while last_acked < 3000 {
        line.clear();
        let n = reader.read_line(&mut line).expect("read child stdout");
        assert!(n > 0, "child exited before producing enough writes");
        if let Some(idx) = line.trim().strip_prefix("ACK ") {
            last_acked = idx.parse().expect("child printed a malformed ACK");
        }
    }

    // Give the child a moment to keep writing past last_acked, then kill it
    // hard — no destructors, no flush, just like a power loss.
    std::thread::sleep(Duration::from_millis(20));
    child.kill().expect("SIGKILL child");
    child.wait().expect("reap child");

    // Reopen and verify every acked write is present and correct.
    let db = Db::open_with(&dir, opts()).expect("parent: reopen");
    for i in 0..=last_acked as u64 {
        let key = format!("key{i:08}");
        let expected = format!("value-for-{i}-padded-padded-padded");
        assert_eq!(
            db.get(key.as_bytes()).expect("parent: get"),
            Some(expected.into_bytes()),
            "acked write {i} was lost after crash"
        );
    }
}
