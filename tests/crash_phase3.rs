//! Phase 3 crash-safety tests: a SIGKILL while background flush and background
//! compaction are both running, plus reclamation of partial files left behind
//! by a crash.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

use lsm_kv::{Db, Options};

const CHILD_ENV: &str = "LSM_PHASE3_CHILD";
const DIR_ENV: &str = "LSM_PHASE3_DIR";

/// A small MemTable threshold forces frequent flushes, so the background flush
/// worker and the background compaction thread are both busy when the crash
/// lands. `sync_wal` keeps every acked write durable.
fn opts() -> Options {
    Options {
        memtable_threshold: 4 * 1024,
        sync_wal: true,
        compaction_threshold: 4,
        ..Options::default()
    }
}

/// When spawned as the child, write forever and never return.
fn run_child_if_requested() {
    if std::env::var(CHILD_ENV).is_err() {
        return;
    }
    let dir = std::env::var(DIR_ENV).expect("child needs target dir");
    let db = Db::open_with(&dir, opts()).expect("child: open");
    let stdout = std::io::stdout();
    for i in 0u64.. {
        let key = format!("key{i:08}");
        let value = format!("value-for-{i}-padded-padded-padded");
        db.put(key.as_bytes(), value.as_bytes())
            .expect("child: put");
        let mut lock = stdout.lock();
        writeln!(lock, "ACK {i}").unwrap();
        lock.flush().unwrap();
    }
    unreachable!();
}

#[test]
fn acked_writes_survive_sigkill_during_background_work() {
    run_child_if_requested();

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(exe)
        .args([
            "--exact",
            "acked_writes_survive_sigkill_during_background_work",
            "--nocapture",
        ])
        .env(CHILD_ENV, "1")
        .env(DIR_ENV, &dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child");

    // Read acks until enough flushes and compactions have happened to be
    // interesting (the small threshold means thousands of writes == many
    // SSTables and several compaction rounds).
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut last_acked: i64 = -1;
    let mut line = String::new();
    while last_acked < 6000 {
        line.clear();
        let n = reader.read_line(&mut line).expect("read child stdout");
        assert!(n > 0, "child exited before producing enough writes");
        if let Some(idx) = line.trim().strip_prefix("ACK ") {
            last_acked = idx.parse().expect("child printed a malformed ACK");
        }
    }

    // Kill hard, mid-write — no destructors, no flush.
    std::thread::sleep(Duration::from_millis(20));
    child.kill().expect("SIGKILL child");
    child.wait().expect("reap child");

    // Reopen: the manifest must reconstruct the live SSTable set, reclaim any
    // orphan tables/tmp files, and the WAL must cover the rest.
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

#[test]
fn partial_tmp_file_is_reclaimed_on_open() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    {
        let db = Db::open_with(dir, opts()).expect("open");
        db.put(b"k", b"v").expect("put");
        db.flush().expect("flush");
    }
    // Simulate a crash mid-flush: a half-written SSTable left as a `.tmp`.
    let stale = dir.join("sst_0000009999.db.tmp");
    std::fs::write(&stale, b"garbage half-written sstable").unwrap();

    let db = Db::open_with(dir, opts()).expect("reopen");
    assert!(!stale.exists(), "partial .tmp file should be reclaimed");
    assert_eq!(db.get(b"k").expect("get"), Some(b"v".to_vec()));
}
