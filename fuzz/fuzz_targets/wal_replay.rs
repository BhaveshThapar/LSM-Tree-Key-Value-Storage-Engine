//! Fuzz WAL replay: a WAL file of arbitrary bytes must be rejected or replayed
//! up to the first torn frame, never panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let path = std::env::temp_dir().join(format!("lsm_fuzz_wal_{}", std::process::id()));
    if std::fs::write(&path, data).is_ok() {
        let _ = lsm_kv::fuzzing::replay_wal(&path);
        let _ = std::fs::remove_file(&path);
    }
});
