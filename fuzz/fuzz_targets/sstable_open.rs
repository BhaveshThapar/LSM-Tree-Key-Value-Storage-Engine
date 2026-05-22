//! Fuzz the SSTable reader: an SSTable file of arbitrary bytes must be
//! rejected (bad magic, out-of-range offsets, ...), never panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let path = std::env::temp_dir().join(format!("lsm_fuzz_sst_{}", std::process::id()));
    if std::fs::write(&path, data).is_ok() {
        let _ = lsm_kv::fuzzing::open_sstable(&path);
        let _ = std::fs::remove_file(&path);
    }
});
