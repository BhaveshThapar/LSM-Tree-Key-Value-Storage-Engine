//! Fuzz the record decoder: arbitrary bytes must be rejected, never panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = lsm_kv::fuzzing::decode_record(data);
});
