#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut input = qget_core::journal::MAGIC.to_vec();
    input.extend_from_slice(data);
    let d = qget_core::journal::decode(&input);
    assert!(d.valid_len as usize <= input.len());
});
