#![no_main]
//! Hostile archives must never escape the extraction directory or panic.
use libfuzzer_sys::fuzz_target;
use qget_core::OrderedConsumer;

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = qget_extract::ExtractOptions::new(dir.path().join("out"));
    opts.threads = 2;
    let Ok(ex) = qget_extract::Extractor::new(opts) else { return };
    let mut ex = Box::new(ex);
    if ex.consume(data).is_ok() {
        let _ = ex.finish();
    }
    // Nothing may appear outside the temporary directory's own subtree.
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let name = entry.unwrap().file_name();
        assert!(name == "out" || name == "out.qget-staging", "{name:?}");
    }
});
