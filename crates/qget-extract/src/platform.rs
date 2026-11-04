//! The few filesystem calls that differ between Linux and macOS.

use std::fs::File;

/// Best-effort block reservation for a large extracted file.
pub fn reserve(file: &File, len: u64) {
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
    let _ = rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, len);
    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
    let _ = (file, len);
}

/// Flush the filesystem holding `dir` once, instead of an fsync per file.
pub fn sync_filesystem(dir: &File) {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let _ = rustix::fs::syncfs(dir);
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = dir;
        rustix::fs::sync();
    }
}
