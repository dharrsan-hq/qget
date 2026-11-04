//! Small filesystem helpers with the durability rules from Pillai et al.
//! (OSDI'14): renames and creations are only durable after the parent
//! directory is fsynced.

use qget_core::{Error, Result};
use std::fs::File;
use std::path::Path;

/// fsync the directory containing `path`.
pub fn sync_parent(path: &Path) -> Result<()> {
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| Error::io(format!("fsync directory {}", dir.display()), e))
}

/// Atomically replace `path` with `bytes`: write tmp, fsync, rename, fsync dir.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".qget-tmp");
    let tmp = std::path::PathBuf::from(tmp);
    let mut f =
        File::create(&tmp).map_err(|e| Error::io(format!("create {}", tmp.display()), e))?;
    f.write_all(bytes)
        .and_then(|_| f.sync_all())
        .map_err(|e| Error::io(format!("write {}", tmp.display()), e))?;
    std::fs::rename(&tmp, path).map_err(|e| Error::io(format!("rename {}", path.display()), e))?;
    sync_parent(path)
}

/// Bytes available to unprivileged users on the filesystem holding `path`.
pub fn available_space(path: &Path) -> Option<u64> {
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let st = rustix::fs::statvfs(dir).ok()?;
    Some(st.f_bavail.saturating_mul(st.f_frsize))
}

/// Bytes already allocated on disk for `file`.
pub fn allocated(file: &File) -> u64 {
    use std::os::unix::fs::MetadataExt;
    file.metadata().map(|m| m.blocks() * 512).unwrap_or(0)
}

/// Reserve `len` bytes for `file` so a full disk fails now, not at 90% (DUR-07).
pub fn preallocate(file: &File, len: u64) -> Result<()> {
    if len == 0 {
        return Ok(());
    }
    match reserve(file, len) {
        Ok(()) => Ok(()),
        Err(rustix::io::Errno::NOSPC) => Err(Error::DiskFull {
            needed: len,
            available: 0,
        }),
        // Filesystems without preallocation (e.g. some FUSE/tmpfs): sparse extend.
        Err(_) => {
            if file.metadata().map(|m| m.len()).unwrap_or(0) < len {
                file.set_len(len)
                    .map_err(|e| Error::io("extend output file", e))?;
            }
            Ok(())
        }
    }
}

/// Allocate disk blocks for `[0, len)` without writing them.
#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
pub fn reserve(file: &File, len: u64) -> rustix::io::Result<()> {
    rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, len)
}

/// macOS has no fallocate; the file is extended sparsely instead.
#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
pub fn reserve(_file: &File, _len: u64) -> rustix::io::Result<()> {
    Err(rustix::io::Errno::NOSYS)
}

/// Flush everything on the filesystem holding `dir` (one call instead of an
/// fsync per extracted file). Elsewhere, falls back to `sync()`.
pub fn sync_filesystem(dir: &File) {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let _ = rustix::fs::syncfs(dir);
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = dir;
        rustix::fs::sync();
    }
}

/// Take an exclusive, non-blocking lock on `file` (DUR-09).
pub fn try_lock(file: &File) -> bool {
    rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive).is_ok()
}

/// Whether the block device under `path` is rotational (an HDD). Linux only;
/// other systems report `false`.
#[cfg(target_os = "linux")]
pub fn is_rotational(path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let Ok(meta) = std::fs::metadata(dir) else {
        return false;
    };
    let dev = meta.dev();
    let (major, minor) = (rustix::fs::major(dev), rustix::fs::minor(dev));
    // /sys/dev/block/M:m points at a partition or disk; the queue lives on the disk.
    let base = format!("/sys/dev/block/{major}:{minor}");
    for candidate in [
        format!("{base}/queue/rotational"),
        format!("{base}/../queue/rotational"),
    ] {
        if let Ok(v) = std::fs::read_to_string(&candidate) {
            return v.trim() == "1";
        }
    }
    false
}

#[cfg(not(target_os = "linux"))]
pub fn is_rotational(_path: &Path) -> bool {
    false
}
