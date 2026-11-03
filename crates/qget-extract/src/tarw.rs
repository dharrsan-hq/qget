//! Tar extraction with a single in-order header parser and a pool of file
//! writers (EXT-04), hardened against hostile archives (EXT-06, SEC-03).
//!
//! Safety rules:
//! - entry paths must be relative and may not contain `..`
//! - nothing is written through a symlink created by the archive
//! - symlink targets may not be absolute or escape the extraction root
//! - hard-link targets must be inside the root
//! - device nodes and FIFOs are refused (or skipped with `allow_special`)
//! - setuid/setgid/sticky bits are dropped

use crossbeam_channel::{Sender, bounded};
use qget_core::{Error, Result};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// Entries up to this size are buffered and written by the pool.
const SMALL_FILE: u64 = 4 << 20;
const MAX_PATH: usize = 4096;

#[derive(Clone, Debug)]
pub struct TarOptions {
    pub root: PathBuf,
    pub threads: usize,
    pub allow_special: bool,
    pub max_entries: u64,
}

#[derive(Clone, Debug, Default)]
pub struct TarReport {
    pub entries: u64,
    pub bytes: u64,
    pub notes: Vec<String>,
}

enum WJob {
    File {
        path: PathBuf,
        data: Vec<u8>,
        mode: u32,
        mtime: u64,
    },
    Flush(Sender<()>),
}

struct Pool {
    senders: Vec<Sender<WJob>>,
    handles: Vec<JoinHandle<()>>,
    error: Arc<Mutex<Option<String>>>,
}

impl Pool {
    fn new(threads: usize) -> Self {
        let error = Arc::new(Mutex::new(None));
        let mut senders = Vec::new();
        let mut handles = Vec::new();
        for i in 0..threads.max(1) {
            let (tx, rx) = bounded::<WJob>(32);
            let err = error.clone();
            handles.push(
                std::thread::Builder::new()
                    .name(format!("qget-write-{i}"))
                    .spawn(move || {
                        while let Ok(job) = rx.recv() {
                            match job {
                                WJob::File {
                                    path,
                                    data,
                                    mode,
                                    mtime,
                                } => {
                                    if let Err(e) = write_file(&path, &data, mode, mtime) {
                                        let mut g = err.lock().expect("error lock");
                                        g.get_or_insert(format!("{}: {e}", path.display()));
                                    }
                                }
                                WJob::Flush(ack) => {
                                    let _ = ack.send(());
                                }
                            }
                        }
                    })
                    .expect("spawn writer"),
            );
            senders.push(tx);
        }
        Self {
            senders,
            handles,
            error,
        }
    }

    /// Same path → same worker, so repeated entries keep archive order.
    fn worker_for(&self, rel: &Path) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        rel.hash(&mut h);
        (h.finish() % self.senders.len() as u64) as usize
    }

    fn send(&self, idx: usize, job: WJob) -> Result<()> {
        self.check()?;
        self.senders[idx]
            .send(job)
            .map_err(|_| Error::Extract("writer pool stopped".into()))
    }

    fn flush(&self, idx: usize) -> Result<()> {
        let (tx, rx) = bounded(1);
        self.send(idx, WJob::Flush(tx))?;
        rx.recv()
            .map_err(|_| Error::Extract("writer pool stopped".into()))?;
        self.check()
    }

    fn flush_all(&self) -> Result<()> {
        for i in 0..self.senders.len() {
            self.flush(i)?;
        }
        Ok(())
    }

    fn check(&self) -> Result<()> {
        match self.error.lock().expect("error lock").as_ref() {
            Some(e) => Err(Error::Extract(e.clone())),
            None => Ok(()),
        }
    }

    fn finish(self) -> Result<()> {
        drop(self.senders);
        for h in self.handles {
            let _ = h.join();
        }
        match self.error.lock().expect("error lock").take() {
            Some(e) => Err(Error::Extract(e)),
            None => Ok(()),
        }
    }
}

fn open_nofollow(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)
}

fn write_file(path: &Path, data: &[u8], mode: u32, mtime: u64) -> std::io::Result<()> {
    let mut f = open_nofollow(path)?;
    f.write_all(data)?;
    finish_file(&f, path, mode, mtime)
}

fn finish_file(f: &File, path: &Path, mode: u32, mtime: u64) -> std::io::Result<()> {
    f.set_permissions(fs::Permissions::from_mode(mode))?;
    let t = filetime::FileTime::from_unix_time(mtime as i64, 0);
    filetime::set_file_handle_times(f, Some(t), Some(t))
        .or_else(|_| filetime::set_file_mtime(path, t))
}

/// Normalise an entry path to a safe relative path. `None` for the root itself.
fn sanitize(p: &Path) -> Result<Option<PathBuf>> {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::Normal(s) => out.push(s),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(Error::Extract(format!(
                    "refusing entry with '..': {}",
                    p.display()
                )));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(Error::Extract(format!(
                    "refusing absolute path: {}",
                    p.display()
                )));
            }
        }
    }
    if out.as_os_str().len() > MAX_PATH {
        return Err(Error::Extract("entry path too long".into()));
    }
    Ok((!out.as_os_str().is_empty()).then_some(out))
}

/// Whether a symlink at `rel` pointing to `target` stays inside the root.
fn symlink_target_ok(rel: &Path, target: &Path) -> bool {
    if target.is_absolute() {
        return false;
    }
    let mut depth: i64 = rel.parent().map_or(0, |p| p.components().count() as i64);
    for c in target.components() {
        match c {
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

pub fn extract_tar<R: Read>(reader: R, opts: &TarOptions) -> Result<TarReport> {
    let pool = Pool::new(opts.threads);
    let result = extract_inner(reader, opts, &pool);
    let finished = pool.finish();
    let report = result?;
    finished?;
    Ok(report)
}

fn extract_inner<R: Read>(reader: R, opts: &TarOptions, pool: &Pool) -> Result<TarReport> {
    let root = &opts.root;
    let mut archive = tar::Archive::new(reader);
    let mut report = TarReport::default();
    let mut symlinks: HashSet<PathBuf> = HashSet::new();
    let mut created_dirs: HashSet<PathBuf> = HashSet::new();
    let mut dir_meta: Vec<(PathBuf, u32, u64)> = Vec::new();
    let mut skipped_special = 0u64;
    let ext = |e: std::io::Error| Error::Extract(e.to_string());

    let entries = archive.entries().map_err(ext)?;
    for entry in entries {
        let mut entry = entry.map_err(ext)?;
        report.entries += 1;
        if report.entries > opts.max_entries {
            return Err(Error::Extract("too many entries".into()));
        }
        let raw_path = entry.path().map_err(ext)?.into_owned();
        let Some(rel) = sanitize(&raw_path)? else {
            continue;
        };
        // Nothing may be written through an archive-created symlink.
        for anc in rel.ancestors().skip(1) {
            if symlinks.contains(anc) {
                return Err(Error::Extract(format!(
                    "refusing to write {} through symlink {}",
                    rel.display(),
                    anc.display()
                )));
            }
        }
        let header = entry.header();
        let kind = header.entry_type();
        let mode = header.mode().unwrap_or(0o644) & 0o777;
        let mtime = header.mtime().unwrap_or(0);
        let target = root.join(&rel);
        let ensure_parent = |created: &mut HashSet<PathBuf>| -> Result<()> {
            if let Some(parent) = rel.parent()
                && !parent.as_os_str().is_empty()
                && !created.contains(parent)
            {
                fs::create_dir_all(root.join(parent))
                    .map_err(|e| Error::io(format!("create {}", parent.display()), e))?;
                for a in parent.ancestors() {
                    created.insert(a.to_path_buf());
                }
            }
            Ok(())
        };
        use tar::EntryType as T;
        match kind {
            T::Directory => {
                fs::create_dir_all(&target)
                    .map_err(|e| Error::io(format!("create {}", rel.display()), e))?;
                for a in rel.ancestors() {
                    created_dirs.insert(a.to_path_buf());
                }
                dir_meta.push((target, mode | 0o700, mtime));
            }
            T::Regular | T::Continuous | T::GNUSparse => {
                ensure_parent(&mut created_dirs)?;
                let idx = pool.worker_for(&rel);
                if symlinks.remove(&rel) {
                    pool.flush(idx)?;
                    let _ = fs::remove_file(&target);
                }
                let size = entry.size();
                report.bytes += size;
                if size <= SMALL_FILE {
                    let mut data = Vec::with_capacity(size as usize);
                    entry.read_to_end(&mut data).map_err(ext)?;
                    pool.send(
                        idx,
                        WJob::File {
                            path: target,
                            data,
                            mode,
                            mtime,
                        },
                    )?;
                } else {
                    // Large file: stream it here, after earlier writes to the same path.
                    pool.flush(idx)?;
                    let mut f = open_nofollow(&target)
                        .map_err(|e| Error::io(format!("create {}", rel.display()), e))?;
                    let _ = rustix::fs::fallocate(&f, rustix::fs::FallocateFlags::empty(), 0, size);
                    let mut buf = vec![0u8; 1 << 20];
                    loop {
                        let n = entry.read(&mut buf).map_err(ext)?;
                        if n == 0 {
                            break;
                        }
                        f.write_all(&buf[..n])
                            .map_err(|e| Error::io(format!("write {}", rel.display()), e))?;
                    }
                    finish_file(&f, &target, mode, mtime)
                        .map_err(|e| Error::io(format!("finish {}", rel.display()), e))?;
                }
            }
            T::Symlink => {
                let link = entry
                    .link_name()
                    .map_err(ext)?
                    .ok_or_else(|| Error::Extract("symlink without target".into()))?
                    .into_owned();
                if !symlink_target_ok(&rel, &link) {
                    return Err(Error::Extract(format!(
                        "refusing symlink {} -> {} (escapes the extraction directory)",
                        rel.display(),
                        link.display()
                    )));
                }
                ensure_parent(&mut created_dirs)?;
                pool.flush(pool.worker_for(&rel))?;
                let _ = fs::remove_file(&target);
                std::os::unix::fs::symlink(&link, &target)
                    .map_err(|e| Error::io(format!("symlink {}", rel.display()), e))?;
                symlinks.insert(rel);
            }
            T::Link => {
                let link = entry
                    .link_name()
                    .map_err(ext)?
                    .ok_or_else(|| Error::Extract("hard link without target".into()))?
                    .into_owned();
                let Some(link_rel) = sanitize(&link)? else {
                    return Err(Error::Extract("hard link to the root".into()));
                };
                if symlinks.contains(&link_rel)
                    || link_rel.ancestors().skip(1).any(|a| symlinks.contains(a))
                {
                    return Err(Error::Extract(format!(
                        "refusing hard link {} via symlink",
                        rel.display()
                    )));
                }
                ensure_parent(&mut created_dirs)?;
                pool.flush_all()?;
                let _ = fs::remove_file(&target);
                fs::hard_link(root.join(&link_rel), &target).map_err(|e| {
                    Error::io(
                        format!("hard link {} -> {}", rel.display(), link_rel.display()),
                        e,
                    )
                })?;
            }
            T::Char | T::Block | T::Fifo => {
                if !opts.allow_special {
                    return Err(Error::Extract(format!(
                        "refusing special file {} (use --allow-special to skip such entries)",
                        rel.display()
                    )));
                }
                skipped_special += 1;
            }
            // PAX/GNU metadata entries are consumed by the tar crate; skip the rest.
            _ => {}
        }
    }
    pool.flush_all()?;
    // Directory permissions and times last, deepest first.
    dir_meta.sort_by_key(|(p, _, _)| std::cmp::Reverse(p.components().count()));
    for (p, mode, mtime) in dir_meta {
        let _ = fs::set_permissions(&p, fs::Permissions::from_mode(mode));
        let t = filetime::FileTime::from_unix_time(mtime as i64, 0);
        let _ = filetime::set_file_mtime(&p, t);
    }
    if skipped_special > 0 {
        report
            .notes
            .push(format!("skipped {skipped_special} special files"));
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_paths() {
        assert_eq!(
            sanitize(Path::new("./a/b")).unwrap(),
            Some(PathBuf::from("a/b"))
        );
        assert_eq!(sanitize(Path::new("./")).unwrap(), None);
        assert!(sanitize(Path::new("../x")).is_err());
        assert!(sanitize(Path::new("a/../../x")).is_err());
        assert!(sanitize(Path::new("/etc/passwd")).is_err());
    }

    #[test]
    fn symlink_targets() {
        assert!(symlink_target_ok(Path::new("a/link"), Path::new("../b")));
        assert!(symlink_target_ok(Path::new("a/link"), Path::new("c/d")));
        assert!(!symlink_target_ok(
            Path::new("a/link"),
            Path::new("../../etc")
        ));
        assert!(!symlink_target_ok(Path::new("link"), Path::new("/etc")));
        assert!(!symlink_target_ok(Path::new("link"), Path::new("..")));
    }
}
