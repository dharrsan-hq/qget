use super::*;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn data(len: usize, seed: u8) -> Vec<u8> {
    // Compressible but non-trivial content.
    (0..len)
        .map(|i| {
            ((i / 7) as u8)
                .wrapping_mul(seed)
                .wrapping_add((i % 13) as u8)
        })
        .collect()
}

/// A tar with nested dirs, small and large files, an internal symlink and a hard link.
fn sample_tar() -> (Vec<u8>, Vec<(String, Vec<u8>)>) {
    let mut b = tar::Builder::new(Vec::new());
    let files = vec![
        ("data/small.txt".to_string(), b"hello world\n".to_vec()),
        ("data/sub/medium.bin".to_string(), data(300_000, 3)),
        ("data/big.bin".to_string(), data(9 << 20, 5)),
        ("data/empty".to_string(), Vec::new()),
    ];
    let mut dir = tar::Header::new_gnu();
    dir.set_entry_type(tar::EntryType::Directory);
    dir.set_mode(0o755);
    dir.set_size(0);
    b.append_data(&mut dir, "data/", std::io::empty()).unwrap();
    for (p, d) in &files {
        let mut h = tar::Header::new_gnu();
        h.set_size(d.len() as u64);
        h.set_mode(0o640);
        h.set_mtime(1_700_000_000);
        b.append_data(&mut h, p, &d[..]).unwrap();
    }
    let mut l = tar::Header::new_gnu();
    l.set_entry_type(tar::EntryType::Symlink);
    l.set_size(0);
    b.append_link(&mut l, "data/sub/link", "../small.txt")
        .unwrap();
    let mut hl = tar::Header::new_gnu();
    hl.set_entry_type(tar::EntryType::Link);
    hl.set_size(0);
    b.append_link(&mut hl, "data/hard", "data/small.txt")
        .unwrap();
    (b.into_inner().unwrap(), files)
}

fn run_extract(archive: &[u8], dest: &Path, chunk: usize) -> Result<ConsumerReport> {
    let mut opts = ExtractOptions::new(dest);
    opts.threads = 4;
    let mut ex = Box::new(Extractor::new(opts)?);
    for c in archive.chunks(chunk) {
        ex.consume(c)?;
    }
    ex.finish()
}

fn check_tree(dest: &Path, files: &[(String, Vec<u8>)]) {
    for (p, d) in files {
        assert_eq!(&std::fs::read(dest.join(p)).unwrap(), d, "{p}");
    }
    assert_eq!(
        std::fs::read(dest.join("data/sub/link")).unwrap(),
        b"hello world\n"
    );
    assert_eq!(
        std::fs::read(dest.join("data/hard")).unwrap(),
        b"hello world\n"
    );
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(dest.join("data/small.txt"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o640);
    assert!(!staging_path(dest).exists());
}

fn lz4(input: &[u8], independent: bool, block_ck: bool, content_ck: bool) -> Vec<u8> {
    let mut info = lz4_flex::frame::FrameInfo::new();
    info.block_mode = if independent {
        lz4_flex::frame::BlockMode::Independent
    } else {
        lz4_flex::frame::BlockMode::Linked
    };
    info.block_checksums = block_ck;
    info.content_checksum = content_ck;
    info.block_size = lz4_flex::frame::BlockSize::Max64KB;
    let mut enc = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
    enc.write_all(input).unwrap();
    enc.finish().unwrap()
}

#[test]
fn detects_formats() {
    let (tar, _) = sample_tar();
    assert_eq!(detect(&tar), Some(Format::Tar));
    assert_eq!(detect(&lz4(&tar, true, false, false)), Some(Format::TarLz4));
    assert_eq!(
        detect(&zstd::encode_all(&tar[..], 1).unwrap()),
        Some(Format::TarZstd)
    );
    assert_eq!(detect(b"PK\x03\x04"), None);
}

#[test]
fn extracts_plain_tar() {
    let dir = tempfile::tempdir().unwrap();
    let (tar, files) = sample_tar();
    let rep = run_extract(&tar, &dir.path().join("out"), 7_777).unwrap();
    check_tree(&dir.path().join("out"), &files);
    assert!(rep.entries >= 6);
}

#[test]
fn extracts_lz4_independent_in_parallel_with_checksums() {
    let dir = tempfile::tempdir().unwrap();
    let (tar, files) = sample_tar();
    let arc = lz4(&tar, true, true, true);
    let rep = run_extract(&arc, &dir.path().join("out"), 65_536).unwrap();
    check_tree(&dir.path().join("out"), &files);
    assert!(
        rep.notes.iter().any(|n| n.contains("parallel LZ4")),
        "{:?}",
        rep.notes
    );
}

#[test]
fn extracts_lz4_linked_serially() {
    let dir = tempfile::tempdir().unwrap();
    let (tar, files) = sample_tar();
    let rep = run_extract(
        &lz4(&tar, false, false, true),
        &dir.path().join("out"),
        10_000,
    )
    .unwrap();
    check_tree(&dir.path().join("out"), &files);
    assert!(
        rep.notes.iter().any(|n| n.contains("serial")),
        "{:?}",
        rep.notes
    );
}

#[test]
fn detects_corrupt_lz4_block() {
    let dir = tempfile::tempdir().unwrap();
    let (tar, _) = sample_tar();
    let mut arc = lz4(&tar, true, true, false);
    let mid = arc.len() / 2;
    arc[mid] ^= 0xFF;
    assert!(run_extract(&arc, &dir.path().join("out"), 65_536).is_err());
}

#[test]
fn extracts_multi_frame_zstd_in_parallel() {
    let dir = tempfile::tempdir().unwrap();
    let (tar, files) = sample_tar();
    let mut arc = Vec::new();
    for piece in tar.chunks(1 << 20) {
        arc.extend(zstd::encode_all(piece, 3).unwrap());
    }
    let rep = run_extract(&arc, &dir.path().join("out"), 50_000).unwrap();
    check_tree(&dir.path().join("out"), &files);
    assert!(rep.notes.iter().any(|n| n.contains("parallel zstd")));
}

#[test]
fn extracts_single_frame_zstd() {
    let dir = tempfile::tempdir().unwrap();
    let (tar, files) = sample_tar();
    let arc = zstd::encode_all(&tar[..], 19).unwrap();
    run_extract(&arc, &dir.path().join("out"), 50_000).unwrap();
    check_tree(&dir.path().join("out"), &files);
}

#[test]
fn large_single_zstd_frame_falls_back_to_serial() {
    let dir = tempfile::tempdir().unwrap();
    // Incompressible content so the single frame exceeds the 64 MiB buffer limit.
    let mut b = tar::Builder::new(Vec::new());
    let mut r = qget_core::rng::Rng::new(9);
    let mut payload = Vec::with_capacity(70 << 20);
    while payload.len() < 70 << 20 {
        payload.extend_from_slice(&r.next_u64().to_le_bytes());
    }
    let mut h = tar::Header::new_gnu();
    h.set_size(payload.len() as u64);
    h.set_mode(0o644);
    b.append_data(&mut h, "rand.bin", &payload[..]).unwrap();
    let tar = b.into_inner().unwrap();
    let arc = zstd::encode_all(&tar[..], 1).unwrap();
    assert!(arc.len() > 64 << 20);
    run_extract(&arc, &dir.path().join("out"), 1 << 20).unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("out/rand.bin")).unwrap(),
        payload
    );
}

#[test]
fn extracts_gzip_and_xz() {
    let (tar, files) = sample_tar();
    let dir = tempfile::tempdir().unwrap();
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gz.write_all(&tar).unwrap();
    run_extract(&gz.finish().unwrap(), &dir.path().join("gz"), 30_000).unwrap();
    check_tree(&dir.path().join("gz"), &files);
    let mut xz = liblzma::write::XzEncoder::new(Vec::new(), 1);
    xz.write_all(&tar).unwrap();
    run_extract(&xz.finish().unwrap(), &dir.path().join("xz"), 30_000).unwrap();
    check_tree(&dir.path().join("xz"), &files);
}

fn cli_compress(tool: &str, args: &[&str], input: &[u8]) -> Option<Vec<u8>> {
    let mut child = Command::new(tool)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;
    let mut stdin = child.stdin.take()?;
    let input = input.to_vec();
    let w = std::thread::spawn(move || stdin.write_all(&input));
    let out = child.wait_with_output().ok()?;
    w.join().ok()?.ok()?;
    out.status.success().then_some(out.stdout)
}

#[test]
fn extracts_archives_made_by_system_tools() {
    let (tar, files) = sample_tar();
    let dir = tempfile::tempdir().unwrap();
    let cases: &[(&str, &[&str])] = &[
        ("lz4", &["-c", "-q"]),
        ("lz4", &["-c", "-q", "-BD"]),
        ("lz4", &["-c", "-q", "-B4", "--content-size"]),
        ("zstd", &["-c", "-q"]),
        ("zstd", &["-c", "-q", "--long=27", "-T4"]),
    ];
    let mut ran = 0;
    for (i, (tool, args)) in cases.iter().enumerate() {
        let Some(arc) = cli_compress(tool, args, &tar) else {
            eprintln!("skipping {tool} {args:?}: not installed");
            continue;
        };
        let dest = dir.path().join(format!("o{i}"));
        run_extract(&arc, &dest, 40_000).unwrap_or_else(|e| panic!("{tool} {args:?}: {e}"));
        check_tree(&dest, &files);
        ran += 1;
    }
    eprintln!("verified {ran} system-tool archives");
}

fn raw_tar_entry(name: &str, kind: u8, link: &str, body: &[u8]) -> Vec<u8> {
    let mut h = [0u8; 512];
    h[..name.len()].copy_from_slice(name.as_bytes());
    h[100..107].copy_from_slice(b"0000644");
    h[108..115].copy_from_slice(b"0000000");
    h[116..123].copy_from_slice(b"0000000");
    let size = format!("{:011o}", body.len());
    h[124..135].copy_from_slice(size.as_bytes());
    h[136..147].copy_from_slice(b"00000000000");
    h[156] = kind;
    h[157..157 + link.len()].copy_from_slice(link.as_bytes());
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    h[148..156].copy_from_slice(b"        ");
    let sum: u32 = h.iter().map(|&b| b as u32).sum();
    let ck = format!("{sum:06o}\0 ");
    h[148..156].copy_from_slice(ck.as_bytes());
    let mut out = h.to_vec();
    out.extend_from_slice(body);
    out.resize(out.len().div_ceil(512) * 512, 0);
    out
}

fn finish_tar(mut entries: Vec<u8>) -> Vec<u8> {
    entries.extend([0u8; 1024]);
    entries
}

#[test]
fn rejects_path_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let arc = finish_tar(raw_tar_entry("../evil", b'0', "", b"x"));
    let err = run_extract(&arc, &dir.path().join("out"), 512).unwrap_err();
    assert!(err.to_string().contains(".."), "{err}");
    assert!(!dir.path().join("evil").exists());
}

#[test]
fn rejects_absolute_paths() {
    let dir = tempfile::tempdir().unwrap();
    let arc = finish_tar(raw_tar_entry("/tmp/qget-evil", b'0', "", b"x"));
    assert!(run_extract(&arc, &dir.path().join("out"), 512).is_err());
}

#[test]
fn rejects_escaping_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let arc = finish_tar(raw_tar_entry("link", b'2', "../../etc", b""));
    assert!(run_extract(&arc, &dir.path().join("out"), 512).is_err());
}

#[test]
fn rejects_write_through_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let outside = dir.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    // "d" -> "." is a legal in-root link, but writing "d/x" through it is refused.
    let mut arc = raw_tar_entry("d", b'2', ".", b"");
    arc.extend(raw_tar_entry("d/x", b'0', "", b"payload"));
    let err = run_extract(&finish_tar(arc), &dir.path().join("out"), 512).unwrap_err();
    assert!(err.to_string().contains("through symlink"), "{err}");
}

#[test]
fn rejects_device_nodes_unless_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let arc = finish_tar(raw_tar_entry("dev", b'3', "", b""));
    assert!(run_extract(&arc, &dir.path().join("a"), 512).is_err());
    let mut opts = ExtractOptions::new(dir.path().join("b"));
    opts.allow_special = true;
    let mut ex = Box::new(Extractor::new(opts).unwrap());
    ex.consume(&arc).unwrap();
    let rep = ex.finish().unwrap();
    assert!(rep.notes.iter().any(|n| n.contains("special")));
}

#[test]
fn refuses_non_empty_destination() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("existing"), b"x").unwrap();
    assert!(matches!(
        Extractor::new(ExtractOptions::new(dir.path())),
        Err(Error::Usage(_))
    ));
}

#[test]
fn truncated_archive_fails() {
    let dir = tempfile::tempdir().unwrap();
    let (tar, _) = sample_tar();
    let arc = lz4(&tar, true, false, false);
    let cut = &arc[..arc.len() / 2];
    assert!(run_extract(cut, &dir.path().join("out"), 4096).is_err());
    assert!(!dir.path().join("out").exists());
}
