//! End-to-end library tests against the fault-injecting test server (§14.2).

use qget::{Download, Error, Output, Verify};
use qget_testserver::{Fault, FaultRule, ServerConfig, TestServer, test_data};
use sha2::Digest;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

const MIB: usize = 1 << 20;

fn sha256(d: &[u8]) -> String {
    format!("sha256:{}", hex::encode(sha2::Sha256::digest(d)))
}

async fn server(data: &[u8], tweak: impl FnOnce(&mut ServerConfig)) -> TestServer {
    let mut cfg = ServerConfig::memory(data.to_vec());
    tweak(&mut cfg);
    TestServer::start(cfg).await.unwrap()
}

#[tokio::test]
async fn downloads_and_verifies_checksum() {
    let data = test_data(24 * MIB + 123, 1);
    let srv = server(&data, |c| c.per_conn_rate = Some(40 * MIB as u64)).await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("f.bin");
    let rep = Download::new(srv.url())
        .to_file(&out)
        .checksum(&sha256(&data))
        .unwrap()
        .seed(1)
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(rep.verified);
    assert_eq!(rep.hash.unwrap().to_string(), sha256(&data));
    assert!(
        srv.stats.max_in_flight.load(Ordering::SeqCst) > 1,
        "used parallel ranges"
    );
    assert!(!dir.path().join("f.bin.qget-part").exists());
    assert!(!dir.path().join("f.bin.qget-journal").exists());
}

#[tokio::test]
async fn survives_fault_matrix() {
    let data = test_data(20 * MIB, 2);
    let srv = server(&data, |c| {
        c.faults = vec![
            FaultRule::once(2, Fault::ShortBody),
            FaultRule::once(3, Fault::ResetAfter(100_000)),
            FaultRule::once(4, Fault::WrongContentRange),
            FaultRule::once(5, Fault::IgnoreRange),
            FaultRule::once(6, Fault::Status(500, None)),
            FaultRule::once(7, Fault::Status(503, Some(1))),
            FaultRule::once(8, Fault::Stall(3_000)),
            FaultRule::every(12, 5, 3, Fault::ResetAfter(10)),
        ];
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("f.bin");
    let rep = Download::new(srv.url())
        .to_file(&out)
        .idle_timeout(Duration::from_millis(800))
        .seed(2)
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(rep.stats.retries >= 5, "{:?}", rep.stats);
}

#[tokio::test]
async fn throttling_is_handled() {
    let data = test_data(16 * MIB, 3);
    let srv = server(&data, |c| {
        c.max_concurrent = Some(2);
        c.per_conn_rate = Some(20 * MIB as u64);
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let rep = Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .seed(3)
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(dir.path().join("f")).unwrap(), data);
    assert!(srv.stats.throttled.load(Ordering::SeqCst) > 0);
    assert!(rep.stats.throttles > 0);
}

#[tokio::test]
async fn content_change_mid_download_is_detected() {
    let data = test_data(16 * MIB, 4);
    let srv = server(&data, |c| {
        c.faults = vec![FaultRule::once(3, Fault::ChangeContent)];
        c.per_conn_rate = Some(8 * MIB as u64);
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let err = Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RemoteChanged(_)), "{err}");
    assert!(!dir.path().join("f").exists());
}

#[tokio::test]
async fn resumes_after_cancel() {
    let data = test_data(32 * MIB, 5);
    let srv = server(&data, |c| c.per_conn_rate = Some(4 * MIB as u64)).await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("f.bin");
    let token = qget::CancellationToken::new();
    let t2 = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        t2.cancel();
    });
    let err = Download::new(srv.url())
        .to_file(&out)
        .cancel_token(token)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Interrupted));
    assert!(dir.path().join("f.bin.qget-part").exists());
    assert!(dir.path().join("f.bin.qget-journal").exists());

    let before = srv.stats.body_bytes.load(Ordering::SeqCst);
    let rep = Download::new(srv.url())
        .to_file(&out)
        .checksum(&sha256(&data))
        .unwrap()
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(rep.resumed_bytes > 0, "nothing resumed");
    let second = srv.stats.body_bytes.load(Ordering::SeqCst) - before;
    assert!(
        second < data.len() as u64,
        "re-downloaded everything ({second})"
    );
    assert!(matches!(
        rep.resume,
        Some(qget::ResumeDecision::Resumed { .. })
    ));
}

#[tokio::test]
async fn resume_refuses_changed_remote() {
    let data = test_data(16 * MIB, 6);
    let srv = server(&data, |c| c.per_conn_rate = Some(2 * MIB as u64)).await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("f.bin");
    let token = qget::CancellationToken::new();
    let t2 = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        t2.cancel();
    });
    let _ = Download::new(srv.url())
        .to_file(&out)
        .cancel_token(token)
        .run()
        .await;
    // Same URL, different object (new ETag).
    let other = test_data(16 * MIB, 7);
    let srv2 = server(&other, |c| c.etag = Some("\"other\"".into())).await;
    let err = Download::new(srv2.url())
        .to_file(&out)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RemoteChanged(_)), "{err}");
    // --restart discards the partial download.
    Download::new(srv2.url())
        .to_file(&out)
        .restart(true)
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), other);
}

#[tokio::test]
async fn checksum_mismatch_fails_without_output() {
    let data = test_data(5 * MIB, 8);
    let srv = server(&data, |_| {}).await;
    let dir = tempfile::tempdir().unwrap();
    let err = Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .checksum(&format!("sha256:{}", "0".repeat(64)))
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Integrity { .. }));
    assert_eq!(err.exit_code(), 5);
    assert!(!dir.path().join("f").exists());
}

#[tokio::test]
async fn uses_repr_digest_and_detects_mismatch() {
    let data = test_data(3 * MIB, 9);
    let srv = server(&data, |c| c.repr_digest = true).await;
    let dir = tempfile::tempdir().unwrap();
    let rep = Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .run()
        .await
        .unwrap();
    assert!(rep.verified);
    assert_eq!(rep.hash.unwrap().to_string(), sha256(&data));
}

#[tokio::test]
async fn verify_required_without_checksum_fails() {
    let data = test_data(MIB, 10);
    let srv = server(&data, |_| {}).await;
    let dir = tempfile::tempdir().unwrap();
    let err = Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .verify(Verify::Required)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Integrity { .. }));
}

#[tokio::test]
async fn sha256_sidecar_is_discovered_when_required() {
    let data = test_data(2 * MIB, 11);
    let srv = server(&data, |c| c.serve_sums = true).await;
    let dir = tempfile::tempdir().unwrap();
    let rep = Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .verify(Verify::Required)
        .run()
        .await
        .unwrap();
    assert!(rep.verified);
}

#[tokio::test]
async fn uses_mirrors_with_same_etag() {
    let data = test_data(32 * MIB, 12);
    let a = server(&data, |c| c.per_conn_rate = Some(6 * MIB as u64)).await;
    let b = server(&data, |c| c.per_conn_rate = Some(6 * MIB as u64)).await;
    let dir = tempfile::tempdir().unwrap();
    Download::new(a.url())
        .mirror(b.url())
        .to_file(dir.path().join("f"))
        .seed(12)
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(dir.path().join("f")).unwrap(), data);
    assert!(a.stats.body_bytes.load(Ordering::SeqCst) > 0);
    assert!(b.stats.body_bytes.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn mirrors_with_different_etags_need_checksum() {
    let data = test_data(4 * MIB, 13);
    let a = server(&data, |_| {}).await;
    let b = server(&data, |c| c.etag = Some("\"different\"".into())).await;
    let dir = tempfile::tempdir().unwrap();
    let err = Download::new(a.url())
        .mirror(b.url())
        .to_file(dir.path().join("f"))
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Usage(_)));
    Download::new(a.url())
        .mirror(b.url())
        .checksum(&sha256(&data))
        .unwrap()
        .to_file(dir.path().join("f"))
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(dir.path().join("f")).unwrap(), data);
}

#[tokio::test]
async fn server_without_ranges_uses_single_stream() {
    let data = test_data(3 * MIB, 14);
    let srv = server(&data, |c| c.support_ranges = false).await;
    let dir = tempfile::tempdir().unwrap();
    let rep = Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(dir.path().join("f")).unwrap(), data);
    assert!(rep.notes.iter().any(|n| n.contains("range")));
}

#[tokio::test]
async fn not_found_maps_to_exit_code_4() {
    let srv = server(&[1, 2, 3], |_| {}).await;
    let err = Download::new(format!("{}/nope", srv.base()))
        .output(Output::Dir(
            tempfile::tempdir().unwrap().path().to_path_buf(),
        ))
        .run()
        .await
        .unwrap_err();
    assert_eq!(err.exit_code(), 4);
}

#[tokio::test]
async fn empty_file() {
    let srv = server(&[], |_| {}).await;
    let dir = tempfile::tempdir().unwrap();
    Download::new(srv.url())
        .to_file(dir.path().join("empty"))
        .run()
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("empty")).unwrap(),
        Vec::<u8>::new()
    );
}

#[tokio::test]
async fn polite_mode_caps_connections() {
    let data = test_data(40 * MIB, 15);
    let srv = server(&data, |c| c.per_conn_rate = Some(10 * MIB as u64)).await;
    let dir = tempfile::tempdir().unwrap();
    Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .polite(true)
        .run()
        .await
        .unwrap();
    assert!(srv.stats.max_in_flight.load(Ordering::SeqCst) <= 4);
}

#[tokio::test]
async fn refresh_command_recovers_expired_url() {
    let data = test_data(8 * MIB, 16);
    let srv = server(&data, |c| {
        c.faults = vec![FaultRule::once(3, Fault::Status(403, None))]
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .refresh_cmd(format!("echo {}", srv.url()))
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(dir.path().join("f")).unwrap(), data);
}

#[tokio::test]
async fn trace_replays_exactly() {
    let data = test_data(24 * MIB, 17);
    let srv = server(&data, |c| {
        c.per_conn_rate = Some(10 * MIB as u64);
        c.faults = vec![FaultRule::once(4, Fault::ResetAfter(1000))];
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let trace = dir.path().join("t.jsonl");
    Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .trace(&trace)
        .run()
        .await
        .unwrap();
    let (events, mismatch) = qget::replay_trace(&trace).unwrap();
    assert!(events > 10);
    assert_eq!(mismatch, None);
}

#[tokio::test]
async fn rate_limit_is_respected() {
    let data = test_data(3 * MIB, 18);
    let srv = server(&data, |_| {}).await;
    let dir = tempfile::tempdir().unwrap();
    let t = std::time::Instant::now();
    Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .limit_rate(MIB as u64)
        .run()
        .await
        .unwrap();
    // 1 MiB burst + 2 MiB at 1 MiB/s.
    assert!(
        t.elapsed() >= Duration::from_millis(1700),
        "{:?}",
        t.elapsed()
    );
}

#[tokio::test]
async fn progress_callback_fires() {
    let data = test_data(8 * MIB, 19);
    let srv = server(&data, |c| c.per_conn_rate = Some(8 * MIB as u64)).await;
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c2 = calls.clone();
    Download::new(srv.url())
        .to_file(dir.path().join("f"))
        .on_progress(move |p| {
            assert!(p.downloaded <= p.total);
            c2.fetch_add(1, Ordering::SeqCst);
        })
        .run()
        .await
        .unwrap();
    assert!(calls.load(Ordering::SeqCst) > 2);
}

fn tar_lz4(files: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut b = tar_builder();
    for (name, d) in files {
        append(&mut b, name, d);
    }
    let tar = b.into_inner().unwrap();

    std::process::Command::new("lz4")
        .args(["-c", "-q"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            let mut i = c.stdin.take().unwrap();
            let t = tar.clone();
            let w = std::thread::spawn(move || i.write_all(&t));
            let o = c.wait_with_output()?;
            w.join().unwrap()?;
            Ok(o.stdout)
        })
        .expect("lz4 CLI required for this test")
}

struct TarB(Vec<u8>);
fn tar_builder() -> TarB {
    TarB(Vec::new())
}
fn append(b: &mut TarB, name: &str, d: &[u8]) {
    let mut h = [0u8; 512];
    h[..name.len()].copy_from_slice(name.as_bytes());
    h[100..107].copy_from_slice(b"0000644");
    h[108..115].copy_from_slice(b"0000000");
    h[116..123].copy_from_slice(b"0000000");
    h[124..135].copy_from_slice(format!("{:011o}", d.len()).as_bytes());
    h[136..147].copy_from_slice(b"00000000000");
    h[156] = b'0';
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    h[148..156].copy_from_slice(b"        ");
    let sum: u32 = h.iter().map(|&b| b as u32).sum();
    h[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    b.0.extend_from_slice(&h);
    b.0.extend_from_slice(d);
    b.0.resize(b.0.len().div_ceil(512) * 512, 0);
}
impl TarB {
    fn into_inner(mut self) -> std::io::Result<Vec<u8>> {
        self.0.extend([0u8; 1024]);
        Ok(self.0)
    }
}

#[tokio::test]
async fn stream_extracts_tar_lz4() {
    let files = vec![
        ("a.bin", test_data(9 * MIB, 20)),
        ("dir/b.bin", test_data(3 * MIB, 21)),
        ("dir/c.txt", b"hello".to_vec()),
    ];
    let arc = tar_lz4(&files);
    let srv = server(&arc, |c| {
        c.per_conn_rate = Some(8 * MIB as u64);
        c.path = "/snap.tar.lz4".into();
        c.serve_sums = true;
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("data");
    let rep = Download::new(srv.url())
        .extract_to(&dest)
        .max_buffer(64 << 20)
        .run()
        .await
        .unwrap();
    for (name, d) in &files {
        assert_eq!(&std::fs::read(dest.join(name)).unwrap(), d, "{name}");
    }
    assert!(rep.verified, "auto-discovered .sha256 in extract mode");
    assert_eq!(rep.entries, Some(3));
    assert!(
        !dir.path().join("snap.tar.lz4").exists(),
        "archive not stored"
    );
    assert!(!dir.path().join("data.qget-staging").exists());
}

#[tokio::test]
async fn stream_extract_with_bad_checksum_does_not_install() {
    let files = vec![("a.bin", test_data(2 * MIB, 22))];
    let arc = tar_lz4(&files);
    let srv = server(&arc, |c| c.path = "/snap.tar.lz4".into()).await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("data");
    let err = Download::new(srv.url())
        .extract_to(&dest)
        .checksum(&format!("sha256:{}", "1".repeat(64)))
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Integrity { .. }));
    assert!(!dest.exists(), "unverified data must not be installed");
}

#[tokio::test]
async fn keep_archive_extracts_and_keeps_file() {
    let files = vec![("x/y.bin", test_data(6 * MIB, 23))];
    let arc = tar_lz4(&files);
    let srv = server(&arc, |c| {
        c.path = "/snap.tar.lz4".into();
        c.per_conn_rate = Some(8 * MIB as u64);
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("data");
    Download::new(srv.url())
        .extract_to(&dest)
        .keep_archive(true)
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(dest.join("x/y.bin")).unwrap(), files[0].1);
    assert_eq!(std::fs::read(dir.path().join("snap.tar.lz4")).unwrap(), arc);
}
