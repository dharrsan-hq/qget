//! Black-box tests of the `qget` binary, including kill -9 crash/resume (DUR-06).

use qget_testserver::{ServerConfig, TestServer, test_data};
use sha2::Digest;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

const MIB: usize = 1 << 20;

fn qget() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_qget"));
    c.env("XDG_CONFIG_HOME", "/nonexistent-qget-test");
    c
}

async fn srv(data: &[u8], f: impl FnOnce(&mut ServerConfig)) -> TestServer {
    let mut cfg = ServerConfig::memory(data.to_vec());
    f(&mut cfg);
    TestServer::start(cfg).await.unwrap()
}

#[tokio::test]
async fn downloads_to_file() {
    let data = test_data(6 * MIB, 1);
    let s = srv(&data, |_| {}).await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("o.bin");
    let st = qget()
        .args(["-q", "-O"])
        .arg(&out)
        .arg(s.url())
        .status()
        .await
        .unwrap();
    assert!(st.success());
    assert_eq!(std::fs::read(&out).unwrap(), data);
}

#[tokio::test]
async fn default_name_and_directory_prefix() {
    let data = test_data(MIB, 2);
    let s = srv(&data, |c| c.path = "/dir/name%20x.bin".into()).await;
    let dir = tempfile::tempdir().unwrap();
    let st = qget()
        .args(["-q", "-P"])
        .arg(dir.path())
        .arg(format!("{}/dir/name%20x.bin", s.base()))
        .status()
        .await
        .unwrap();
    assert!(st.success());
    assert_eq!(std::fs::read(dir.path().join("name x.bin")).unwrap(), data);
}

#[tokio::test]
async fn streams_to_stdout() {
    let data = test_data(12 * MIB, 3);
    let s = srv(&data, |c| c.per_conn_rate = Some(16 * MIB as u64)).await;
    let out = qget()
        .args(["-q", "-O", "-"])
        .arg(s.url())
        .output()
        .await
        .unwrap();
    assert!(out.status.success());
    assert_eq!(out.stdout, data);
}

#[tokio::test]
async fn exit_codes() {
    let data = test_data(MIB, 4);
    let s = srv(&data, |_| {}).await;
    let dir = tempfile::tempdir().unwrap();
    let st = qget()
        .args(["-q", "-P"])
        .arg(dir.path())
        .arg(format!("{}/missing", s.base()))
        .status()
        .await
        .unwrap();
    assert_eq!(st.code(), Some(4));
    let st = qget()
        .args([
            "-q",
            "--checksum",
            &format!("sha256:{}", "0".repeat(64)),
            "-O",
        ])
        .arg(dir.path().join("x"))
        .arg(s.url())
        .status()
        .await
        .unwrap();
    assert_eq!(st.code(), Some(5));
    let st = qget()
        .args(["--checksum", "nope", "http://x/y"])
        .status()
        .await
        .unwrap();
    assert_eq!(st.code(), Some(2));
    let st = qget()
        .args(["-q", "-t", "2", "-T", "1", "-O"])
        .arg(dir.path().join("y"))
        .arg("http://127.0.0.1:1/unreachable")
        .status()
        .await
        .unwrap();
    assert_eq!(st.code(), Some(3));
}

#[tokio::test]
async fn json_progress_reports_done() {
    let data = test_data(4 * MIB, 5);
    let s = srv(&data, |c| c.per_conn_rate = Some(8 * MIB as u64)).await;
    let dir = tempfile::tempdir().unwrap();
    let out = qget()
        .args(["--progress", "json", "-O"])
        .arg(dir.path().join("f"))
        .arg(s.url())
        .output()
        .await
        .unwrap();
    assert!(out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    let done = err
        .lines()
        .find(|l| l.contains("\"done\""))
        .expect("done event");
    let v: serde_json::Value = serde_json::from_str(done).unwrap();
    assert_eq!(v["size"], 4 * MIB);
    assert!(err.lines().any(|l| l.contains("\"progress\"")));
}

#[tokio::test]
async fn explain_and_trace_replay() {
    let data = test_data(10 * MIB, 6);
    let s = srv(&data, |c| c.per_conn_rate = Some(8 * MIB as u64)).await;
    let dir = tempfile::tempdir().unwrap();
    let trace = dir.path().join("t.jsonl");
    let out = qget()
        .args(["--explain", "--seed", "42", "--trace"])
        .arg(&trace)
        .arg("-O")
        .arg(dir.path().join("f"))
        .arg(s.url())
        .output()
        .await
        .unwrap();
    assert!(out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("bottleneck:"), "{err}");
    assert!(err.contains("seed:            42"));
    let rep = qget().arg("replay").arg(&trace).output().await.unwrap();
    assert!(
        rep.status.success(),
        "{}",
        String::from_utf8_lossy(&rep.stderr)
    );
    assert!(String::from_utf8_lossy(&rep.stdout).contains("every decision reproduced"));
}

#[tokio::test]
async fn hash_completions_and_man() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("f");
    std::fs::write(&f, b"abc").unwrap();
    let out = qget()
        .args(["hash", "--algo", "sha256"])
        .arg(&f)
        .output()
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    let out = qget().args(["completions", "zsh"]).output().await.unwrap();
    assert!(String::from_utf8_lossy(&out.stdout).contains("#compdef qget"));
    let out = qget().arg("man").output().await.unwrap();
    let man = String::from_utf8_lossy(&out.stdout);
    assert!(man.contains(".TH qget") && man.contains("EXIT STATUS"));
}

#[tokio::test]
async fn extracts_via_cli() {
    let mut b = tar::Builder::new(Vec::new());
    let payload = test_data(3 * MIB, 7);
    let mut h = tar::Header::new_gnu();
    h.set_size(payload.len() as u64);
    h.set_mode(0o644);
    b.append_data(&mut h, "x/data.bin", &payload[..]).unwrap();
    let tar = b.into_inner().unwrap();
    let arc = zstd::encode_all(&tar[..], 3).unwrap();
    let s = srv(&arc, |c| c.path = "/s.tar.zst".into()).await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out");
    let st = qget()
        .args(["-q", "-x"])
        .arg(&dest)
        .arg(s.url())
        .status()
        .await
        .unwrap();
    assert!(st.success());
    assert_eq!(std::fs::read(dest.join("x/data.bin")).unwrap(), payload);
}

/// Kill qget with SIGKILL at random moments, resume each time, and require a
/// byte-exact result (DUR-06). `QGET_CRASH_ROUNDS` raises the number of kills.
#[tokio::test(flavor = "multi_thread")]
async fn sigkill_crash_resume_is_byte_exact() {
    let data = test_data(48 * MIB, 8);
    let expected = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&data)));
    let s = srv(&data, |c| c.per_conn_rate = Some(3 * MIB as u64)).await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("crash.bin");
    let rounds: u64 = std::env::var("QGET_CRASH_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);
    let mut rng = qget_core_rng(99);
    let mut kills = 0;
    for _ in 0..rounds {
        let mut child = qget()
            .args(["-q", "-O"])
            .arg(&out)
            .arg(s.url())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let wait = 150 + rng() % 700;
        tokio::time::sleep(Duration::from_millis(wait)).await;
        if child.try_wait().unwrap().is_some() {
            break;
        }
        child.start_kill().unwrap(); // SIGKILL
        child.wait().await.unwrap();
        kills += 1;
    }
    let st = qget()
        .args(["-q", "--checksum", &expected, "-O"])
        .arg(&out)
        .arg(s.url())
        .status()
        .await
        .unwrap();
    assert!(st.success(), "final run failed after {kills} kills");
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(!dir.path().join("crash.bin.qget-journal").exists());
    assert!(kills >= 2, "test did not kill enough times ({kills})");
    let served = s.stats.body_bytes.load(std::sync::atomic::Ordering::SeqCst);
    eprintln!(
        "{kills} kills; served {} MiB for a {} MiB file",
        served >> 20,
        data.len() >> 20
    );
}

fn qget_core_rng(seed: u64) -> impl FnMut() -> u64 {
    let mut x = seed | 1;
    move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    }
}

/// Three real processes: mailbox, sender, receiver using the short code.
#[cfg(feature = "p2p")]
#[tokio::test(flavor = "multi_thread")]
async fn yeet_and_yoink_with_short_code() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let mbox = format!("127.0.0.1:{port}");
    let mut mailbox = qget()
        .args(["mailbox", "--listen", &mbox])
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("hello.bin");
    let data = test_data(5 * MIB, 9);
    std::fs::write(&src, &data).unwrap();
    let mut sender = qget()
        .args([
            "yeet",
            "--no-relay",
            "--bind",
            "127.0.0.1:0",
            "--once",
            "--mailbox",
            &mbox,
        ])
        .arg(&src)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(sender.stderr.take().unwrap()).lines();
    let mut code = None;
    while let Ok(Some(l)) = tokio::time::timeout(Duration::from_secs(20), lines.next_line())
        .await
        .unwrap()
    {
        if let Some(c) = l
            .trim()
            .strip_prefix("qget yoink ")
            .filter(|c| !c.starts_with("qget1"))
        {
            code = Some(c.to_string());
            break;
        }
    }
    let code = code.expect("sender printed a code");
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    let out = dir.path().join("got.bin");
    let st = qget()
        .args(["yoink", "-q", "--no-relay", "--mailbox", &mbox, "-O"])
        .arg(&out)
        .arg(&code)
        .status()
        .await
        .unwrap();
    assert!(st.success());
    assert_eq!(std::fs::read(&out).unwrap(), data);
    let st = tokio::time::timeout(Duration::from_secs(20), sender.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(st.success(), "--once sender exits after serving");
    mailbox.start_kill().unwrap();
}
