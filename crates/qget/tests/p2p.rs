//! Peer-to-peer end-to-end tests over real iroh QUIC on loopback.
#![cfg(feature = "p2p")]

use qget::p2p::{self, P2pNet, Receive};
use qget::{Error, Output};
use qget_testserver::{ServerConfig, TestServer, test_data};
use std::sync::atomic::Ordering;
use std::time::Duration;

const MIB: usize = 1 << 20;

fn net() -> P2pNet {
    P2pNet::local()
}

#[tokio::test(flavor = "multi_thread")]
async fn send_and_receive_by_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("payload.bin");
    let data = test_data(24 * MIB + 77, 1);
    std::fs::write(&src, &data).unwrap();
    let sending = p2p::send(&src, &net(), None).await.unwrap();
    assert_eq!(
        sending.ticket.root,
        blake3::hash(&data).to_hex().to_string()
    );

    let ticket = qget::p2p::P2pTicket::decode(&sending.ticket.encode()).unwrap();
    let out = dir.path().join("got.bin");
    let rep = Receive::new(ticket)
        .net(net())
        .output(Output::File(out.clone()))
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(rep.verified);
    assert_eq!(
        rep.hash.unwrap().hex,
        blake3::hash(&data).to_hex().to_string()
    );
    assert!(sending.served() >= data.len() as u64);
    sending.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn short_code_via_mailbox() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mbox = listener.local_addr().unwrap().to_string();
    tokio::spawn(qget_p2p::mailbox::serve(listener));

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("hello.txt");
    std::fs::write(&src, b"hello over a short code\n").unwrap();
    let mut sending = p2p::send(&src, &net(), Some(&mbox)).await.unwrap();
    let code = sending.code.clone().unwrap().to_string();
    assert_eq!(code.split('-').count(), 4, "{code}");

    let ticket = p2p::resolve_code(&mbox, &code).await.unwrap();
    sending.code_claimed().await.unwrap();
    assert_eq!(ticket, sending.ticket);
    let out = dir.path().join("got.txt");
    Receive::new(ticket)
        .net(net())
        .output(Output::File(out.clone()))
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), b"hello over a short code\n");
    // A code is single-use.
    assert!(p2p::resolve_code(&mbox, &code).await.is_err());
    sending.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_code_is_rejected() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mbox = listener.local_addr().unwrap().to_string();
    tokio::spawn(qget_p2p::mailbox::serve(listener));
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("f");
    std::fs::write(&src, b"secret").unwrap();
    let mut sending = p2p::send(&src, &net(), Some(&mbox)).await.unwrap();
    let real = sending.code.clone().unwrap();
    let mut wrong = real.clone();
    wrong.words[0] = if real.words[0] == "acid" {
        "acorn".into()
    } else {
        "acid".into()
    };
    let err = p2p::resolve_code(&mbox, &wrong.to_string())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::P2p(_)), "{err}");
    assert!(
        sending.code_claimed().await.is_err(),
        "sender must see the failed exchange"
    );
    sending.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tampered_provider_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("f.bin");
    let data = test_data(6 * MIB, 2);
    std::fs::write(&src, &data).unwrap();
    let sending = p2p::send(&src, &net(), None).await.unwrap();
    // The file changes on disk after it was hashed: the peer now serves bad bytes.
    let mut bad = data.clone();
    bad[3 * MIB + 5] ^= 0xFF;
    std::fs::write(&src, &bad).unwrap();
    let out = dir.path().join("got.bin");
    let err = Receive::new(sending.ticket.clone())
        .net(net())
        .output(Output::File(out.clone()))
        .run()
        .await
        .unwrap_err();
    assert_eq!(err.exit_code(), 5, "{err}");
    assert!(!out.exists());
    sending.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn directory_share_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let share = dir.path().join("project");
    std::fs::create_dir_all(share.join("sub")).unwrap();
    std::fs::write(share.join("a.txt"), b"alpha").unwrap();
    std::fs::write(share.join("sub/b.bin"), test_data(2 * MIB, 3)).unwrap();
    let sending = p2p::send(&share, &net(), None).await.unwrap();
    assert!(sending.ticket.dir);
    let dest = dir.path().join("recv");
    std::fs::create_dir(&dest).unwrap();
    let rep = Receive::new(sending.ticket.clone())
        .net(net())
        .output(Output::Dir(dest.clone()))
        .run()
        .await
        .unwrap();
    let root = rep.output.unwrap();
    assert_eq!(std::fs::read(root.join("project/a.txt")).unwrap(), b"alpha");
    assert_eq!(
        std::fs::read(root.join("project/sub/b.bin")).unwrap(),
        test_data(2 * MIB, 3)
    );
    sending.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn hybrid_http_mirror_and_peer() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("f.bin");
    let data = test_data(32 * MIB, 4);
    std::fs::write(&src, &data).unwrap();
    let sending = p2p::send(&src, &net(), None).await.unwrap();
    let mut cfg = ServerConfig::memory(data.clone());
    cfg.per_conn_rate = Some(20 * MIB as u64);
    let http = TestServer::start(cfg).await.unwrap();
    let out = dir.path().join("got.bin");
    let rep = Receive::new(sending.ticket.clone())
        .net(net())
        .mirror(http.url())
        .output(Output::File(out.clone()))
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert_eq!(rep.sources.len(), 2);
    sending.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn lying_http_mirror_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("f.bin");
    let data = test_data(16 * MIB, 5);
    std::fs::write(&src, &data).unwrap();
    let sending = p2p::send(&src, &net(), None).await.unwrap();
    // Same size, different bytes.
    let http = TestServer::start(ServerConfig::memory(test_data(16 * MIB, 6)))
        .await
        .unwrap();
    let out = dir.path().join("got.bin");
    Receive::new(sending.ticket.clone())
        .net(net())
        .mirror(http.url())
        .output(Output::File(out.clone()))
        .run()
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(&out).unwrap(),
        data,
        "only verified bytes were written"
    );
    sending.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn swarm_of_two_peers() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.bin");
    let data = test_data(32 * MIB, 7);
    std::fs::write(&a, &data).unwrap();
    let b = dir.path().join("b.bin");
    std::fs::write(&b, &data).unwrap();
    let s1 = p2p::send(&a, &net(), None).await.unwrap();
    let s2 = p2p::seed(&b, "a.bin", &net()).await.unwrap();
    assert_eq!(s1.ticket.root, s2.ticket.root);
    let out = dir.path().join("got.bin");
    Receive::new(s1.ticket.clone())
        .peer(s2.ticket.clone())
        .net(net())
        .output(Output::File(out.clone()))
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(
        s1.served() > 0 && s2.served() > 0,
        "{} / {}",
        s1.served(),
        s2.served()
    );
    s1.shutdown().await;
    s2.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_resumes_after_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("f.bin");
    let data = test_data(64 * MIB, 8);
    std::fs::write(&src, &data).unwrap();
    let sending = p2p::send(&src, &net(), None).await.unwrap();
    let out = dir.path().join("got.bin");
    let token = qget::CancellationToken::new();
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (t2, s2) = (token.clone(), seen.clone());
    let res = Receive::new(sending.ticket.clone())
        .net(net())
        .output(Output::File(out.clone()))
        .max_connections(2)
        .cancel_token(token)
        .on_progress(move |p| {
            s2.store(p.downloaded, Ordering::SeqCst);
            if p.downloaded > 8 * MIB as u64 {
                t2.cancel();
            }
        })
        .run()
        .await;
    if res.is_ok() {
        eprintln!("transfer finished before cancel; resume path not exercised");
        sending.shutdown().await;
        return;
    }
    assert!(matches!(res, Err(Error::Interrupted)));
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let rep = Receive::new(sending.ticket.clone())
        .net(net())
        .output(Output::File(out.clone()))
        .run()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(rep.resumed_bytes > 0);
    sending.shutdown().await;
}
