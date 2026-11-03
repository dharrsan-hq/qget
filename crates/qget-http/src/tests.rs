use super::*;
use qget_testserver::{Fault, FaultRule, ServerConfig, TestServer, test_data};

fn client() -> HttpClient {
    HttpClient::new(&ClientOptions::default()).unwrap()
}

async fn read_all(mut s: Box<dyn ChunkStream>) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(c) = s.next_chunk().await {
        out.extend_from_slice(&c.unwrap());
    }
    out
}

#[tokio::test]
async fn probe_reports_size_ranges_and_validators() {
    let srv = TestServer::start(ServerConfig::memory(test_data(10_000, 1)))
        .await
        .unwrap();
    let info = client().probe(&srv.url()).await.unwrap();
    assert_eq!(info.size, Some(10_000));
    assert!(info.ranges);
    assert_eq!(info.validator.etag.as_deref(), Some("\"v1\""));
    assert!(info.validator.last_modified.is_some());
}

#[tokio::test]
async fn probe_works_when_head_is_rejected() {
    let mut cfg = ServerConfig::memory(test_data(5_000, 2));
    cfg.reject_head = true;
    let srv = TestServer::start(cfg).await.unwrap();
    let info = client().probe(&srv.url()).await.unwrap();
    assert_eq!(info.size, Some(5_000));
    assert!(info.ranges);
}

#[tokio::test]
async fn probe_detects_missing_range_support() {
    let mut cfg = ServerConfig::memory(test_data(5_000, 3));
    cfg.support_ranges = false;
    let srv = TestServer::start(cfg).await.unwrap();
    let info = client().probe(&srv.url()).await.unwrap();
    assert_eq!(info.size, Some(5_000));
    assert!(!info.ranges);
}

#[tokio::test]
async fn probe_follows_redirects_and_reads_digest() {
    let mut cfg = ServerConfig::memory(test_data(5_000, 4));
    cfg.redirect = true;
    cfg.repr_digest = true;
    let srv = TestServer::start(cfg).await.unwrap();
    let info = client()
        .probe(&format!("{}/redirect/file.bin", srv.base()))
        .await
        .unwrap();
    assert!(info.url.ends_with("/file.bin") && !info.url.contains("redirect"));
    assert_eq!(info.digest.unwrap().hex, srv.sha256_hex().unwrap());
}

#[tokio::test]
async fn probe_404_is_http_status() {
    let srv = TestServer::start(ServerConfig::memory(vec![1, 2, 3]))
        .await
        .unwrap();
    let err = client()
        .probe(&format!("{}/missing", srv.base()))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::HttpStatus { status: 404, .. }));
}

#[tokio::test]
async fn range_fetch_returns_exact_bytes() {
    let data = test_data(100_000, 5);
    let srv = TestServer::start(ServerConfig::memory(data.clone()))
        .await
        .unwrap();
    let c = client();
    let info = c.probe(&srv.url()).await.unwrap();
    let src = HttpSource::new(c, &info);
    let body = read_all(src.open(ByteRange::new(1000, 50_000)).await.unwrap()).await;
    assert_eq!(body, &data[1000..50_000]);
}

#[tokio::test]
async fn wrong_content_range_is_transient() {
    let mut cfg = ServerConfig::memory(test_data(10_000, 6));
    cfg.faults = vec![FaultRule::once(2, Fault::WrongContentRange)];
    let srv = TestServer::start(cfg).await.unwrap();
    let c = client();
    let info = c.probe(&srv.url()).await.unwrap(); // GET #1
    let src = HttpSource::new(c, &info);
    let err = src.open(ByteRange::new(0, 100)).await.err().unwrap();
    assert!(matches!(err, FailKind::Transient(_)), "{err:?}");
}

#[tokio::test]
async fn ignored_range_is_not_stitched() {
    let mut cfg = ServerConfig::memory(test_data(10_000, 7));
    cfg.faults = vec![FaultRule::once(2, Fault::IgnoreRange)];
    let srv = TestServer::start(cfg).await.unwrap();
    let c = client();
    let info = c.probe(&srv.url()).await.unwrap();
    let src = HttpSource::new(c, &info);
    let err = src.open(ByteRange::new(100, 200)).await.err().unwrap();
    assert!(matches!(err, FailKind::Transient(_)), "{err:?}");
}

#[tokio::test]
async fn changed_content_is_fatal_remote_changed() {
    let mut cfg = ServerConfig::memory(test_data(10_000, 8));
    cfg.faults = vec![FaultRule::once(2, Fault::ChangeContent)];
    let srv = TestServer::start(cfg).await.unwrap();
    let c = client();
    let info = c.probe(&srv.url()).await.unwrap();
    let src = HttpSource::new(c, &info);
    let err = src.open(ByteRange::new(0, 100)).await.err().unwrap();
    assert!(
        matches!(
            err,
            FailKind::Fatal {
                kind: FatalKind::RemoteChanged,
                ..
            }
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn throttling_carries_retry_after() {
    let mut cfg = ServerConfig::memory(test_data(10_000, 9));
    cfg.faults = vec![FaultRule::once(2, Fault::Status(429, Some(3)))];
    let srv = TestServer::start(cfg).await.unwrap();
    let c = client();
    let info = c.probe(&srv.url()).await.unwrap();
    let src = HttpSource::new(c, &info);
    let err = src.open(ByteRange::new(0, 100)).await.err().unwrap();
    assert_eq!(
        err,
        FailKind::Throttled {
            retry_after_ms: Some(3000)
        }
    );
}

#[tokio::test]
async fn not_found_is_fatal_and_5xx_transient() {
    let mut cfg = ServerConfig::memory(test_data(10_000, 10));
    cfg.faults = vec![
        FaultRule::once(2, Fault::Status(404, None)),
        FaultRule::once(3, Fault::Status(502, None)),
    ];
    let srv = TestServer::start(cfg).await.unwrap();
    let c = client();
    let info = c.probe(&srv.url()).await.unwrap();
    let src = HttpSource::new(c, &info);
    let e1 = src.open(ByteRange::new(0, 100)).await.err().unwrap();
    assert!(matches!(
        e1,
        FailKind::Fatal {
            kind: FatalKind::HttpStatus(404),
            ..
        }
    ));
    let e2 = src.open(ByteRange::new(0, 100)).await.err().unwrap();
    assert!(matches!(e2, FailKind::Transient(_)));
}

#[tokio::test]
async fn expired_url_is_refreshed() {
    let mut cfg = ServerConfig::memory(test_data(10_000, 11));
    cfg.faults = vec![FaultRule::once(2, Fault::Status(403, None))];
    let srv = TestServer::start(cfg).await.unwrap();
    let c = client();
    let info = c.probe(&srv.url()).await.unwrap();
    let fresh = srv.url();
    let src = HttpSource::new(c, &info).with_refresher(Arc::new(move || {
        let u = fresh.clone();
        Box::pin(async move { Ok(u) })
    }));
    let e = src.open(ByteRange::new(0, 100)).await.err().unwrap();
    assert!(matches!(e, FailKind::Transient(ref m) if m.contains("refreshed")));
    assert!(src.open(ByteRange::new(0, 100)).await.is_ok());
}

#[test]
fn parses_content_disposition() {
    let mut h = HeaderMap::new();
    h.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"../../etc/passwd\""),
    );
    assert_eq!(filename_from(&h).as_deref(), Some("passwd"));
    h.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename*=UTF-8''snap%20shot.tar.lz4"),
    );
    assert_eq!(filename_from(&h).as_deref(), Some("snap shot.tar.lz4"));
    h.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"..\""),
    );
    assert_eq!(filename_from(&h), None);
}

#[test]
fn weak_etags_are_ignored() {
    let mut h = HeaderMap::new();
    h.insert(header::ETAG, HeaderValue::from_static("W/\"abc\""));
    assert_eq!(validator_from(&h).etag, None);
    h.insert(header::ETAG, HeaderValue::from_static("\"abc\""));
    assert_eq!(validator_from(&h).etag.as_deref(), Some("\"abc\""));
}

#[test]
fn parses_repr_digest() {
    let mut h = HeaderMap::new();
    // sha-256 of "hello"
    h.insert(
        "repr-digest",
        HeaderValue::from_static("sha-256=:LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=:"),
    );
    assert_eq!(
        digest_from(&h).unwrap().hex,
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
}
