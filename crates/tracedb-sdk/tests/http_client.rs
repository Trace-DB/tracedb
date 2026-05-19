use serde_json::json;
use std::fs;
use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};
use tracedb_query::{
    FreshnessMode, HybridQuery, HybridQueryRow, RecordDeleteRequest, RecordGetRequest, RecordInput,
    RecordPutBatchRequest, RecordScanRequest, TableSchema, VectorColumnSchema,
};
use tracedb_sdk::{
    BranchesResponse, DatabasesResponse, ErrorResponse, HealthResponse, JobsResponse,
    MetricsResponse, ReadyResponse, RestoreRequest, SnapshotRequest, TraceDbAsyncClient,
    TraceDbClient, TraceDbClientConfig, TraceDbClientError, TraceDbRequestOptions,
};

fn schema() -> TableSchema {
    TableSchema {
        name: "docs".to_string(),
        primary_id_column: "id".to_string(),
        tenant_id_column: "tenant".to_string(),
        scalar_columns: vec!["status".to_string()],
        text_indexed_columns: vec!["body".to_string()],
        vector_columns: vec![VectorColumnSchema {
            name: "embedding".to_string(),
            dimensions: 3,
            source_columns: vec!["body".to_string()],
        }],
    }
}

fn record(id: &str, tenant: &str, body: &str, embedding: [f32; 3]) -> RecordInput {
    RecordInput {
        table: "docs".to_string(),
        id: id.to_string(),
        tenant_id: tenant.to_string(),
        fields: json!({
            "id": id,
            "tenant": tenant,
            "status": "published",
            "body": body,
            "embedding": embedding,
        })
        .as_object()
        .unwrap()
        .clone(),
    }
}

fn query(explain: bool) -> HybridQuery {
    HybridQuery {
        table: "docs".to_string(),
        tenant_id: "tenant-a".to_string(),
        text: Some("rust api".to_string()),
        vector: Some(vec![1.0, 0.0, 0.0]),
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 5,
        freshness: FreshnessMode::Strict,
        explain,
    }
}

fn capture_json_body_server() -> (String, std::thread::JoinHandle<serde_json::Value>) {
    capture_json_body_response_server(r#"{"ok":true}"#)
}

fn capture_json_body_response_server(
    response_body: &'static str,
) -> (String, std::thread::JoinHandle<serde_json::Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => request.extend_from_slice(&buffer[..read]),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    break
                }
                Err(error) => panic!("read request: {error}"),
            }
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream.write_all(response.as_bytes()).unwrap();
        let request_text = String::from_utf8(request).expect("utf8 request");
        let (_, body) = request_text
            .split_once("\r\n\r\n")
            .expect("request header boundary");
        serde_json::from_str(body).expect("json request body")
    });
    (format!("http://{addr}"), handle)
}

fn capture_http_request_server() -> (String, std::thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => request.extend_from_slice(&buffer[..read]),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    break
                }
                Err(error) => panic!("read request: {error}"),
            }
        }
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
            )
            .unwrap();
        String::from_utf8(request).expect("utf8 request")
    });
    (format!("http://{addr}"), handle)
}

fn http_response_server(response: &'static [u8]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        let mut buffer = [0; 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    break
                }
                Err(error) => panic!("read request: {error}"),
            }
        }
        stream.write_all(response).unwrap();
    });
    format!("http://{addr}")
}

fn sequence_response_server(responses: Vec<&'static [u8]>) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen_attempts = Arc::clone(&attempts);
    std::thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            seen_attempts.fetch_add(1, Ordering::SeqCst);
            stream
                .set_read_timeout(Some(Duration::from_millis(250)))
                .unwrap();
            let mut buffer = [0; 1024];
            loop {
                match stream.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) =>
                    {
                        break
                    }
                    Err(error) => panic!("read request: {error}"),
                }
            }
            stream.write_all(response).unwrap();
        }
    });
    (format!("http://{addr}"), attempts)
}

fn stalled_response_server(stall_for: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = [0; 1024];
        let _ = stream.read(&mut buffer);
        std::thread::sleep(stall_for);
    });
    format!("http://{addr}")
}

fn stalled_then_response_server(
    stall_for: Duration,
    response: &'static [u8],
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen_attempts = Arc::clone(&attempts);
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        seen_attempts.fetch_add(1, Ordering::SeqCst);
        std::thread::spawn(move || {
            let _stream = stream;
            std::thread::sleep(stall_for);
        });

        let (mut stream, _) = listener.accept().unwrap();
        seen_attempts.fetch_add(1, Ordering::SeqCst);
        read_complete_http_request_for_test(&mut stream);
        stream.write_all(response).unwrap();
    });
    (format!("http://{addr}"), attempts)
}

fn read_complete_http_request_for_test(stream: &mut std::net::TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();
    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                request.extend_from_slice(&buffer[..read]);
                if http_request_is_complete(&request) {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break
            }
            Err(error) => panic!("read request: {error}"),
        }
    }
}

fn http_request_is_complete(request: &[u8]) -> bool {
    let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let head = String::from_utf8_lossy(&request[..header_end]);
    let content_length = head
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length:"))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    request.len() >= header_end + 4 + content_length
}

struct TestWake {
    notified: AtomicBool,
}

impl Wake for TestWake {
    fn wake(self: Arc<Self>) {
        self.notified.store(true, Ordering::SeqCst);
    }
}

fn test_waker() -> Waker {
    Waker::from(Arc::new(TestWake {
        notified: AtomicBool::new(false),
    }))
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> (Poll<F::Output>, Duration) {
    let waker = test_waker();
    let mut context = Context::from_waker(&waker);
    let started = Instant::now();
    let poll = future.poll(&mut context);
    (poll, started.elapsed())
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    loop {
        let (poll, _) = poll_once(future.as_mut());
        if let Poll::Ready(output) = poll {
            return output;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn async_client_decodes_typed_readiness_response() {
    let url = http_response_server(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 42\r\nConnection: close\r\n\r\n{\"ready\":true,\"service\":\"tracedb-engine\"}",
    );
    let client = TraceDbAsyncClient::new(TraceDbClientConfig::managed(url, "dev-token"));

    let response = block_on(client.ready_typed()).expect("async ready");

    assert!(response.ready);
    assert_eq!(response.service.as_deref(), Some("tracedb-engine"));
}

#[test]
fn async_client_starts_http_work_without_blocking_first_poll() {
    let url = stalled_response_server(Duration::from_millis(250));
    let client = TraceDbAsyncClient::new(
        TraceDbClientConfig::managed(url, "dev-token").with_timeout(Duration::from_millis(200)),
    );
    let mut future = Box::pin(client.ready_typed());

    let (poll, elapsed) = poll_once(future.as_mut());

    assert!(
        elapsed < Duration::from_millis(50),
        "first async poll should not block on socket I/O; elapsed {elapsed:?}"
    );
    assert!(
        poll.is_pending(),
        "first async poll should hand work to the background transport"
    );
    let error = block_on(future).expect_err("stalled response should time out");
    match error {
        TraceDbClientError::Timeout {
            method,
            path,
            timeout_ms,
        } => {
            assert_eq!(method, "GET");
            assert_eq!(path, "/v1/ready");
            assert_eq!(timeout_ms, 200);
        }
        other => panic!("unexpected async error: {other:?}"),
    }
}

#[test]
fn retryable_health_requests_retry_5xx_then_return_success() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"warming\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
    ]);
    let client =
        TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token").with_safe_retries(1));

    let response = client.health().expect("health retry");

    assert_eq!(response["ok"], true);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn write_routes_do_not_retry_5xx_without_idempotency_contract() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"warming\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
    ]);
    let client =
        TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token").with_safe_retries(1));

    let error = client
        .apply_schema(&schema())
        .expect_err("schema writes should not retry");

    match error {
        TraceDbClientError::HttpStatus { status, .. } => assert_eq!(status, 503),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn managed_client_injects_database_and_branch_ids_into_json_posts() {
    let (url, request_body) = capture_json_body_server();
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url, "dev-token")
            .with_database("db_prod")
            .with_branch("db_prod:beta"),
    );

    let response = client
        .request_json(
            "POST",
            "/v1/query",
            Some(&json!({
                "table": "docs",
                "tenant_id": "tenant-a",
            })),
        )
        .expect("post");
    let body = request_body.join().expect("request body");

    assert_eq!(response["ok"], true);
    assert_eq!(body["table"], "docs");
    assert_eq!(body["tenant_id"], "tenant-a");
    assert_eq!(body["database_id"], "db_prod");
    assert_eq!(body["branch_id"], "db_prod:beta");
}

#[test]
fn snapshot_typed_posts_target_and_decodes_response() {
    let (url, request_body) =
        capture_json_body_response_server(r#"{"snapshot":true,"target":"/tmp/tracedb-snapshot"}"#);
    let client = TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token"));

    let response = client
        .snapshot_typed(&SnapshotRequest::new("/tmp/tracedb-snapshot"))
        .expect("snapshot");
    let body = request_body.join().expect("request body");

    assert!(response.snapshot);
    assert_eq!(response.target, "/tmp/tracedb-snapshot");
    assert_eq!(body["target"], "/tmp/tracedb-snapshot");
}

#[test]
fn restore_typed_posts_source_target_and_decodes_response() {
    let (url, request_body) = capture_json_body_response_server(
        r#"{"restored":true,"source":"/tmp/tracedb-snapshot","target":"/tmp/tracedb-restore"}"#,
    );
    let client = TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token"));

    let response = client
        .restore_typed(&RestoreRequest::new(
            "/tmp/tracedb-snapshot",
            "/tmp/tracedb-restore",
        ))
        .expect("restore");
    let body = request_body.join().expect("request body");

    assert!(response.restored);
    assert_eq!(response.source, "/tmp/tracedb-snapshot");
    assert_eq!(response.target, "/tmp/tracedb-restore");
    assert_eq!(body["source"], "/tmp/tracedb-snapshot");
    assert_eq!(body["target"], "/tmp/tracedb-restore");
}

#[test]
fn request_options_send_idempotency_key_header_without_enabling_write_retries() {
    let (url, request) = capture_http_request_server();
    let client =
        TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token").with_safe_retries(2));
    let options = TraceDbRequestOptions::new().with_idempotency_key("batch-1");

    let response = client
        .request_json_with_options(
            "POST",
            "/v1/records/put-batch",
            Some(&json!({ "records": [] })),
            &options,
        )
        .expect("post with idempotency key");
    let request = request.join().expect("request");

    assert_eq!(response["ok"], true);
    assert!(
        request.contains("Idempotency-Key: batch-1\r\n"),
        "request should include Idempotency-Key header: {request}"
    );
}

#[test]
fn request_options_reject_invalid_idempotency_key_header_values() {
    let client = TraceDbClient::new(TraceDbClientConfig::managed(
        "http://127.0.0.1:1",
        "dev-token",
    ));
    let options = TraceDbRequestOptions::new().with_idempotency_key("bad\r\nx-extra: true");

    let error = client
        .request_json_with_options("POST", "/v1/records/put-batch", Some(&json!({})), &options)
        .expect_err("invalid header value should be rejected before network I/O");
    let message = error.to_string();

    match error {
        TraceDbClientError::InvalidRequest {
            method,
            path,
            message,
        } => {
            assert_eq!(method, "POST");
            assert_eq!(path, "/v1/records/put-batch");
            assert!(message.contains("idempotency key"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(message.contains("POST /v1/records/put-batch"), "{message}");
    assert!(message.contains("idempotency key"), "{message}");
}

#[test]
fn write_routes_with_idempotency_key_still_do_not_retry_5xx() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"warming\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
    ]);
    let client =
        TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token").with_safe_retries(1));
    let options = TraceDbRequestOptions::new().with_idempotency_key("schema-1");

    let error = client
        .apply_schema_with_options(&schema(), &options)
        .expect_err("schema writes should not retry automatically");

    match error {
        TraceDbClientError::HttpStatus { status, .. } => assert_eq!(status, 503),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn admin_snapshot_safe_retries_do_not_retry_even_with_idempotency_key() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"warming\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 54\r\nConnection: close\r\n\r\n{\"snapshot\":true,\"target\":\"/tmp/tracedb-snapshot\"}",
    ]);
    let client =
        TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token").with_safe_retries(1));
    let options = TraceDbRequestOptions::new().with_idempotency_key("snapshot-1");

    let error = client
        .snapshot_typed_with_options(&SnapshotRequest::new("/tmp/tracedb-snapshot"), &options)
        .expect_err("safe_retries should not retry admin requests");

    match error {
        TraceDbClientError::HttpStatus { status, .. } => assert_eq!(status, 503),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn idempotency_retries_skip_writes_without_idempotency_key() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"warming\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
    ]);
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url, "dev-token").with_idempotency_retries(1),
    );

    let error = client
        .apply_schema(&schema())
        .expect_err("idempotency retries should not apply without Idempotency-Key");

    match error {
        TraceDbClientError::HttpStatus { status, .. } => assert_eq!(status, 503),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn write_routes_with_idempotency_key_retry_5xx_when_enabled() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"warming\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
    ]);
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url, "dev-token").with_idempotency_retries(1),
    );
    let options = TraceDbRequestOptions::new().with_idempotency_key("schema-1");

    let response = client
        .apply_schema_with_options(&schema(), &options)
        .expect("schema write should retry when idempotent retries are enabled");

    assert_eq!(response["ok"], true);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn admin_snapshot_retries_5xx_with_idempotency_key_when_enabled() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"warming\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 54\r\nConnection: close\r\n\r\n{\"snapshot\":true,\"target\":\"/tmp/tracedb-snapshot\"}",
    ]);
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url, "dev-token").with_idempotency_retries(1),
    );
    let options = TraceDbRequestOptions::new().with_idempotency_key("snapshot-1");

    let response = client
        .snapshot_typed_with_options(&SnapshotRequest::new("/tmp/tracedb-snapshot"), &options)
        .expect("snapshot should retry when idempotent retries are enabled");

    assert!(response.snapshot);
    assert_eq!(response.target, "/tmp/tracedb-snapshot");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn write_routes_with_idempotency_key_retry_timeout_when_enabled() {
    let (url, attempts) = stalled_then_response_server(
        Duration::from_millis(250),
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
    );
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url, "dev-token")
            .with_timeout(Duration::from_millis(25))
            .with_idempotency_retries(1),
    );
    let options = TraceDbRequestOptions::new().with_idempotency_key("schema-timeout-1");

    let response = client
        .apply_schema_with_options(&schema(), &options)
        .expect("schema write timeout should retry when idempotency retries are enabled");

    assert_eq!(response["ok"], true);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn idempotency_retries_do_not_retry_conflicts_or_4xx() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: 24\r\nConnection: close\r\n\r\n{\"error\":\"body changed\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
    ]);
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url, "dev-token").with_idempotency_retries(1),
    );
    let options = TraceDbRequestOptions::new().with_idempotency_key("schema-conflict-1");

    let error = client
        .apply_schema_with_options(&schema(), &options)
        .expect_err("idempotency retries should not retry 409 conflicts");

    match error {
        TraceDbClientError::HttpStatus { status, .. } => assert_eq!(status, 409),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn idempotency_retries_do_not_apply_to_read_routes() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"warming\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
    ]);
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url, "dev-token").with_idempotency_retries(1),
    );
    let options = TraceDbRequestOptions::new().with_idempotency_key("query-1");

    let error = client
        .request_json_with_options("POST", "/v1/query", Some(&json!({})), &options)
        .expect_err("idempotency retries should not apply to read routes");

    match error {
        TraceDbClientError::HttpStatus { status, .. } => assert_eq!(status, 503),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn idempotency_retries_do_not_apply_to_unsupported_routes() {
    let (url, attempts) = sequence_response_server(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"error\":\"warming\"}",
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
    ]);
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url, "dev-token").with_idempotency_retries(1),
    );
    let options = TraceDbRequestOptions::new().with_idempotency_key("jobs-1");

    let error = client
        .request_json_with_options("GET", "/v1/admin/jobs", None, &options)
        .expect_err("idempotency retries should not apply to unsupported routes");

    match error {
        TraceDbClientError::HttpStatus { status, .. } => assert_eq!(status, 503),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn client_config_defaults_idempotency_retries_for_older_json() {
    let config: TraceDbClientConfig = serde_json::from_value(json!({
        "url": "http://127.0.0.1:1",
        "token": "dev-token"
    }))
    .expect("old config shape should deserialize");

    assert_eq!(config.safe_retries, 0);
    assert_eq!(config.idempotency_retries, 0);
}

#[test]
fn request_timeout_errors_include_method_path_and_timeout() {
    let url = stalled_response_server(Duration::from_millis(250));
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url, "dev-token").with_timeout(Duration::from_millis(25)),
    );

    let error = client
        .request_json("GET", "/v1/ready", None)
        .expect_err("stalled response should time out");
    let message = error.to_string();

    match error {
        TraceDbClientError::Timeout {
            method,
            path,
            timeout_ms,
        } => {
            assert_eq!(method, "GET");
            assert_eq!(path, "/v1/ready");
            assert_eq!(timeout_ms, 25);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(message.contains("GET /v1/ready"), "{message}");
    assert!(message.contains("timed out after 25 ms"), "{message}");
}

#[test]
fn http_status_errors_include_method_path_status_and_body() {
    let url = http_response_server(
        b"HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 23\r\nConnection: close\r\n\r\n{\"error\":\"not found\"}",
    );
    let client = TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token"));

    let error = client
        .request_json("POST", "/v1/missing", Some(&json!({})))
        .expect_err("missing route should fail");
    let message = error.to_string();

    assert_eq!(
        error.error_response(),
        Some(ErrorResponse {
            error: "not found".to_string()
        })
    );
    assert_eq!(error.server_error().as_deref(), Some("not found"));
    match error {
        TraceDbClientError::HttpStatus {
            method,
            path,
            status,
            body,
            ..
        } => {
            assert_eq!(method, "POST");
            assert_eq!(path, "/v1/missing");
            assert_eq!(status, 404);
            assert_eq!(body, "{\"error\":\"not found\"}");
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(message.contains("POST /v1/missing"), "{message}");
    assert!(message.contains("status 404"), "{message}");
    assert!(message.contains("{\"error\":\"not found\"}"), "{message}");
}

#[test]
fn invalid_response_errors_include_method_and_path() {
    let url = http_response_server(b"not an http response");
    let client = TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token"));

    let error = client
        .request_json("GET", "/v1/ready", None)
        .expect_err("invalid response should fail");
    let message = error.to_string();

    match error {
        TraceDbClientError::InvalidResponse {
            method,
            path,
            message,
        } => {
            assert_eq!(method, "GET");
            assert_eq!(path, "/v1/ready");
            assert_eq!(message, "missing header boundary");
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(message.contains("GET /v1/ready"), "{message}");
    assert!(message.contains("missing header boundary"), "{message}");
}

#[test]
fn invalid_json_response_errors_include_method_and_path() {
    let url = http_response_server(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 8\r\nConnection: close\r\n\r\nnot-json",
    );
    let client = TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token"));

    let error = client
        .request_json("GET", "/v1/ready", None)
        .expect_err("invalid json should fail");
    let message = error.to_string();

    match error {
        TraceDbClientError::InvalidResponse {
            method,
            path,
            message,
        } => {
            assert_eq!(method, "GET");
            assert_eq!(path, "/v1/ready");
            assert!(
                message.starts_with("invalid JSON body:"),
                "message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(message.contains("GET /v1/ready"), "{message}");
    assert!(message.contains("invalid JSON body:"), "{message}");
}

#[test]
fn typed_response_shape_errors_include_method_and_path() {
    let url = http_response_server(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\nConnection: close\r\n\r\n{\"ready\":\"yes\"}",
    );
    let client = TraceDbClient::new(TraceDbClientConfig::managed(url, "dev-token"));

    let error = client
        .ready_typed()
        .expect_err("invalid typed response should fail");
    let message = error.to_string();

    match error {
        TraceDbClientError::InvalidResponse {
            method,
            path,
            message,
        } => {
            assert_eq!(method, "GET");
            assert_eq!(path, "/v1/ready");
            assert!(
                message.starts_with("invalid JSON shape:"),
                "message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(message.contains("GET /v1/ready"), "{message}");
    assert!(message.contains("invalid JSON shape:"), "{message}");
}

#[test]
fn typed_readonly_responses_deserialize_gateway_shapes() {
    let health: HealthResponse = serde_json::from_value(json!({
        "ok": true,
        "service": "tracedb-gateway",
        "engine_url": "http://127.0.0.1:8090",
        "catalog_databases": 2,
        "metered_requests": 17,
    }))
    .expect("gateway health");
    assert!(health.ok);
    assert_eq!(health.service.as_deref(), Some("tracedb-gateway"));
    assert_eq!(health.catalog_databases, Some(2));
    assert_eq!(health.metered_requests, Some(17));

    let ready: ReadyResponse = serde_json::from_value(json!({
        "ok": true,
        "ready": true,
        "service": "tracedb-gateway",
        "engine_url": "http://127.0.0.1:8090",
        "engine_health_checked": true,
        "engine_status_code": 200,
        "catalog_databases": 2,
        "metered_requests": 18,
    }))
    .expect("gateway ready");
    assert!(ready.ready);
    assert_eq!(ready.ok, Some(true));
    assert_eq!(ready.engine_health_checked, Some(true));
    assert_eq!(ready.engine_status_code, Some(200));

    let not_ready: ReadyResponse = serde_json::from_value(json!({
        "ok": false,
        "ready": false,
        "service": "tracedb-gateway",
        "engine_url": "http://127.0.0.1:8090",
        "engine_health_checked": true,
        "error": "connection refused",
    }))
    .expect("gateway not ready");
    assert!(!not_ready.ready);
    assert_eq!(not_ready.error.as_deref(), Some("connection refused"));

    let databases: DatabasesResponse = serde_json::from_value(json!({
        "gateway": true,
        "databases": [{
            "org_id": "org-a",
            "project_id": "project-a",
            "database_id": "db-a",
            "name": "primary",
            "region": "us-west",
            "endpoint": "https://db-a.example.test",
        }],
    }))
    .expect("gateway databases");
    assert_eq!(databases.gateway, Some(true));
    assert_eq!(databases.databases[0].database_id, "db-a");
    assert_eq!(databases.databases[0].org_id.as_deref(), Some("org-a"));

    let branches: BranchesResponse = serde_json::from_value(json!({
        "gateway": true,
        "branches": [{
            "database_id": "db-a",
            "branch_id": "db-a:main",
            "parent_branch_id": null,
            "state": "Ready",
            "endpoint": "https://db-a-main.example.test",
        }],
    }))
    .expect("gateway branches");
    assert_eq!(branches.gateway, Some(true));
    assert_eq!(branches.branches[0].branch_id, "db-a:main");
    assert_eq!(branches.branches[0].parent_branch_id, None);

    let metrics: MetricsResponse = serde_json::from_value(json!({
        "gateway": true,
        "service": "tracedb-gateway",
        "requests": 21,
        "rate_limit_enabled": true,
        "rate_limit_requests": 1000,
    }))
    .expect("gateway metrics");
    assert_eq!(metrics.gateway, Some(true));
    assert_eq!(metrics.requests, Some(21));
    assert_eq!(metrics.rate_limit_enabled, Some(true));

    let jobs: JobsResponse = serde_json::from_value(json!({
        "jobs": [{
            "queue": "tracedb.snapshot.create",
            "state": "idle",
        }],
    }))
    .expect("gateway admin jobs");
    assert_eq!(jobs.jobs[0].queue, "tracedb.snapshot.create");
}

#[test]
fn client_executes_real_http_product_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    let client = TraceDbClient::new(TraceDbClientConfig::managed(
        format!("http://{addr}"),
        "dev-token",
    ));

    assert_eq!(client.ready().expect("ready")["ready"], true);
    assert_eq!(client.apply_schema(&schema()).expect("schema")["epoch"], 1);
    let batch = RecordPutBatchRequest::new(vec![
        record(
            "intro",
            "tenant-a",
            "rust database api quickstart",
            [1.0, 0.0, 0.0],
        ),
        record("ops", "tenant-a", "snapshot restore flow", [0.0, 1.0, 0.0]),
    ]);
    assert_eq!(
        client.put_batch(&batch).expect("put batch")["record_count"],
        2
    );
    assert_eq!(
        client
            .get(&RecordGetRequest::new("docs", "tenant-a", "intro"))
            .expect("get")["record"]["id"],
        "intro"
    );
    assert_eq!(
        client
            .scan(&RecordScanRequest::new("docs", "tenant-a").limit(10))
            .expect("scan")["returned_count"],
        2
    );
    let lean = client.query(&query(false)).expect("query");
    assert!(lean.get("results").is_some(), "lean query body: {lean}");
    assert!(lean.get("explain").is_none(), "lean query body: {lean}");

    let explained = client.explain(&query(false)).expect("explain");
    assert!(
        explained.get("returned_count").is_some(),
        "explain body: {explained}"
    );
    assert_eq!(
        client
            .delete(&RecordDeleteRequest::new("docs", "tenant-a", "ops"))
            .expect("delete")["deleted"],
        true
    );
    assert_eq!(
        client
            .get(&RecordGetRequest::new("docs", "tenant-a", "ops"))
            .expect("get deleted")["record"],
        serde_json::Value::Null
    );
}

#[test]
fn async_client_executes_real_http_read_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    let client = TraceDbAsyncClient::new(TraceDbClientConfig::managed(
        format!("http://{addr}"),
        "dev-token",
    ));

    assert!(block_on(client.ready_typed()).expect("async ready").ready);
    block_on(client.request_json(
        "POST",
        "/v1/schema/apply",
        Some(&serde_json::to_value(schema()).expect("schema json")),
    ))
    .expect("async schema apply");
    let batch = RecordPutBatchRequest::new(vec![
        record("async-intro", "tenant-a", "async rust sdk", [1.0, 0.0, 0.0]),
        record("async-ops", "tenant-a", "async read path", [0.0, 1.0, 0.0]),
    ]);
    block_on(client.request_json(
        "POST",
        "/v1/records/put-batch",
        Some(&serde_json::to_value(batch).expect("batch json")),
    ))
    .expect("async batch ingest");

    let scan = block_on(client.scan_typed(&RecordScanRequest::new("docs", "tenant-a").limit(10)))
        .expect("async scan");
    assert_eq!(scan.returned_count, 2);
    let query = block_on(client.query_typed(&query(false))).expect("async query");
    assert!(
        query
            .results
            .iter()
            .any(|row| row.record_id.as_str() == "async-intro"),
        "async query results: {:?}",
        query.results
    );
}

#[test]
fn client_executes_typed_http_product_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    let client = TraceDbClient::new(TraceDbClientConfig::managed(
        format!("http://{addr}"),
        "dev-token",
    ));

    let ready = client.ready_typed().expect("ready");
    assert!(ready.ready);
    assert_eq!(ready.service.as_deref(), Some("tracedb-engine"));
    let health = client.health_typed().expect("health");
    assert!(health.ok);
    assert_eq!(health.service.as_deref(), Some("tracedb-engine"));
    let databases = client.list_databases_typed().expect("databases");
    assert_eq!(databases.mode.as_deref(), Some("local"));
    assert_eq!(databases.databases.len(), 1);
    assert_eq!(databases.databases[0].database_id, "local");
    let branches = client.list_branches_typed().expect("branches");
    assert_eq!(branches.branches.len(), 1);
    assert!(branches.branches[0].branch_id.contains("main"));
    let metrics = client.public_safe_metrics_typed().expect("metrics");
    assert_eq!(metrics.service.as_deref(), Some("tracedb-engine"));
    assert!(metrics.latest_epoch.is_some());
    assert_eq!(
        client.apply_schema_typed(&schema()).expect("schema").epoch,
        1
    );
    let batch = RecordPutBatchRequest::new(vec![
        record(
            "intro",
            "tenant-a",
            "rust database api quickstart",
            [1.0, 0.0, 0.0],
        ),
        record("ops", "tenant-a", "snapshot restore flow", [0.0, 1.0, 0.0]),
    ]);
    let batch_response = client.put_batch_typed(&batch).expect("put batch");
    assert_eq!(batch_response.epoch, 2);
    assert_eq!(batch_response.record_count, 2);

    let got = client
        .get_record_typed(&RecordGetRequest::new("docs", "tenant-a", "intro"))
        .expect("get");
    assert_eq!(got.record.expect("record").id, "intro");

    let scan = client
        .scan_typed(&RecordScanRequest::new("docs", "tenant-a").limit(10))
        .expect("scan");
    assert_eq!(scan.returned_count, 2);
    assert_eq!(scan.records.len(), 2);

    let lean = client.query_typed(&query(false)).expect("query");
    assert_eq!(lean.results.len(), 2);
    assert!(lean.explain.is_none());
    let typed_rows: Vec<&HybridQueryRow> = lean.results.iter().collect();
    assert!(typed_rows.iter().any(|row| {
        row.record_id == "intro"
            && row.tenant_id == "tenant-a"
            && row.fields["id"] == "intro"
            && row.score.final_score.is_finite()
    }));

    let explained = client.explain_typed(&query(false)).expect("explain");
    assert_eq!(explained.returned_count, 2);

    let delete = client
        .delete_typed(&RecordDeleteRequest::new("docs", "tenant-a", "ops"))
        .expect("delete");
    assert!(delete.deleted);
    assert_eq!(delete.epoch, 3);
    let deleted = client
        .get_record_typed(&RecordGetRequest::new("docs", "tenant-a", "ops"))
        .expect("get deleted");
    assert!(deleted.record.is_none());
    let jobs = client.list_admin_jobs_typed().expect("admin jobs");
    assert!(jobs
        .jobs
        .iter()
        .any(|job| job.queue == "tracedb.snapshot.create" && job.state == "idle"));
}

#[test]
fn client_idempotency_options_replay_write_response_against_real_server() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(format!("http://{addr}"), "dev-token").with_safe_retries(2),
    );
    client.apply_schema_typed(&schema()).expect("schema");
    let batch = RecordPutBatchRequest::new(vec![record(
        "intro",
        "tenant-a",
        "rust database api quickstart",
        [1.0, 0.0, 0.0],
    )]);
    let options = TraceDbRequestOptions::new().with_idempotency_key("batch-intro-1");

    let first = client
        .put_batch_typed_with_options(&batch, &options)
        .expect("first batch");
    let replay = client
        .put_batch_typed_with_options(&batch, &options)
        .expect("replayed batch");

    assert_eq!(first.epoch, 2);
    assert_eq!(replay.epoch, 2);
    assert_eq!(
        client
            .scan_typed(&RecordScanRequest::new("docs", "tenant-a").limit(10))
            .expect("scan")
            .returned_count,
        1
    );

    let changed_batch = RecordPutBatchRequest::new(vec![record(
        "other",
        "tenant-a",
        "same idempotency key with a different body",
        [0.0, 1.0, 0.0],
    )]);
    let error = client
        .put_batch_typed_with_options(&changed_batch, &options)
        .expect_err("same key with changed body should conflict");
    assert_eq!(
        error.server_error().as_deref(),
        Some("idempotency key reused with different request body")
    );
    match error {
        TraceDbClientError::HttpStatus { status, body, .. } => {
            assert_eq!(status, 409);
            assert!(
                body.contains("idempotency key reused with different request body"),
                "conflict body: {body}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn client_executes_typed_snapshot_restore_with_idempotency_options() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().join("engine");
    let server_data_dir = data_dir.clone();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(server_data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    let client = TraceDbClient::new(TraceDbClientConfig::managed(
        format!("http://{addr}"),
        "dev-token",
    ));
    client.apply_schema_typed(&schema()).expect("schema");
    let batch = RecordPutBatchRequest::new(vec![record(
        "intro",
        "tenant-a",
        "rust database api quickstart",
        [1.0, 0.0, 0.0],
    )]);
    client.put_batch_typed(&batch).expect("put batch");

    let snapshot_dir = temp.path().join("snapshot-copy");
    let snapshot_target = snapshot_dir.to_string_lossy().to_string();
    let snapshot_request = SnapshotRequest::new(snapshot_target.clone());
    let snapshot_options = TraceDbRequestOptions::new().with_idempotency_key("snapshot-1");
    let snapshot = client
        .snapshot_typed_with_options(&snapshot_request, &snapshot_options)
        .expect("snapshot");
    assert!(snapshot.snapshot);
    assert_eq!(snapshot.target, snapshot_target);
    let snapshot_marker = snapshot_dir.join("idempotency-marker");
    fs::write(&snapshot_marker, "preserve").expect("write snapshot marker");
    let replayed_snapshot = client
        .snapshot_typed_with_options(&snapshot_request, &snapshot_options)
        .expect("replayed snapshot");
    assert_eq!(replayed_snapshot.target, snapshot_target);
    assert!(
        snapshot_marker.exists(),
        "idempotent snapshot replay should not recopy over the target"
    );

    let restore_dir = temp.path().join("restore-copy");
    let restore_target = restore_dir.to_string_lossy().to_string();
    let restore_request = RestoreRequest::new(snapshot_target.clone(), restore_target.clone());
    let restore_options = TraceDbRequestOptions::new().with_idempotency_key("restore-1");
    let restore = client
        .restore_typed_with_options(&restore_request, &restore_options)
        .expect("restore");
    assert!(restore.restored);
    assert_eq!(restore.source, snapshot_target);
    assert_eq!(restore.target, restore_target);
    let restore_marker = restore_dir.join("idempotency-marker");
    fs::write(&restore_marker, "preserve").expect("write restore marker");
    let replayed_restore = client
        .restore_typed_with_options(&restore_request, &restore_options)
        .expect("replayed restore");
    assert_eq!(replayed_restore.target, restore_target);
    assert!(
        restore_marker.exists(),
        "idempotent restore replay should not recopy over the target"
    );
}
