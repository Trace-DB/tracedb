use serde_json::json;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tracedb_query::{
    FreshnessMode, HybridQuery, HybridQueryRow, RecordDeleteRequest, RecordGetRequest, RecordInput,
    RecordPutBatchRequest, RecordScanRequest, TableSchema, VectorColumnSchema,
};
use tracedb_sdk::{TraceDbClient, TraceDbClientConfig, TraceDbClientError};

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
        let request_text = String::from_utf8(request).expect("utf8 request");
        let (_, body) = request_text
            .split_once("\r\n\r\n")
            .expect("request header boundary");
        serde_json::from_str(body).expect("json request body")
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

    match error {
        TraceDbClientError::HttpStatus {
            method,
            path,
            status,
            body,
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
}
