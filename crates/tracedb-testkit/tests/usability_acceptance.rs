use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tracedb_query::{
    FreshnessMode, HybridQuery, RecordDeleteRequest, RecordGetRequest, RecordInput,
    RecordPatchRequest, RecordPutRequest, RecordScanRequest, TableSchema, TraceDb,
    VectorColumnSchema,
};
use tracedb_sdk::{TraceDbClient, TraceDbClientConfig};

fn schema() -> TableSchema {
    TableSchema {
        name: "docs".to_string(),
        primary_id_column: "id".to_string(),
        tenant_id_column: "tenant".to_string(),
        scalar_columns: vec!["conversation".to_string(), "status".to_string()],
        text_indexed_columns: vec!["body".to_string()],
        vector_columns: vec![VectorColumnSchema {
            name: "embedding".to_string(),
            dimensions: 3,
            source_columns: vec!["body".to_string()],
        }],
    }
}

fn record(id: &str, tenant: &str, body: &str, status: &str, vector: [f32; 3]) -> RecordInput {
    RecordInput {
        table: "docs".to_string(),
        id: id.to_string(),
        tenant_id: tenant.to_string(),
        fields: json!({
            "id": id,
            "tenant": tenant,
            "conversation": "c1",
            "status": status,
            "body": body,
            "embedding": vector,
        })
        .as_object()
        .unwrap()
        .clone(),
    }
}

fn query() -> HybridQuery {
    HybridQuery {
        table: "docs".to_string(),
        tenant_id: "tenant-a".to_string(),
        text: Some("rust".to_string()),
        vector: Some(vec![1.0, 0.0, 0.0]),
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 10,
        freshness: FreshnessMode::Strict,
        explain: true,
    }
}

#[test]
fn embedded_crud_tombstone_compaction_snapshot_and_recovery_are_usable() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut db = TraceDb::open(temp.path()).expect("open");
    db.apply_schema(schema()).expect("schema");

    let put_epoch = db
        .put(RecordPutRequest::new(record(
            "a",
            "tenant-a",
            "rust database kernel",
            "draft",
            [1.0, 0.0, 0.0],
        )))
        .expect("put");
    assert_eq!(put_epoch.get(), 2);

    let got = db
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("get")
        .expect("record exists");
    assert_eq!(got.fields["status"], json!("draft"));

    db.patch(RecordPatchRequest::new(
        "docs",
        "tenant-a",
        "a",
        json!({ "status": "published", "body": "rust database kernel updated" })
            .as_object()
            .unwrap()
            .clone(),
    ))
    .expect("patch");
    let patched = db
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("get patched")
        .expect("patched exists");
    assert_eq!(patched.fields["status"], json!("published"));
    assert_eq!(patched.fields["embedding"], json!([1.0, 0.0, 0.0]));

    db.put(RecordPutRequest::new(record(
        "b",
        "tenant-a",
        "rust vector row",
        "published",
        [0.9, 0.1, 0.0],
    )))
    .expect("put b");
    let scanned = db
        .scan(RecordScanRequest::new("docs", "tenant-a").limit(10))
        .expect("scan");
    assert_eq!(scanned.records.len(), 2);

    db.delete(RecordDeleteRequest::new("docs", "tenant-a", "a").tombstone("user_delete"))
        .expect("delete");
    assert!(db
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("get deleted")
        .is_none());
    let result = db.query(query()).expect("query");
    assert!(!result.results.iter().any(|row| row.record_id == "a"));

    db.compact().expect("compact");
    assert!(db
        .inspect_manifest()
        .unwrap()
        .indexes
        .iter()
        .any(|index| { index.kind == "text" && index.state == tracedb_core::IndexState::Ready }));

    let snapshot_dir = temp.path().join("snapshot-copy");
    db.create_snapshot(&snapshot_dir).expect("snapshot");
    let restore_temp = tempfile::tempdir().expect("restore tempdir");
    let restored = TraceDb::restore_snapshot(&snapshot_dir, restore_temp.path()).expect("restore");
    assert!(restored
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("restored get deleted")
        .is_none());
    assert_eq!(
        restored
            .scan(RecordScanRequest::new("docs", "tenant-a").limit(10))
            .expect("restored scan")
            .records
            .len(),
        1
    );

    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("recovered get deleted")
        .is_none());
}

#[test]
fn http_api_exposes_crud_admin_metrics_and_readiness_routes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    assert_http_contains(addr, "GET", "/ready", "", "\"ready\":true");
    assert_http_contains(
        addr,
        "GET",
        "/metrics",
        "",
        "\"service\":\"tracedb-engine\"",
    );
    assert_http_contains(
        addr,
        "POST",
        "/v1/schema/apply",
        &serde_json::to_string(&schema()).unwrap(),
        "\"epoch\":1",
    );
    assert_http_contains(
        addr,
        "POST",
        "/v1/records/put",
        &serde_json::to_string(&record(
            "a",
            "tenant-a",
            "rust database kernel",
            "draft",
            [1.0, 0.0, 0.0],
        ))
        .unwrap(),
        "\"epoch\":2",
    );
    assert_http_contains(
        addr,
        "POST",
        "/v1/records/put-batch",
        r#"{"records":[{"table":"docs","id":"b","tenant_id":"tenant-a","fields":{"id":"b","tenant":"tenant-a","body":"batch http write","status":"draft","embedding":[0.0,1.0,0.0]}}]}"#,
        "\"record_count\":1",
    );
    let batch_timing_response = http_response(
        addr,
        "POST",
        "/v1/records/put-batch",
        r#"{"include_write_timing":true,"records":[{"table":"docs","id":"c","tenant_id":"tenant-a","fields":{"id":"c","tenant":"tenant-a","body":"timed batch http write","status":"draft","embedding":[0.0,0.0,1.0]}}]}"#,
    );
    assert!(
        batch_timing_response.contains("\"write_timing\""),
        "batch write timing response missing write_timing: {batch_timing_response}"
    );
    assert!(
        batch_timing_response.contains("\"wal_payload_bytes\""),
        "batch write timing response missing WAL byte attribution: {batch_timing_response}"
    );
    for field in [
        "store_apply_validate_identity_ms",
        "store_apply_validate_vector_ms",
        "store_apply_key_ms",
        "store_apply_fields_ms",
        "store_apply_finalize_identity_ms",
        "store_apply_features_ms",
        "store_apply_install_ms",
    ] {
        assert!(
            batch_timing_response.contains(&format!("\"{field}\"")),
            "batch write timing response missing store-apply attribution field {field}: {batch_timing_response}"
        );
    }
    assert_http_contains(
        addr,
        "POST",
        "/v1/records/patch",
        r#"{"table":"docs","tenant_id":"tenant-a","id":"a","fields":{"status":"published"}}"#,
        "\"epoch\":5",
    );
    assert_http_contains(
        addr,
        "POST",
        "/v1/records/get",
        r#"{"table":"docs","tenant_id":"tenant-a","id":"a"}"#,
        "\"status\":\"published\"",
    );
    assert_http_contains(
        addr,
        "POST",
        "/v1/records/scan",
        r#"{"table":"docs","tenant_id":"tenant-a","limit":10}"#,
        "\"returned_count\":3",
    );
    let query_response = http_response(
        addr,
        "POST",
        "/v1/query",
        r#"{"table":"docs","tenant_id":"tenant-a","text":"rust","vector":[1.0,0.0,0.0],"top_k":5,"freshness":"AllowDirty","explain":true}"#,
    );
    let lowered_query_response = query_response.to_ascii_lowercase();
    assert!(
        lowered_query_response.contains("server-timing:"),
        "query response should include server timing header: {query_response}"
    );
    for token in [
        "read;dur=",
        "parse;dur=",
        "lock_wait;dur=",
        "engine;dur=",
        "engine_core;dur=",
        "explain_build;dur=",
        "materialize;dur=",
        "response_shape;dur=",
        "body_encode;dur=",
        "encode;dur=",
        "prewrite_total;dur=",
    ] {
        assert!(
            lowered_query_response.contains(token),
            "query server timing should include {token}: {query_response}"
        );
    }
    assert_http_contains(
        addr,
        "POST",
        "/v1/records/delete",
        r#"{"table":"docs","tenant_id":"tenant-a","id":"a","tombstone":"user_delete"}"#,
        "\"deleted\":true",
    );
    assert_http_contains(
        addr,
        "POST",
        "/v1/admin/compact",
        r#"{}"#,
        "\"compacted\":true",
    );
    assert_http_contains(addr, "GET", "/v1/admin/jobs", "", "tracedb.segment.compact");
}

#[test]
fn http_query_explain_false_is_lean_while_explain_surfaces_remain_full() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    assert_http_contains(
        addr,
        "POST",
        "/v1/schema/apply",
        &serde_json::to_string(&schema()).unwrap(),
        "\"epoch\":1",
    );
    assert_http_contains(
        addr,
        "POST",
        "/v1/records/put",
        &serde_json::to_string(&record(
            "a",
            "tenant-a",
            "rust database kernel",
            "published",
            [1.0, 0.0, 0.0],
        ))
        .unwrap(),
        "\"epoch\":2",
    );

    let lean_query = http_response(
        addr,
        "POST",
        "/v1/query",
        r#"{"table":"docs","tenant_id":"tenant-a","text":"rust","vector":[1.0,0.0,0.0],"top_k":5,"freshness":"AllowDirty","explain":false}"#,
    );
    assert!(
        lean_query.starts_with("HTTP/1.1 200 OK"),
        "unexpected lean query response: {lean_query}"
    );
    assert!(
        lean_query.contains("\"results\""),
        "lean query should still return result rows: {lean_query}"
    );
    assert!(
        !lean_query.contains("\"explain\""),
        "explain=false query should not serialize explain payload: {lean_query}"
    );
    let lean_json = http_json_body(&lean_query);
    assert!(lean_json.get("results").is_some(), "lean body: {lean_json}");
    assert!(lean_json.get("explain").is_none(), "lean body: {lean_json}");

    let explain_query = http_response(
        addr,
        "POST",
        "/v1/query",
        r#"{"table":"docs","tenant_id":"tenant-a","text":"rust","vector":[1.0,0.0,0.0],"top_k":5,"freshness":"AllowDirty","explain":true}"#,
    );
    assert!(
        explain_query.starts_with("HTTP/1.1 200 OK"),
        "unexpected explain query response: {explain_query}"
    );
    assert!(
        explain_query.contains("\"explain\""),
        "explain=true query should keep explain payload: {explain_query}"
    );
    let explain_query_json = http_json_body(&explain_query);
    assert!(
        explain_query_json.get("results").is_some(),
        "explain query body: {explain_query_json}"
    );
    assert!(
        explain_query_json.get("explain").is_some(),
        "explain query body: {explain_query_json}"
    );

    let explain_endpoint = http_response(
        addr,
        "POST",
        "/v1/explain",
        r#"{"table":"docs","tenant_id":"tenant-a","text":"rust","vector":[1.0,0.0,0.0],"top_k":5,"freshness":"AllowDirty","explain":false}"#,
    );
    assert!(
        explain_endpoint.starts_with("HTTP/1.1 200 OK"),
        "unexpected explain endpoint response: {explain_endpoint}"
    );
    assert!(
        explain_endpoint.contains("\"returned_count\""),
        "explain endpoint should return explain fields: {explain_endpoint}"
    );
    assert!(
        !explain_endpoint.contains("\"results\""),
        "explain endpoint should not return query rows: {explain_endpoint}"
    );
    let explain_endpoint_json = http_json_body(&explain_endpoint);
    assert!(
        explain_endpoint_json.get("returned_count").is_some(),
        "explain endpoint body: {explain_endpoint_json}"
    );
    assert!(
        explain_endpoint_json.get("results").is_none(),
        "explain endpoint body: {explain_endpoint_json}"
    );
}

#[test]
fn http_server_rejects_oversized_content_length_before_body_read() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .write_all(
            b"POST /v1/query HTTP/1.1\r\nHost: localhost\r\nContent-Length: 16777217\r\n\r\n",
        )
        .expect("write headers");
    let _ = stream.shutdown(std::net::Shutdown::Write);

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    assert!(
        response.contains("request body exceeds 16MiB"),
        "response should reject oversized body before reading it, got {response:?}"
    );
}

#[test]
fn sdk_builds_stable_usability_requests() {
    let client = TraceDbClient::new(TraceDbClientConfig::managed(
        "http://localhost:18081",
        "dev-token",
    ));
    let put = client
        .table("docs")
        .tenant("tenant-a")
        .put("a")
        .field("body", json!("hello"))
        .build();
    assert_eq!(put.path, "/v1/records/put");
    assert_eq!(put.body["id"], json!("a"));

    let scan = client
        .table("docs")
        .tenant("tenant-a")
        .scan()
        .limit(25)
        .build();
    assert_eq!(scan.path, "/v1/records/scan");
    assert_eq!(scan.body["limit"], json!(25));

    let delete = client.table("docs").tenant("tenant-a").delete("a").build();
    assert_eq!(delete.path, "/v1/records/delete");
    assert_eq!(delete.body["tombstone"], json!("user_delete"));
}

#[test]
fn local_cloud_packaging_declares_cloud_shape_without_engine_volume_leaks() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    assert!(root.join("Dockerfile").exists());
    assert!(root.join("docker-compose.yml").exists());
    assert!(root.join("docs/Operations/Local Cloud.md").exists());

    let compose = std::fs::read_to_string(root.join("docker-compose.yml")).unwrap();
    for service in [
        "tracedb-gateway",
        "tracedb-engine",
        "tracedb-worker",
        "postgres-catalog",
        "valkey-queue",
        "minio-bucket",
    ] {
        assert!(compose.contains(service), "missing {service}");
    }
    assert!(compose.contains("TRACEDB_SERVICE_MODE=engine"));
    assert!(compose.contains("tracedb-data:/data/tracedb"));
    assert!(!gateway_or_worker_mount_engine_data(&compose));
}

#[test]
fn versioned_http_api_reference_tracks_current_product_routes() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let api_doc = root.join("docs/api/v1-http.md");
    let markdown = std::fs::read_to_string(&api_doc)
        .unwrap_or_else(|error| panic!("read {}: {error}", api_doc.display()));

    for route in [
        "GET /v1/health",
        "GET /v1/ready",
        "GET /v1/databases",
        "GET /v1/branches",
        "GET /v1/metrics/public-safe",
        "POST /v1/schema/apply",
        "POST /v1/insert",
        "POST /v1/records/put",
        "POST /v1/records/put-batch",
        "POST /v1/records/patch",
        "POST /v1/records/delete",
        "POST /v1/records/get",
        "POST /v1/records/scan",
        "POST /v1/query",
        "POST /v1/explain",
        "POST /v1/admin/compact",
        "POST /v1/admin/snapshot",
        "POST /v1/admin/restore",
        "GET /v1/admin/jobs",
    ] {
        assert!(
            markdown.contains(&format!("`{route}`")),
            "versioned API reference missing `{route}`"
        );
    }

    for boundary in [
        "SQL compatibility is not implemented",
        "Mutation and admin routes are not retried by the SDK without an explicit idempotency contract",
        "Internal TraceDB-only runs are development evidence",
    ] {
        assert!(
            markdown.contains(boundary),
            "versioned API reference missing boundary: {boundary}"
        );
    }

    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README");
    assert!(
        readme.contains("docs/api/v1-http.md"),
        "README should link to the versioned v1 API reference"
    );
}

#[test]
fn generated_openapi_v1_artifact_tracks_current_product_routes() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let artifact = root.join("docs/api/v1-openapi.json");
    let generator = root.join("scripts/generate_openapi_v1.py");

    let check = Command::new("python3")
        .arg(&generator)
        .arg("--check")
        .current_dir(root)
        .output()
        .unwrap_or_else(|error| panic!("run {} --check: {error}", generator.display()));
    assert!(
        check.status.success(),
        "OpenAPI artifact should be reproducible from generator\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&check.stdout),
        String::from_utf8_lossy(&check.stderr)
    );

    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(&artifact)
            .unwrap_or_else(|error| panic!("read {}: {error}", artifact.display())),
    )
    .expect("parse OpenAPI JSON");

    assert_eq!(spec["openapi"], json!("3.1.0"));
    assert_eq!(spec["info"]["title"], json!("TraceDB v1 HTTP API"));
    assert_eq!(spec["info"]["version"], json!("0.1.0-development"));
    let description = spec["info"]["description"]
        .as_str()
        .expect("OpenAPI info.description");
    for boundary in [
        "SQL compatibility is not implemented",
        "Internal TraceDB-only runs are development evidence",
        "Mutation and admin routes are not retried by the SDK without an explicit idempotency contract",
    ] {
        assert!(
            description.contains(boundary),
            "OpenAPI description missing boundary: {boundary}"
        );
    }

    let mut operation_ids = BTreeSet::new();
    for (method, path) in [
        ("get", "/v1/health"),
        ("get", "/v1/ready"),
        ("get", "/v1/databases"),
        ("get", "/v1/branches"),
        ("get", "/v1/metrics/public-safe"),
        ("post", "/v1/schema/apply"),
        ("post", "/v1/insert"),
        ("post", "/v1/records/put"),
        ("post", "/v1/records/put-batch"),
        ("post", "/v1/records/patch"),
        ("post", "/v1/records/delete"),
        ("post", "/v1/records/get"),
        ("post", "/v1/records/scan"),
        ("post", "/v1/query"),
        ("post", "/v1/explain"),
        ("post", "/v1/admin/compact"),
        ("post", "/v1/admin/snapshot"),
        ("post", "/v1/admin/restore"),
        ("get", "/v1/admin/jobs"),
    ] {
        let operation = &spec["paths"][path][method];
        let operation_id = operation["operationId"]
            .as_str()
            .unwrap_or_else(|| panic!("OpenAPI artifact missing operationId for {method} {path}"));
        assert!(
            operation_ids.insert(operation_id.to_string()),
            "OpenAPI artifact has duplicate operationId {operation_id}"
        );
        assert!(
            operation["responses"]["200"]["content"]["application/json"]["schema"].is_object(),
            "OpenAPI artifact missing JSON 200 response for {method} {path}"
        );
        for status in ["400", "401", "404", "429", "500", "502", "503"] {
            assert!(
                operation["responses"][status]["content"]["application/json"]["schema"].is_object(),
                "OpenAPI artifact missing JSON {status} response for {method} {path}"
            );
        }
        if method == "post" {
            assert!(
                operation["requestBody"]["content"]["application/json"]["schema"].is_object(),
                "OpenAPI artifact missing JSON request body for {method} {path}"
            );
        } else {
            assert!(
                operation.get("requestBody").is_none(),
                "OpenAPI artifact should not declare a request body for {method} {path}"
            );
        }
        assert!(
            operation["x-tracedb-mutates-state"].is_boolean(),
            "OpenAPI artifact missing mutation marker for {method} {path}"
        );
        assert!(
            operation["x-tracedb-sdk-safe-retry"].is_boolean(),
            "OpenAPI artifact missing SDK retry marker for {method} {path}"
        );
    }

    for schema_name in [
        "TableSchema",
        "RecordInput",
        "RecordOutput",
        "RecordGetRequest",
        "RecordScanRequest",
        "RecordScanOutput",
        "RecordPatchRequest",
        "RecordDeleteRequest",
        "RecordPutBatchRequest",
        "HybridQuery",
        "HybridQueryRow",
        "HybridExplain",
    ] {
        assert!(
            spec["components"]["schemas"][schema_name].is_object(),
            "OpenAPI artifact missing component schema {schema_name}"
        );
    }

    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README");
    assert!(
        readme.contains("docs/api/v1-openapi.json"),
        "README should link to the generated OpenAPI artifact"
    );
}

fn gateway_or_worker_mount_engine_data(compose: &str) -> bool {
    let mut current_service = "";
    for line in compose.lines() {
        if line.starts_with("  tracedb-gateway:") {
            current_service = "gateway";
        } else if line.starts_with("  tracedb-worker:") {
            current_service = "worker";
        } else if line.starts_with("  tracedb-engine:") || line.starts_with("  postgres-catalog:") {
            current_service = "";
        }
        if matches!(current_service, "gateway" | "worker") && line.contains("/data/tracedb") {
            return true;
        }
    }
    false
}

fn assert_http_contains(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    expected: &str,
) {
    let response = http_response(addr, method, path, body);
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "unexpected response: {response}"
    );
    assert!(
        response.contains(expected),
        "expected {expected} in {response}"
    );
}

fn http_response(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn http_json_body(response: &str) -> Value {
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .expect("http response body");
    serde_json::from_str(body).expect("json response body")
}
