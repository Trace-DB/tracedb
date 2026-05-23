use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
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
        text_field: None,
        text: Some("rust".to_string()),
        vector_field: None,
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
    let addr = start_http_test_server(temp.path().to_path_buf());

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
    let snapshot_dir = temp.path().join("http-snapshot");
    let restore_dir = temp.path().join("http-restore");
    assert_http_contains(
        addr,
        "POST",
        "/v1/admin/snapshot",
        &json!({ "target": snapshot_dir }).to_string(),
        "\"snapshot\":true",
    );
    let restore_response = http_response(
        addr,
        "POST",
        "/v1/admin/restore",
        &json!({
            "source": snapshot_dir,
            "target": restore_dir,
            "verify_record": {
                "table": "docs",
                "tenant_id": "tenant-a",
                "id": "a"
            }
        })
        .to_string(),
    );
    assert!(
        restore_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected restore response: {restore_response}"
    );
    let restore_json = http_json_body(&restore_response);
    assert_eq!(restore_json["restored"], json!(true));
    assert_eq!(restore_json["verification"]["status"], json!("passed"));
    assert_eq!(restore_json["verification"]["record_visible"], json!(true));
    assert_eq!(restore_json["verification"]["record"]["id"], json!("a"));
    assert_http_contains(addr, "GET", "/v1/admin/jobs", "", "tracedb.segment.compact");
    let missing_response = http_response(addr, "GET", "/v1/missing", "");
    assert!(
        missing_response.starts_with("HTTP/1.1 404 Not Found"),
        "unexpected missing-route response: {missing_response}"
    );
    let missing_json = http_json_body(&missing_response);
    assert_eq!(missing_json["error"], json!("not found"));
    assert_eq!(missing_json["code"], json!("not_found"));
}

#[test]
fn http_idempotency_key_replays_write_response_and_rejects_mismatched_body() {
    let temp = tempfile::tempdir().expect("tempdir");
    let addr = start_http_test_server(temp.path().to_path_buf());

    assert_http_contains(
        addr,
        "POST",
        "/v1/schema/apply",
        &serde_json::to_string(&schema()).unwrap(),
        "\"epoch\":1",
    );

    let first_body = serde_json::to_string(&record(
        "a",
        "tenant-a",
        "rust database kernel",
        "draft",
        [1.0, 0.0, 0.0],
    ))
    .unwrap();
    let first_response = http_response_with_headers(
        addr,
        "POST",
        "/v1/records/put",
        &[("Idempotency-Key", "put-a-1")],
        &first_body,
    );
    assert!(
        first_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected first response: {first_response}"
    );
    assert_eq!(http_json_body(&first_response)["epoch"], json!(2));

    let replay_response = http_response_with_headers(
        addr,
        "POST",
        "/v1/records/put",
        &[("Idempotency-Key", "put-a-1")],
        &first_body,
    );
    assert!(
        replay_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected replay response: {replay_response}"
    );
    assert_eq!(http_json_body(&replay_response)["epoch"], json!(2));

    let mismatched_body = serde_json::to_string(&record(
        "b",
        "tenant-a",
        "different record under reused key",
        "draft",
        [0.0, 1.0, 0.0],
    ))
    .unwrap();
    let mismatch_response = http_response_with_headers(
        addr,
        "POST",
        "/v1/records/put",
        &[("Idempotency-Key", "put-a-1")],
        &mismatched_body,
    );
    assert!(
        mismatch_response.starts_with("HTTP/1.1 409 Conflict"),
        "idempotency key reuse with different body should conflict: {mismatch_response}"
    );
    assert!(
        mismatch_response.contains("idempotency key reused with different request body"),
        "conflict should explain the idempotency-key violation: {mismatch_response}"
    );

    let scan_response = http_response(
        addr,
        "POST",
        "/v1/records/scan",
        r#"{"table":"docs","tenant_id":"tenant-a","limit":10}"#,
    );
    let scan = http_json_body(&scan_response);
    assert_eq!(scan["returned_count"], json!(1));
    assert_eq!(scan["records"][0]["id"], json!("a"));
}

#[test]
fn http_idempotency_key_replays_after_engine_reopen_from_same_data_dir() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().to_path_buf();
    let (addr, first_shutdown, first_handle) = start_stoppable_server(data_dir.clone());

    assert_http_contains(
        addr,
        "POST",
        "/v1/schema/apply",
        &serde_json::to_string(&schema()).unwrap(),
        "\"epoch\":1",
    );
    let first_body = serde_json::to_string(&record(
        "a",
        "tenant-a",
        "rust database kernel",
        "draft",
        [1.0, 0.0, 0.0],
    ))
    .unwrap();
    let first_response = http_response_with_headers(
        addr,
        "POST",
        "/v1/records/put",
        &[("Idempotency-Key", "put-a-reopen-1")],
        &first_body,
    );
    assert!(
        first_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected first response: {first_response}"
    );
    assert_eq!(http_json_body(&first_response)["epoch"], json!(2));
    let cache_json: Value = serde_json::from_str(
        &fs::read_to_string(data_dir.join("http-idempotency-cache.json"))
            .expect("idempotency cache file"),
    )
    .expect("idempotency cache JSON");
    assert!(
        cache_json.as_array().unwrap().iter().any(|entry| {
            entry["method"] == json!("POST")
                && entry["path"] == json!("/v1/records/put")
                && entry["key"] == json!("put-a-reopen-1")
        }),
        "first engine should persist the replay entry before clean reopen: {cache_json}"
    );
    stop_stoppable_server(first_shutdown, first_handle);

    let (reopened_addr, reopened_shutdown, reopened_handle) = start_stoppable_server(data_dir);

    let replay_response = http_response_with_headers(
        reopened_addr,
        "POST",
        "/v1/records/put",
        &[("Idempotency-Key", "put-a-reopen-1")],
        &first_body,
    );
    assert!(
        replay_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected cross-reopen replay response: {replay_response}"
    );
    assert_eq!(
        http_json_body(&replay_response)["epoch"],
        json!(2),
        "reopened engine should replay the first idempotent response"
    );

    let mismatched_body = serde_json::to_string(&record(
        "b",
        "tenant-a",
        "different record under reused key after reopen",
        "draft",
        [0.0, 1.0, 0.0],
    ))
    .unwrap();
    let mismatch_response = http_response_with_headers(
        reopened_addr,
        "POST",
        "/v1/records/put",
        &[("Idempotency-Key", "put-a-reopen-1")],
        &mismatched_body,
    );
    assert!(
        mismatch_response.starts_with("HTTP/1.1 409 Conflict"),
        "reopened engine should preserve mismatched-body conflicts: {mismatch_response}"
    );

    let scan_response = http_response(
        reopened_addr,
        "POST",
        "/v1/records/scan",
        r#"{"table":"docs","tenant_id":"tenant-a","limit":10}"#,
    );
    let scan = http_json_body(&scan_response);
    assert_eq!(scan["returned_count"], json!(1));
    assert_eq!(scan["records"][0]["id"], json!("a"));
    stop_stoppable_server(reopened_shutdown, reopened_handle);
}

#[test]
fn http_query_explain_false_is_lean_while_explain_surfaces_remain_full() {
    let temp = tempfile::tempdir().expect("tempdir");
    let addr = start_http_test_server(temp.path().to_path_buf());

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
fn http_traceql_endpoint_executes_native_query_string_through_hybrid_query() {
    let temp = tempfile::tempdir().expect("tempdir");
    let addr = start_http_test_server(temp.path().to_path_buf());

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
    assert_http_contains(
        addr,
        "POST",
        "/v1/records/put",
        &serde_json::to_string(&record(
            "b",
            "tenant-a",
            "rust draft note",
            "draft",
            [0.0, 1.0, 0.0],
        ))
        .unwrap(),
        "\"epoch\":3",
    );

    let traceql = json!({
        "query": "FROM docs\nTENANT tenant-a\nWHERE status = \"published\"\nMATCH body \"rust\"\nNEAR embedding [1.0, 0.0, 0.0]\nFRESHNESS allow_dirty\nLIMIT 5"
    })
    .to_string();
    let response = http_response(addr, "POST", "/v1/traceql", &traceql);
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "unexpected TraceQL response: {response}"
    );
    let body = http_json_body(&response);
    assert!(body.get("results").is_some(), "TraceQL body: {body}");
    assert!(body.get("explain").is_none(), "TraceQL body: {body}");
    assert_eq!(body["results"][0]["record_id"], json!("a"));
    assert_eq!(body["results"].as_array().expect("results").len(), 1);

    let explain_traceql = json!({
        "query": "FROM docs\nTENANT tenant-a\nMATCH body \"rust\"\nNEAR embedding [1.0, 0.0, 0.0]\nFRESHNESS allow_dirty\nLIMIT 5\nEXPLAIN"
    })
    .to_string();
    let explain_response = http_response(addr, "POST", "/v1/traceql", &explain_traceql);
    assert!(
        explain_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected TraceQL explain response: {explain_response}"
    );
    let explain_body = http_json_body(&explain_response);
    assert!(
        explain_body.get("results").is_some(),
        "TraceQL explain body: {explain_body}"
    );
    assert!(
        explain_body.get("explain").is_some(),
        "TraceQL explain body: {explain_body}"
    );

    let invalid_traceql = json!({
        "query": "FROM docs\nTENANT tenant-a\nDROP TABLE docs"
    })
    .to_string();
    let invalid_response = http_response(addr, "POST", "/v1/traceql", &invalid_traceql);
    assert!(
        invalid_response.starts_with("HTTP/1.1 400 Bad Request"),
        "invalid TraceQL should preserve bad-request envelope: {invalid_response}"
    );
    let invalid_body = http_json_body(&invalid_response);
    assert_eq!(invalid_body["code"], json!("bad_request"));
    assert!(
        invalid_body["error"]
            .as_str()
            .expect("error string")
            .contains("invalid TraceQL"),
        "invalid TraceQL body: {invalid_body}"
    );

    let sqlish = json!({
        "query": "SELECT * FROM docs WHERE tenant_id = \"tenant-a\" AND status = \"published\" LIMIT 5"
    })
    .to_string();
    let sqlish_response = http_response(addr, "POST", "/v1/traceql", &sqlish);
    assert!(
        sqlish_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected SQL-ish TraceQL response: {sqlish_response}"
    );
    let sqlish_body = http_json_body(&sqlish_response);
    assert_eq!(sqlish_body["results"][0]["record_id"], json!("a"));
    assert!(
        sqlish_body.get("explain").is_none(),
        "SQL-ish body: {sqlish_body}"
    );

    let invalid_sqlish = json!({
        "query": "SELECT * FROM docs JOIN users ON docs.user_id = users.id WHERE tenant_id = \"tenant-a\""
    })
    .to_string();
    let invalid_sqlish_response = http_response(addr, "POST", "/v1/traceql", &invalid_sqlish);
    assert!(
        invalid_sqlish_response.starts_with("HTTP/1.1 400 Bad Request"),
        "invalid SQL-ish should preserve bad-request envelope: {invalid_sqlish_response}"
    );
    let invalid_sqlish_body = http_json_body(&invalid_sqlish_response);
    assert_eq!(invalid_sqlish_body["code"], json!("bad_request"));
    assert!(
        invalid_sqlish_body["error"]
            .as_str()
            .expect("error string")
            .contains("SQL-ish"),
        "invalid SQL-ish body: {invalid_sqlish_body}"
    );
}

#[test]
fn http_graphql_endpoint_executes_bounded_query_through_hybrid_query() {
    let temp = tempfile::tempdir().expect("tempdir");
    let addr = start_http_test_server(temp.path().to_path_buf());

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
    assert_http_contains(
        addr,
        "POST",
        "/v1/records/put",
        &serde_json::to_string(&record(
            "b",
            "tenant-a",
            "graphql draft note",
            "draft",
            [0.0, 1.0, 0.0],
        ))
        .unwrap(),
        "\"epoch\":3",
    );

    let graphql = json!({
        "query": "query { docs(tenant_id: \"tenant-a\", where: {status: \"published\"}, match: \"rust\", near: [1.0, 0.0, 0.0], freshness: ALLOW_DIRTY, limit: 5) { record_id } }"
    })
    .to_string();
    let response = http_response(addr, "POST", "/v1/graphql", &graphql);
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "unexpected GraphQL response: {response}"
    );
    let body = http_json_body(&response);
    assert!(body.get("results").is_some(), "GraphQL body: {body}");
    assert!(body.get("explain").is_none(), "GraphQL body: {body}");
    assert_eq!(body["results"][0]["record_id"], json!("a"));

    let explain_graphql = json!({
        "query": "query { docs(tenant_id: \"tenant-a\", match: \"rust\", near: [1.0, 0.0, 0.0], freshness: ALLOW_DIRTY, limit: 5, explain: true) { record_id } }"
    })
    .to_string();
    let explain_response = http_response(addr, "POST", "/v1/graphql", &explain_graphql);
    assert!(
        explain_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected GraphQL explain response: {explain_response}"
    );
    let explain_body = http_json_body(&explain_response);
    assert!(
        explain_body.get("results").is_some(),
        "GraphQL explain body: {explain_body}"
    );
    assert!(
        explain_body.get("explain").is_some(),
        "GraphQL explain body: {explain_body}"
    );

    let invalid_graphql = json!({
        "query": "mutation { docs(tenant_id: \"tenant-a\") { record_id } }"
    })
    .to_string();
    let invalid_response = http_response(addr, "POST", "/v1/graphql", &invalid_graphql);
    assert!(
        invalid_response.starts_with("HTTP/1.1 400 Bad Request"),
        "invalid GraphQL should preserve bad-request envelope: {invalid_response}"
    );
    let invalid_body = http_json_body(&invalid_response);
    assert_eq!(invalid_body["code"], json!("bad_request"));
    assert!(
        invalid_body["error"]
            .as_str()
            .expect("error string")
            .contains("invalid GraphQL adapter"),
        "invalid GraphQL body: {invalid_body}"
    );
}

#[test]
fn http_graphql_schema_exports_sdl_from_applied_table_schema() {
    let temp = tempfile::tempdir().expect("tempdir");
    let addr = start_http_test_server(temp.path().to_path_buf());

    assert_http_contains(
        addr,
        "POST",
        "/v1/schema/apply",
        &serde_json::to_string(&schema()).unwrap(),
        "\"epoch\":1",
    );

    let response = http_response(addr, "GET", "/v1/graphql/schema", "");
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "unexpected GraphQL schema response: {response}"
    );
    let body = http_json_body(&response);
    assert_eq!(body["adapter"], json!("bounded_graphql_query_adapter"));
    assert_eq!(body["tables"], json!(["docs"]));
    let sdl = body["schema"].as_str().expect("schema SDL string");
    for token in [
        "scalar TraceDBJSON",
        "enum TraceDBFreshness",
        "type Query",
        "docs(",
        "tenant_id: ID!",
        "where: DocsWhere",
        "type DocsRow",
        "status: TraceDBJSON",
        "body: String",
        "embedding: [Float!]",
    ] {
        assert!(sdl.contains(token), "SDL missing {token}: {sdl}");
    }
    assert_eq!(
        body["execution"],
        json!("POST /v1/graphql returns TraceDB QueryResponse, not a GraphQL data envelope")
    );
}

#[test]
fn http_server_rejects_oversized_content_length_before_body_read() {
    let temp = tempfile::tempdir().expect("tempdir");
    let addr = start_http_test_server(temp.path().to_path_buf());

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
        "POST /v1/traceql",
        "POST /v1/graphql",
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
        "Idempotency-Key supports local data-dir-backed replay",
        "exits non-zero when any check fails while preserving the JSON summary",
        "server_error_code",
        "TraceDbAsyncClient",
        "background thread per request",
        "async typed write/admin helpers",
        "`{ \"error\": string, \"code\"?: string }`",
        "stable machine-readable value",
        "TRACEDB_URL",
        "TRACEDB_TOKEN",
        "TRACEDB_TIMEOUT_MS",
        "TRACEDB_SAFE_RETRIES",
        "TRACEDB_WAIT_READY_MS",
        "TRACEDB_DATABASE_ID",
        "TRACEDB_BRANCH_ID",
        "ready_wait_timeout_ms",
        "cargo run -p tracedb-cli -- doctor http",
        "Internal TraceDB-only runs are development evidence",
        "No cursor metadata is emitted today",
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
fn local_product_regression_runner_declares_current_product_gate() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let source_path = root.join("crates/tracedb-cli/src/main.rs");
    let source = std::fs::read_to_string(&source_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", source_path.display()));

    for token in [
        "product-regression",
        "product-quickstart",
        "local-product-regression",
        "embedded_demo",
        "embedded_verify",
        "http_demo",
        "local_doctor",
        "rust_sdk_quickstart",
        "python_sdk_smoke",
        "typescript_check",
        "typescript_http_smoke",
        "typescript_gateway_smoke",
        "--inject-failure",
        "--list-steps",
        "--only",
        "--report-file",
        "report_file",
        "typescript_enabled",
        "target/tracedb/product-quickstart.json",
        "embedded_demo",
        "embedded_verify",
        "only_step",
        "local-product-regression-step-list",
        "only_supported",
        "conflicts with --skip-typescript",
        "human_summary",
        "failure_injection",
        "injected_failure",
        "TRACEDB_WAIT_READY_MS",
        "local_only",
        "not_implemented",
        "not_checked",
    ] {
        assert!(
            source.contains(token),
            "product smoke runner should include {token}"
        );
    }

    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README");
    let docs_readme =
        std::fs::read_to_string(root.join("docs/README.md")).expect("read docs README");
    let api_doc = std::fs::read_to_string(root.join("docs/api/v1-http.md")).expect("read API doc");
    for (name, markdown) in [
        ("README", readme.as_str()),
        ("docs README", docs_readme.as_str()),
    ] {
        assert!(
            markdown.contains("modal run scripts/modal_product_verify.py --mode quickstart"),
            "{name} should document the Modal product verification lane"
        );
        assert!(
            markdown.contains("remote Linux product verification"),
            "{name} should scope Modal as remote Linux product verification"
        );
    }
    for (name, markdown) in [
        ("README", readme),
        ("docs README", docs_readme),
        ("API doc", api_doc),
    ] {
        assert!(
            markdown.contains("cargo run -p tracedb-cli -- product-regression"),
            "{name} should document the local product smoke runner"
        );
        assert!(
            markdown.contains("local product regression"),
            "{name} should keep the runner scoped as local product regression"
        );
        assert!(
            markdown.contains("--inject-failure STEP"),
            "{name} should document the local product regression failure contract"
        );
        assert!(
            markdown.contains("--list-steps"),
            "{name} should document product regression step discovery"
        );
        assert!(
            markdown.contains("only_supported"),
            "{name} should document product regression selector support metadata"
        );
        assert!(
            markdown.contains("human_summary"),
            "{name} should document product regression human-readable JSON summary metadata"
        );
        assert!(
            markdown.contains("--report-file PATH"),
            "{name} should document product regression report-file output"
        );
        assert!(
            markdown.contains("product-quickstart"),
            "{name} should document the local product quickstart wrapper"
        );
        assert!(
            markdown.contains("target/tracedb/product-quickstart.json"),
            "{name} should document the default product quickstart report path"
        );
        assert!(
            markdown.contains("report_file"),
            "{name} should document the machine-readable report file field"
        );
        assert!(
            markdown.contains("product-quickstart --skip-typescript"),
            "{name} should document the reduced product quickstart fallback command"
        );
        assert!(
            markdown.contains("typescript_enabled"),
            "{name} should document the product quickstart TypeScript-enabled receipt field"
        );
        assert!(
            markdown.contains("reduced local evidence path"),
            "{name} should call out the skip-TypeScript fallback as reduced evidence"
        );
        assert!(
            markdown.contains("product-quickstart --inject-failure embedded_demo"),
            "{name} should document the product quickstart failure receipt check"
        );
        assert!(
            markdown.contains("conflicts with --skip-typescript"),
            "{name} should document TypeScript-only selector skip conflicts"
        );
        assert!(
            markdown.contains("--only embedded_demo"),
            "{name} should document the first supported product regression single-step mode"
        );
        assert!(
            markdown.contains("--only embedded_verify"),
            "{name} should document dependency-aware embedded verify targeted execution"
        );
        assert!(
            markdown.contains("--only http_demo"),
            "{name} should document targeted local HTTP demo execution"
        );
        assert!(
            markdown.contains("--only local_doctor"),
            "{name} should document targeted local endpoint diagnostics"
        );
        assert!(
            markdown.contains("--only rust_sdk_quickstart"),
            "{name} should document targeted Rust SDK quickstart execution"
        );
        assert!(
            markdown.contains("--only python_sdk_smoke"),
            "{name} should document targeted Python SDK smoke execution"
        );
        assert!(
            markdown.contains("--only typescript_check"),
            "{name} should document targeted generated TypeScript typecheck execution"
        );
        assert!(
            markdown.contains("--only typescript_http_smoke"),
            "{name} should document targeted generated TypeScript HTTP smoke execution"
        );
        assert!(
            markdown.contains("--only typescript_gateway_smoke"),
            "{name} should document targeted public TypeScript SDK gateway smoke execution"
        );
    }
}

#[test]
fn platform_contract_v0_declares_sdk_conformance_harness() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let doc_path = root.join("docs/platform-contract-v0.md");
    let manifest_path = root.join("docs/platform-contract-v0.json");
    let markdown = std::fs::read_to_string(&doc_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", doc_path.display()));
    let manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(&manifest_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", manifest_path.display())),
    )
    .expect("parse Platform Contract v0 manifest");

    assert_eq!(
        manifest["contract"],
        json!("tracedb-platform-contract-v0"),
        "manifest should name the canonical platform contract"
    );
    assert_eq!(
        manifest["status"],
        json!("contract-freeze-draft"),
        "manifest should be explicit that v0 is a freeze draft, not a managed SLA"
    );
    assert_eq!(
        manifest["sql_compatibility"],
        json!("not_implemented"),
        "manifest must preserve the SQL status guard"
    );

    let model_components = manifest["developer_model"]
        .as_array()
        .expect("developer_model array")
        .iter()
        .filter_map(|value| value.as_str())
        .collect::<BTreeSet<_>>();
    for component in [
        "connection_config",
        "database_branch_config",
        "table_handles",
        "schema_migrations",
        "record_writes",
        "batch_ingest",
        "query_builder",
        "traceql_string_execution",
        "result_envelope",
        "explain_provenance_freshness_jobs",
        "errors_retries_idempotency",
        "pagination_cursors",
        "admin_compact_snapshot_restore",
    ] {
        assert!(
            model_components.contains(component),
            "Platform Contract v0 manifest missing developer model component {component}"
        );
        assert!(
            markdown.contains(component),
            "Platform Contract v0 markdown missing developer model component {component}"
        );
    }

    let surface_ids = manifest["surfaces"]
        .as_array()
        .expect("surfaces array")
        .iter()
        .filter_map(|surface| surface["id"].as_str())
        .collect::<BTreeSet<_>>();
    for surface in [
        "http_direct",
        "rust_sdk",
        "typescript_sdk",
        "python_sdk",
        "traceql_sqlish",
        "graphql",
    ] {
        assert!(
            surface_ids.contains(surface),
            "Platform Contract v0 manifest missing conformance surface {surface}"
        );
        assert!(
            markdown.contains(surface),
            "Platform Contract v0 markdown missing conformance surface {surface}"
        );
    }

    let scenario_ids = manifest["conformance_scenarios"]
        .as_array()
        .expect("conformance_scenarios array")
        .iter()
        .filter_map(|scenario| scenario["id"].as_str())
        .collect::<BTreeSet<_>>();
    for scenario in [
        "schema_apply",
        "put",
        "batch",
        "patch",
        "get",
        "scan",
        "query",
        "traceql_string_execution",
        "explain",
        "delete",
        "idempotency",
        "errors",
        "snapshot_restore",
    ] {
        assert!(
            scenario_ids.contains(scenario),
            "Platform Contract v0 manifest missing conformance scenario {scenario}"
        );
        assert!(
            markdown.contains(scenario),
            "Platform Contract v0 markdown missing conformance scenario {scenario}"
        );
    }

    for boundary in [
        "SQL compatibility is not implemented",
        "not PostgreSQL-compatible",
        "same behavior, same errors, same result shape",
        "docs/api/v1-http.md",
        "docs/api/v1-openapi.json",
        "scripts/platform_conformance.py",
    ] {
        assert!(
            markdown.contains(boundary),
            "Platform Contract v0 markdown missing boundary: {boundary}"
        );
    }

    let python_surface = manifest["surfaces"]
        .as_array()
        .expect("surfaces array")
        .iter()
        .find(|surface| surface["id"] == json!("python_sdk"))
        .expect("python_sdk surface");
    let python_evidence = python_surface["evidence"]
        .as_array()
        .expect("python_sdk evidence array")
        .iter()
        .filter_map(|value| value.as_str())
        .collect::<BTreeSet<_>>();
    for evidence in [
        "clients/python/install_smoke.py",
        "python3 clients/python/install_smoke.py",
        "TraceDB.graphql",
        "graphql_request",
    ] {
        assert!(
            python_evidence.contains(evidence),
            "python_sdk manifest evidence missing {evidence}"
        );
        assert!(
            markdown.contains(evidence),
            "Platform Contract v0 markdown missing Python install evidence {evidence}"
        );
    }
    let python_install_smoke =
        std::fs::read_to_string(root.join("clients/python/install_smoke.py"))
            .expect("read Python install smoke");
    for token in [
        "venv.EnvBuilder",
        "pip",
        "--no-deps",
        "--target",
        "TraceDB.from_env",
        "graphql_request",
        "python sdk install smoke ok",
    ] {
        assert!(
            python_install_smoke.contains(token),
            "Python install smoke should contain {token}"
        );
    }

    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README");
    let docs_readme =
        std::fs::read_to_string(root.join("docs/README.md")).expect("read docs README");
    for (name, source) in [("README", readme), ("docs README", docs_readme)] {
        assert!(
            source.contains("docs/platform-contract-v0.md"),
            "{name} should link to Platform Contract v0 markdown"
        );
        assert!(
            source.contains("docs/platform-contract-v0.json"),
            "{name} should link to Platform Contract v0 manifest"
        );
        assert!(
            source.contains("scripts/platform_conformance.py"),
            "{name} should link to the Platform Contract v0 conformance runner"
        );
    }
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
    assert_eq!(
        spec["components"]["schemas"]["ErrorResponse"]["properties"]["code"]["type"],
        json!("string"),
        "OpenAPI ErrorResponse should expose optional machine-readable code"
    );
    let description = spec["info"]["description"]
        .as_str()
        .expect("OpenAPI info.description");
    for boundary in [
        "SQL compatibility is not implemented",
        "Internal TraceDB-only runs are development evidence",
        "Idempotency-Key supports local data-dir-backed replay for mutation and admin routes",
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
        ("post", "/v1/traceql"),
        ("post", "/v1/graphql"),
        ("get", "/v1/graphql/schema"),
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
        assert!(
            operation["x-tracedb-sdk-idempotency-retry-supported"].is_boolean(),
            "OpenAPI artifact missing SDK idempotency retry marker for {method} {path}"
        );
        if operation["x-tracedb-mutates-state"] == json!(true) {
            assert!(
                operation["responses"]["409"]["content"]["application/json"]["schema"].is_object(),
                "OpenAPI artifact missing JSON 409 idempotency conflict for {method} {path}"
            );
            let parameters = operation["parameters"].as_array().unwrap_or_else(|| {
                panic!("OpenAPI artifact missing parameters for {method} {path}")
            });
            assert!(
                parameters.iter().any(|parameter| {
                    parameter["name"] == json!("Idempotency-Key")
                        && parameter["in"] == json!("header")
                        && parameter["required"] == json!(false)
                }),
                "OpenAPI artifact missing Idempotency-Key header for {method} {path}"
            );
            assert_eq!(
                operation["x-tracedb-idempotency-key-supported"],
                json!(true),
                "OpenAPI artifact missing idempotency marker for {method} {path}"
            );
            assert_eq!(
                operation["x-tracedb-idempotency-durability"],
                json!("local-data-dir-reopen"),
                "OpenAPI artifact should state local-only idempotency durability for {method} {path}"
            );
            assert_eq!(
                operation["x-tracedb-sdk-idempotency-retry-supported"],
                json!(true),
                "OpenAPI artifact should mark idempotency retry eligibility for {method} {path}"
            );
        }
    }

    for schema_name in [
        "TableSchema",
        "RecordInput",
        "RecordOutput",
        "RecordPutBody",
        "RecordGetRequest",
        "RecordScanRequest",
        "RecordScanOutput",
        "RecordPatchRequest",
        "RecordDeleteRequest",
        "RecordPutBatchRequest",
        "HybridQuery",
        "GraphQlSchemaResponse",
        "HybridScoreComponents",
        "HealthResponse",
        "ReadyResponse",
        "DatabaseSummary",
        "DatabasesResponse",
        "BranchSummary",
        "BranchesResponse",
        "MetricsResponse",
        "AdminJob",
        "JobsResponse",
        "HybridQueryRow",
        "AccessPathExplain",
        "Candidate",
        "QueryPhaseTiming",
        "AccessPathTiming",
        "HybridExplain",
    ] {
        assert!(
            spec["components"]["schemas"][schema_name].is_object(),
            "OpenAPI artifact missing component schema {schema_name}"
        );
    }
    let put_schema = &spec["paths"]["/v1/records/put"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert_eq!(
        put_schema["$ref"],
        json!("#/components/schemas/RecordPutBody"),
        "OpenAPI artifact should expose the server's direct-or-wrapper put body"
    );
    assert!(
        spec["components"]["schemas"]["RecordPutBody"]["oneOf"]
            .as_array()
            .is_some_and(|schemas| {
                schemas
                    .iter()
                    .any(|schema| schema["$ref"] == json!("#/components/schemas/RecordInput"))
                    && schemas.iter().any(|schema| {
                        schema["$ref"] == json!("#/components/schemas/RecordPutRequest")
                    })
            }),
        "RecordPutBody should allow direct RecordInput and wrapper RecordPutRequest"
    );
    assert!(
        spec["components"]["schemas"]["GetRecordResponse"]["properties"]["record"]["oneOf"]
            .as_array()
            .is_some_and(|schemas| {
                schemas
                    .iter()
                    .any(|schema| schema["$ref"] == json!("#/components/schemas/RecordOutput"))
                    && schemas.iter().any(|schema| schema["type"] == json!("null"))
            }),
        "GetRecordResponse.record should reference RecordOutput or null"
    );
    assert_eq!(
        spec["components"]["schemas"]["RecordOutput"]["properties"]["version_id"]["type"],
        json!("integer"),
        "RecordOutput should expose the server's serialized version_id field"
    );
    assert!(
        spec["components"]["schemas"]["RecordOutput"]["properties"]["version"].is_null(),
        "RecordOutput should not document a non-serialized version field"
    );
    assert_eq!(
        spec["components"]["schemas"]["RecordScanOutput"]["properties"]["records"]["items"]["$ref"],
        json!("#/components/schemas/RecordOutput"),
        "RecordScanOutput.records should reference RecordOutput"
    );
    assert_eq!(
        spec["components"]["schemas"]["RecordScanOutput"]["properties"]["returned_count"]["type"],
        json!("integer"),
        "RecordScanOutput should expose returned_count"
    );
    assert_eq!(
        spec["components"]["schemas"]["HybridQuery"]["properties"]["scalar_eq"]["type"],
        json!("object"),
        "HybridQuery should expose scalar_eq predicates"
    );
    assert_eq!(
        spec["components"]["schemas"]["HybridQuery"]["properties"]["graph_seed"]["type"],
        json!(["string", "null"]),
        "HybridQuery should expose graph_seed"
    );
    assert_eq!(
        spec["components"]["schemas"]["HybridQuery"]["properties"]["temporal_as_of"]["type"],
        json!(["integer", "null"]),
        "HybridQuery should expose temporal_as_of"
    );
    assert_eq!(
        spec["components"]["schemas"]["HybridQueryRow"]["properties"]["version_id"]["type"],
        json!("integer"),
        "HybridQueryRow should expose version_id"
    );
    assert_eq!(
        spec["components"]["schemas"]["HybridQueryRow"]["properties"]["score"]["$ref"],
        json!("#/components/schemas/HybridScoreComponents"),
        "HybridQueryRow.score should reference HybridScoreComponents"
    );
    assert_eq!(
        spec["components"]["schemas"]["QueryResponse"]["properties"]["results"]["items"]["$ref"],
        json!("#/components/schemas/HybridQueryRow"),
        "QueryResponse.results should reference HybridQueryRow"
    );
    assert_eq!(
        spec["components"]["schemas"]["QueryResponse"]["properties"]["explain"]["$ref"],
        json!("#/components/schemas/HybridExplain"),
        "QueryResponse.explain should reference HybridExplain"
    );
    assert_eq!(
        spec["components"]["schemas"]["HybridExplain"]["properties"]["access_paths"]["items"]
            ["$ref"],
        json!("#/components/schemas/AccessPathExplain"),
        "HybridExplain.access_paths should reference AccessPathExplain"
    );
    assert_eq!(
        spec["components"]["schemas"]["HybridExplain"]["properties"]["planner_candidates"]["items"]
            ["$ref"],
        json!("#/components/schemas/Candidate"),
        "HybridExplain.planner_candidates should reference Candidate"
    );
    assert_eq!(
        spec["components"]["schemas"]["HybridExplain"]["properties"]["phase_timings"]["items"]
            ["$ref"],
        json!("#/components/schemas/QueryPhaseTiming"),
        "HybridExplain.phase_timings should reference QueryPhaseTiming"
    );
    assert_eq!(
        spec["components"]["schemas"]["HybridExplain"]["properties"]["access_path_timings"]
            ["items"]["$ref"],
        json!("#/components/schemas/AccessPathTiming"),
        "HybridExplain.access_path_timings should reference AccessPathTiming"
    );
    assert_eq!(
        spec["components"]["schemas"]["HealthResponse"]["properties"]["ok"]["type"],
        json!("boolean"),
        "HealthResponse should expose ok"
    );
    assert_eq!(
        spec["components"]["schemas"]["HealthResponse"]["properties"].get("data_dir_available"),
        None,
        "HealthResponse should not advertise legacy /health-only detail fields for /v1/health"
    );
    assert_eq!(
        spec["components"]["schemas"]["ReadyResponse"]["properties"]["ready"]["type"],
        json!("boolean"),
        "ReadyResponse should expose ready"
    );
    assert_eq!(
        spec["components"]["schemas"]["DatabasesResponse"]["properties"]["databases"]["items"]
            ["$ref"],
        json!("#/components/schemas/DatabaseSummary"),
        "DatabasesResponse.databases should reference DatabaseSummary"
    );
    assert_eq!(
        spec["components"]["schemas"]["DatabaseSummary"]["properties"]["database_id"]["type"],
        json!("string"),
        "DatabaseSummary should expose database_id"
    );
    assert_eq!(
        spec["components"]["schemas"]["BranchesResponse"]["properties"]["branches"]["items"]
            ["$ref"],
        json!("#/components/schemas/BranchSummary"),
        "BranchesResponse.branches should reference BranchSummary"
    );
    assert_eq!(
        spec["components"]["schemas"]["BranchSummary"]["properties"]["branch_id"]["type"],
        json!("string"),
        "BranchSummary should expose branch_id"
    );
    assert_eq!(
        spec["components"]["schemas"]["MetricsResponse"]["properties"]["latest_epoch"]["type"],
        json!("integer"),
        "MetricsResponse should expose latest_epoch"
    );
    assert_eq!(
        spec["components"]["schemas"]["JobsResponse"]["properties"]["jobs"]["items"]["$ref"],
        json!("#/components/schemas/AdminJob"),
        "JobsResponse.jobs should reference AdminJob"
    );
    assert_eq!(
        spec["components"]["schemas"]["AdminJob"]["properties"]["queue"]["type"],
        json!("string"),
        "AdminJob should expose queue"
    );

    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README");
    assert!(
        readme.contains("docs/api/v1-openapi.json"),
        "README should link to the generated OpenAPI artifact"
    );
}

#[test]
fn generated_typescript_client_artifact_tracks_openapi_routes() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let generator = root.join("scripts/generate_typescript_client.py");
    let client = root.join("clients/typescript/src/client.ts");

    let check = Command::new("python3")
        .arg(&generator)
        .arg("--check")
        .current_dir(root)
        .output()
        .unwrap_or_else(|error| panic!("run {} --check: {error}", generator.display()));
    assert!(
        check.status.success(),
        "TypeScript client artifact should be reproducible from generator\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&check.stdout),
        String::from_utf8_lossy(&check.stderr)
    );

    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("docs/api/v1-openapi.json"))
            .expect("read OpenAPI artifact"),
    )
    .expect("parse OpenAPI JSON");
    let source = std::fs::read_to_string(&client)
        .unwrap_or_else(|error| panic!("read {}: {error}", client.display()));

    for token in [
        "export class TraceDbClient",
        "export type TraceDbRequestOptions",
        "export class TraceDbHttpError",
        "readonly responseJson?: JsonValue;",
        "readonly errorResponse?: ErrorResponse;",
        "readonly responseError?: string;",
        "readonly responseCode?: string;",
        "code?: string;",
        "return typeof code === \"string\" ? { error, code } : { error };",
        "export class TraceDbRequestError",
        "Generated schema aliases keep OpenAPI's permissive additionalProperties boundary",
        "export interface TableSchema extends JsonObject",
        "export interface RecordInput extends JsonObject",
        "export interface HealthResponse extends JsonObject",
        "export interface ReadyResponse extends JsonObject",
        "export interface DatabaseSummary extends JsonObject",
        "export interface DatabasesResponse extends JsonObject",
        "export interface BranchSummary extends JsonObject",
        "export interface BranchesResponse extends JsonObject",
        "export interface MetricsResponse extends JsonObject",
        "export interface AdminJob extends JsonObject",
        "export interface JobsResponse extends JsonObject",
        "export type RecordPutBody = RecordInput | RecordPutRequest;",
        "export interface RecordPutBatchRequest extends JsonObject",
        "export interface HybridScoreComponents extends JsonObject",
        "export interface AccessPathExplain extends JsonObject",
        "export interface Candidate extends JsonObject",
        "export interface QueryPhaseTiming extends JsonObject",
        "export interface AccessPathTiming extends JsonObject",
        "record?: RecordOutput | null;",
        "version_id?: number;",
        "export interface HybridQuery extends JsonObject",
        "scalar_eq?: JsonObject;",
        "graph_seed?: string | null;",
        "temporal_as_of?: number | null;",
        "export interface QueryResponse extends JsonObject",
        "export interface GraphQlSchemaResponse extends JsonObject",
        "export interface SnapshotRequest extends JsonObject",
        "export interface RestoreResponse extends JsonObject",
        "name?: string;",
        "records?: RecordInput[];",
        "records?: RecordOutput[];",
        "results?: HybridQueryRow[];",
        "score?: HybridScoreComponents;",
        "planner_candidates?: Candidate[];",
        "phase_timings?: QueryPhaseTiming[];",
        "databases?: DatabaseSummary[];",
        "branches?: BranchSummary[];",
        "jobs?: AdminJob[];",
        "vector?: number[] | null;",
        "record_count?: number;",
        "source?: string;",
        "Idempotency-Key",
        "SQL compatibility is not implemented.",
        "idempotency key must be non-empty and must not contain CR or LF",
        "key.includes(\"\\r\")",
        "key.includes(\"\\n\")",
        "database_id",
        "branch_id",
        "if (method !== \"GET\")",
        "const routed: JsonObject = { ...body };",
        "routed.database_id === undefined",
        "routed.branch_id === undefined",
    ] {
        assert!(
            source.contains(token),
            "generated TypeScript client missing {token}"
        );
    }

    for method_name in [
        "health",
        "ready",
        "listDatabases",
        "listBranches",
        "publicSafeMetrics",
        "applySchema",
        "insert",
        "putRecord",
        "putBatch",
        "patchRecord",
        "deleteRecord",
        "getRecord",
        "scanRecords",
        "query",
        "graphqlSchema",
        "explain",
        "compact",
        "snapshot",
        "restore",
        "listAdminJobs",
    ] {
        assert!(
            source.contains(&format!("async {method_name}(")),
            "generated TypeScript client missing method {method_name}"
        );
    }
    for signature in [
        "async health(options: TraceDbRequestOptions = {}): Promise<HealthResponse>",
        "async ready(options: TraceDbRequestOptions = {}): Promise<ReadyResponse>",
        "async listDatabases(options: TraceDbRequestOptions = {}): Promise<DatabasesResponse>",
        "async listBranches(options: TraceDbRequestOptions = {}): Promise<BranchesResponse>",
        "async publicSafeMetrics(options: TraceDbRequestOptions = {}): Promise<MetricsResponse>",
        "async applySchema(body: TableSchema, options: TraceDbRequestOptions = {}): Promise<EpochResponse>",
        "async putRecord(body: RecordPutBody, options: TraceDbRequestOptions = {}): Promise<EpochResponse>",
        "async putBatch(body: RecordPutBatchRequest, options: TraceDbRequestOptions = {}): Promise<PutBatchResponse>",
        "async query(body: HybridQuery, options: TraceDbRequestOptions = {}): Promise<QueryResponse>",
        "async graphqlSchema(options: TraceDbRequestOptions = {}): Promise<GraphQlSchemaResponse>",
        "async snapshot(body: SnapshotRequest, options: TraceDbRequestOptions = {}): Promise<SnapshotResponse>",
        "async listAdminJobs(options: TraceDbRequestOptions = {}): Promise<JobsResponse>",
        "private async request<TResponse extends JsonValue>",
    ] {
        assert!(
            source.contains(signature),
            "generated TypeScript client missing typed signature {signature}"
        );
    }

    let paths = spec["paths"].as_object().expect("OpenAPI paths object");
    for (path, methods) in paths {
        let methods = methods.as_object().expect("OpenAPI path methods object");
        for (method, operation) in methods {
            let operation_id = operation["operationId"]
                .as_str()
                .unwrap_or_else(|| panic!("OpenAPI operationId for {method} {path}"));
            let method_literal = method.to_ascii_uppercase();
            assert!(
                source.contains(&format!("\"{method_literal}\", \"{path}\"")),
                "generated TypeScript client missing {method_literal} {path} for {operation_id}"
            );
            assert!(
                source.contains(&format!("// {operation_id}: {method_literal} {path}")),
                "generated TypeScript client missing provenance comment for {operation_id}"
            );
        }
    }

    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README");
    assert!(
        readme.contains("clients/typescript/src/client.ts"),
        "README should link to the generated TypeScript client artifact"
    );
}

#[test]
fn generated_typescript_client_smoke_executes_in_node_runtime() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let smoke = root.join("clients/typescript/smoke.ts");

    let check = Command::new("node")
        .arg("--experimental-strip-types")
        .arg(&smoke)
        .current_dir(root)
        .output()
        .unwrap_or_else(|error| panic!("run Node TypeScript smoke: {error}"));
    assert!(
        check.status.success(),
        "generated TypeScript client smoke should execute in Node\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&check.stdout),
        String::from_utf8_lossy(&check.stderr)
    );
    assert!(
        String::from_utf8_lossy(&check.stdout).contains("typescript client smoke ok"),
        "Node smoke should print success sentinel\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&check.stdout),
        String::from_utf8_lossy(&check.stderr)
    );
}

#[test]
fn typescript_sdk_package_declares_public_entrypoint_boundary() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let package_json = root.join("clients/typescript/package.json");
    let package_lock_json = root.join("clients/typescript/package-lock.json");
    let tsconfig_json = root.join("clients/typescript/tsconfig.json");
    let tsconfig_build_json = root.join("clients/typescript/tsconfig.build.json");

    let package: Value = serde_json::from_str(
        &std::fs::read_to_string(&package_json)
            .unwrap_or_else(|error| panic!("read {}: {error}", package_json.display())),
    )
    .expect("parse TypeScript client package.json");
    assert_eq!(package["name"], json!("@tracedb/sdk"));
    assert_eq!(
        package["description"],
        json!("TraceDB public TypeScript SDK over the generated HTTP transport.")
    );
    assert_eq!(package["private"], json!(false));
    assert_eq!(package["type"], json!("module"));
    assert_eq!(package["license"], json!("MIT"));
    assert_eq!(package["sideEffects"], json!(false));
    assert_eq!(package["main"], json!("./dist/index.js"));
    assert_eq!(package["types"], json!("./dist/index.d.ts"));
    assert_eq!(package["files"], json!(["dist", "README.md"]));
    assert_eq!(package["publishConfig"]["access"], json!("public"));
    assert_eq!(package["exports"]["."]["types"], json!("./dist/index.d.ts"));
    assert_eq!(package["exports"]["."]["default"], json!("./dist/index.js"));
    assert_eq!(
        package["exports"]["./transport"]["types"],
        json!("./dist/client.d.ts")
    );
    assert_eq!(
        package["exports"]["./transport"]["default"],
        json!("./dist/client.js")
    );
    assert_eq!(
        package["scripts"]["build"],
        json!(
            "node scripts/clean-dist.mjs && tsc -p tsconfig.build.json && node scripts/rewrite-declaration-imports.mjs"
        )
    );
    assert_eq!(
        package["scripts"]["typecheck"],
        json!("tsc --noEmit -p tsconfig.json")
    );
    assert_eq!(
        package["scripts"]["smoke"],
        json!("node --experimental-strip-types smoke.ts")
    );
    assert_eq!(
        package["scripts"]["public-smoke"],
        json!("node --experimental-strip-types public-sdk-smoke.ts")
    );
    assert_eq!(
        package["scripts"]["package-smoke"],
        json!(
            "node --experimental-strip-types package-entry-smoke.ts && node --experimental-strip-types build-package-smoke.ts"
        )
    );
    assert_eq!(
        package["scripts"]["pack-dry-run"],
        json!("npm pack --dry-run --json")
    );
    assert_eq!(
        package["scripts"]["consumer-smoke"],
        json!("node scripts/packed-consumer-smoke.mjs")
    );
    assert_eq!(
        package["scripts"]["http-smoke"],
        json!("node --experimental-strip-types http-smoke.ts")
    );
    assert_eq!(
        package["scripts"]["public-http-smoke"],
        json!("node --experimental-strip-types public-http-smoke.ts")
    );
    assert_eq!(
        package["scripts"]["quickstart"],
        json!("node --experimental-strip-types quickstart.ts")
    );
    assert_eq!(
        package["scripts"]["gateway-smoke"],
        json!("node --experimental-strip-types gateway-smoke.ts")
    );
    assert_eq!(
        package["scripts"]["check"],
        json!(
            "npm run typecheck && npm run smoke && npm run public-smoke && npm run build && npm run package-smoke && npm run pack-dry-run && npm run consumer-smoke"
        )
    );
    assert_eq!(package["devDependencies"]["typescript"], json!("6.0.3"));
    assert_eq!(package["devDependencies"]["@types/node"], json!("25.9.0"));

    let package_lock: Value = serde_json::from_str(
        &std::fs::read_to_string(&package_lock_json)
            .unwrap_or_else(|error| panic!("read {}: {error}", package_lock_json.display())),
    )
    .expect("parse TypeScript client package-lock.json");
    assert_eq!(package_lock["lockfileVersion"], json!(3));
    assert_eq!(package_lock["name"], json!("@tracedb/sdk"));
    assert_eq!(package_lock["packages"][""]["name"], json!("@tracedb/sdk"));
    assert_eq!(package_lock["packages"][""]["license"], json!("MIT"));
    assert_eq!(
        package_lock["packages"][""]["devDependencies"]["typescript"],
        json!("6.0.3")
    );
    assert_eq!(
        package_lock["packages"][""]["devDependencies"]["@types/node"],
        json!("25.9.0")
    );

    let tsconfig: Value = serde_json::from_str(
        &std::fs::read_to_string(&tsconfig_json)
            .unwrap_or_else(|error| panic!("read {}: {error}", tsconfig_json.display())),
    )
    .expect("parse TypeScript client tsconfig.json");
    assert_eq!(tsconfig["compilerOptions"]["module"], json!("NodeNext"));
    assert_eq!(
        tsconfig["compilerOptions"]["moduleResolution"],
        json!("NodeNext")
    );
    assert_eq!(tsconfig["compilerOptions"]["strict"], json!(true));
    assert_eq!(tsconfig["compilerOptions"]["noEmit"], json!(true));
    assert_eq!(
        tsconfig["compilerOptions"]["allowImportingTsExtensions"],
        json!(true)
    );
    assert_eq!(
        tsconfig["include"],
        json!([
            "src/index.ts",
            "src/client.ts",
            "src/sdk.ts",
            "smoke.ts",
            "public-sdk-smoke.ts",
            "http-smoke.ts",
            "public-http-smoke.ts",
            "quickstart.ts",
            "gateway-smoke.ts"
        ])
    );

    let tsconfig_build: Value = serde_json::from_str(
        &std::fs::read_to_string(&tsconfig_build_json)
            .unwrap_or_else(|error| panic!("read {}: {error}", tsconfig_build_json.display())),
    )
    .expect("parse TypeScript client tsconfig.build.json");
    assert_eq!(tsconfig_build["extends"], json!("./tsconfig.json"));
    assert_eq!(
        tsconfig_build["compilerOptions"]["allowImportingTsExtensions"],
        json!(false)
    );
    assert_eq!(
        tsconfig_build["compilerOptions"]["declaration"],
        json!(true)
    );
    assert_eq!(tsconfig_build["compilerOptions"]["noEmit"], json!(false));
    assert_eq!(tsconfig_build["compilerOptions"]["outDir"], json!("./dist"));
    assert_eq!(tsconfig_build["compilerOptions"]["rootDir"], json!("./src"));
    assert_eq!(
        tsconfig_build["compilerOptions"]["rewriteRelativeImportExtensions"],
        json!(true)
    );
    assert_eq!(
        tsconfig_build["include"],
        json!(["src/index.ts", "src/client.ts", "src/sdk.ts"])
    );

    let public_sdk = std::fs::read_to_string(root.join("clients/typescript/src/sdk.ts"))
        .expect("read TypeScript public SDK wrapper");
    for token in [
        "export class TraceDB",
        "table(name: string): TraceDBTable",
        "export class TraceDBTable",
        "insertRows",
        "insertBatch",
        "patch",
        "explainPlan",
        "compact",
        "snapshot",
        "restore",
        "listAdminJobs",
        "RecordPutBatchRequest",
        "graphql(query: string",
        "graphqlRequest",
        "where({ tenant_id })",
        "TraceDbClient",
    ] {
        assert!(
            public_sdk.contains(token),
            "TypeScript public SDK wrapper should include {token}"
        );
    }

    let build_smoke =
        std::fs::read_to_string(root.join("clients/typescript/build-package-smoke.ts"))
            .expect("read TypeScript build package smoke");
    for token in [
        "dist/index.js",
        "dist/index.d.ts",
        "dist/client.js",
        "dist/client.d.ts",
        "dist/sdk.js",
        "dist/sdk.d.ts",
        "@tracedb/sdk",
        "@tracedb/sdk/transport",
        "typescript build package smoke ok",
    ] {
        assert!(
            build_smoke.contains(token),
            "TypeScript build package smoke should include {token}"
        );
    }

    let rewrite_declarations = std::fs::read_to_string(
        root.join("clients/typescript/scripts/rewrite-declaration-imports.mjs"),
    )
    .expect("read TypeScript declaration rewrite script");
    for token in [".ts\\\"", ".js\\\"", "dist"] {
        assert!(
            rewrite_declarations.contains(token),
            "TypeScript declaration rewrite script should include {token}"
        );
    }

    let consumer_smoke =
        std::fs::read_to_string(root.join("clients/typescript/scripts/packed-consumer-smoke.mjs"))
            .expect("read TypeScript packed consumer smoke");
    for token in [
        "mkdtempSync",
        "npm",
        "pack",
        "--pack-destination",
        "npm",
        "install",
        "@tracedb/sdk",
        "@tracedb/sdk/transport",
        "node_modules/@tracedb/sdk/dist/index.js",
        "typescript packed consumer smoke ok",
    ] {
        assert!(
            consumer_smoke.contains(token),
            "TypeScript packed consumer smoke should include {token}"
        );
    }

    let public_smoke = std::fs::read_to_string(root.join("clients/typescript/public-sdk-smoke.ts"))
        .expect("read TypeScript public SDK smoke");
    for token in [
        "typescript public sdk smoke ok",
        "new TraceDB",
        ".table(\"docs\")",
        ".insertRows",
        ".insertBatch",
        ".patch",
        ".explainPlan",
        "db.compact",
        "db.snapshot",
        "db.restore",
        ".where({ tenant_id",
        ".match(\"body\"",
        ".near(\"embedding\"",
        ".with({ explain: true, freshness: \"lazy\" })",
        "TRACEDB_SAFE_RETRIES should retry bounded GraphQL",
        "db.graphql",
        "db.graphqlRequest",
        "TraceDbRequestError",
    ] {
        assert!(
            public_smoke.contains(token),
            "TypeScript public SDK smoke should include {token}"
        );
    }

    let public_http_smoke =
        std::fs::read_to_string(root.join("clients/typescript/public-http-smoke.ts"))
            .expect("read TypeScript public SDK HTTP smoke");
    for token in [
        "typescript public sdk http smoke ok",
        "new TraceDB",
        "await db.applySchema",
        "await docs.insert",
        "await docs.insertRows",
        "await docs.insertBatch",
        "await docs.patch",
        "await docs.get",
        "await docs.limit(10).scan",
        "await db.graphql",
        "graphql_query_execution",
        ".explainPlan()",
        "await db.compact",
        "await db.snapshot",
        "await db.restore",
        "local-http-typescript-public-sdk-smoke",
        "sdk_surface: \"public\"",
        "sql_module: \"not_implemented\"",
    ] {
        assert!(
            public_http_smoke.contains(token),
            "TypeScript public SDK HTTP smoke should include {token}"
        );
    }

    let http_smoke = std::fs::read_to_string(root.join("clients/typescript/http-smoke.ts"))
        .expect("read TypeScript HTTP smoke");
    for token in [
        "typescript client http smoke ok",
        "spawn(\"cargo\"",
        "TRACEDB_DATA_DIR",
        "TRACEDB_BIND",
        "await client.ready()",
        "await client.applySchema",
        "await client.putBatch",
        "await client.query",
        "await client.snapshot",
        "sql_module: \"not_implemented\"",
    ] {
        assert!(
            http_smoke.contains(token),
            "TypeScript HTTP smoke should include {token}"
        );
    }

    let quickstart = std::fs::read_to_string(root.join("clients/typescript/quickstart.ts"))
        .expect("read TypeScript endpoint quickstart");
    for token in [
        "typescript client endpoint quickstart ok",
        "TRACEDB_URL",
        "TRACEDB_TOKEN",
        "TRACEDB_DATABASE_ID",
        "TRACEDB_BRANCH_ID",
        "TRACEDB_ADMIN_DIR",
        "await client.ready()",
        "await client.applySchema",
        "await client.putBatch",
        "await client.query",
        "await client.explain",
        "await client.deleteRecord",
        "sql_module: \"not_implemented\"",
    ] {
        assert!(
            quickstart.contains(token),
            "TypeScript endpoint quickstart should include {token}"
        );
    }

    let gateway_smoke = std::fs::read_to_string(root.join("clients/typescript/gateway-smoke.ts"))
        .expect("read TypeScript gateway smoke");
    for token in [
        "typescript public sdk gateway smoke ok",
        "new TraceDB",
        "await db.applySchema",
        "await docs.insert",
        "await docs.insertBatch",
        "await docs.patch",
        "await docs.get",
        "await docs.limit(10).scan",
        ".explainPlan()",
        "TRACEDB_SERVICE_MODE",
        "gateway",
        "TRACEDB_REQUIRE_API_KEY",
        "TRACEDB_API_TOKEN",
        "TRACEDB_ENGINE_URL",
        "databaseId",
        "branchId",
        "TraceDbHttpError",
        "invalid api token",
        "unknown branch db_missing:main",
        "local-gateway-typescript-public-sdk-smoke",
        "db.traceql",
        "MATCH body \"TypeScript public SDK\"",
        "db.graphqlSchema",
        "db.graphql",
        "sdk_surface: \"public\"",
        "sql_module: \"not_implemented\"",
    ] {
        assert!(
            gateway_smoke.contains(token),
            "TypeScript gateway smoke should include {token}"
        );
    }

    let readme = std::fs::read_to_string(root.join("clients/typescript/README.md"))
        .expect("read TypeScript client README");
    for command in [
        "npm ci",
        "npm run typecheck",
        "npm run smoke",
        "npm run public-smoke",
        "npm run build",
        "npm run package-smoke",
        "npm run pack-dry-run",
        "npm run consumer-smoke",
        "npm run http-smoke",
        "npm run public-http-smoke",
        "npm run quickstart",
        "npm run gateway-smoke",
        "TRACEDB_URL",
        "TRACEDB_ADMIN_DIR",
        "TRACEDB_REQUIRE_API_KEY",
        "TRACEDB_DATABASE_ID",
        "TraceDbRequestError",
        "CR/LF-containing idempotency keys",
        "RecordScanOutput.records",
        "QueryResponse.results",
        "HybridScoreComponents",
        "TraceDB",
        "insertRows",
        "insertBatch",
        "dist/index.js",
        "dist/index.d.ts",
        "not a package publishing pipeline",
    ] {
        assert!(
            readme.contains(command),
            "TypeScript client README should document {command}"
        );
    }
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
    http_response_with_headers(addr, method, path, &[], body)
}

fn http_response_with_headers(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> String {
    let mut stream = connect_with_retry(addr);
    let extra_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n{extra_headers}Content-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn connect_with_retry(addr: std::net::SocketAddr) -> TcpStream {
    let mut last_error = None;
    for _ in 0..50 {
        match TcpStream::connect(addr) {
            Ok(stream) => return stream,
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    panic!(
        "connect to {addr} after retries: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "no connection attempt made".to_string())
    );
}

fn start_http_test_server(data_dir: PathBuf) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve_listener(data_dir, listener);
    });
    wait_for_http_ready(addr);
    addr
}

fn wait_for_http_ready(addr: SocketAddr) {
    let url = format!("http://{addr}");
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url.clone(), "dev-token")
            .with_timeout(Duration::from_millis(100)),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_error = None;
    while Instant::now() < deadline {
        match client.ready_typed() {
            Ok(response) if response.ready => return,
            Ok(response) => {
                last_error = Some(format!("ready endpoint returned not-ready: {response:?}"));
            }
            Err(error) => {
                last_error = Some(error.to_string());
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "TraceDB HTTP acceptance test server did not become ready at {url}; last error: {}",
        last_error.unwrap_or_else(|| "no readiness attempt completed".to_string())
    );
}

fn start_stoppable_server(
    data_dir: PathBuf,
) -> (SocketAddr, Arc<AtomicBool>, JoinHandle<std::io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || {
        tracedb_server::serve_listener_with_shutdown(data_dir, listener, || {
            server_shutdown.load(Ordering::SeqCst)
        })
    });
    wait_for_http_ready(addr);
    (addr, shutdown, handle)
}

fn stop_stoppable_server(shutdown: Arc<AtomicBool>, handle: JoinHandle<std::io::Result<()>>) {
    shutdown.store(true, Ordering::SeqCst);
    handle
        .join()
        .expect("server thread panicked")
        .expect("server shutdown");
}

fn http_json_body(response: &str) -> Value {
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .expect("http response body");
    serde_json::from_str(body).expect("json response body")
}
