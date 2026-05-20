use serde_json::Value;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tracedb_query::{RecordScanRequest, TraceDb};
use tracedb_sdk::{TraceDbClient, TraceDbClientConfig};

static QUICKSTART_EXAMPLE_LOCK: Mutex<()> = Mutex::new(());

fn quickstart_example_lock() -> MutexGuard<'static, ()> {
    QUICKSTART_EXAMPLE_LOCK
        .lock()
        .expect("quickstart example lock poisoned")
}

#[test]
fn sdk_quickstart_example_runs_against_real_http_server() {
    let _guard = quickstart_example_lock();
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("engine");
    let admin_dir = temp.path().join("sdk-admin");
    let url = start_quickstart_test_server(data_dir);

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace_root)
        .env_remove("TRACEDB_IDEMPOTENCY_RETRIES")
        .args([
            "run",
            "-q",
            "-p",
            "tracedb-sdk",
            "--example",
            "quickstart",
            "--",
            "--url",
            &url,
            "--token",
            "dev-token",
            "--timeout-ms",
            "5000",
            "--safe-retries",
            "1",
            "--idempotency-retries",
            "1",
            "--admin-dir",
            admin_dir.to_str().expect("utf8 admin dir"),
        ])
        .output()
        .expect("run quickstart example");

    assert!(
        output.status.success(),
        "quickstart failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("quickstart emits json summary");

    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "rust-sdk-quickstart");
    assert_eq!(summary["server_url"], url);
    assert_eq!(summary["database_id"], Value::Null);
    assert_eq!(summary["branch_id"], Value::Null);
    assert_eq!(summary["table"], "docs");
    assert_eq!(summary["tenant_id"], "tenant-a");
    assert_eq!(summary["steps"]["health"], true);
    assert_eq!(summary["steps"]["catalog"], true);
    assert_eq!(summary["steps"]["metrics"], true);
    assert_eq!(summary["steps"]["schema_apply"], true);
    assert_eq!(summary["steps"]["batch_ingest"], true);
    assert_eq!(summary["steps"]["patch"], true);
    assert_eq!(summary["steps"]["query"], true);
    assert_eq!(summary["steps"]["scan"], true);
    assert_eq!(summary["steps"]["delete"], true);
    assert_eq!(summary["steps"]["compact"], true);
    assert_eq!(summary["steps"]["snapshot"], true);
    assert_eq!(summary["steps"]["restore"], true);
    assert_eq!(summary["steps"]["jobs"], true);
    assert_eq!(summary["health_ok"], true);
    assert!(summary["database_count"].as_u64().is_some());
    assert!(summary["branch_count"].as_u64().is_some());
    assert!(summary["metrics_latest_epoch"].as_u64().is_some());
    assert!(summary["admin_job_count"].as_u64().is_some());
    assert_eq!(summary["admin"]["requested"], true);
    assert_eq!(summary["admin"]["compact"], true);
    assert_eq!(summary["admin"]["snapshot"], true);
    assert_eq!(summary["admin"]["restore"], true);
    assert_eq!(summary["idempotency_retries"], 1);
    assert_eq!(summary["idempotency_keys"], true);
    assert_eq!(summary["patched"], true);
    assert_eq!(summary["patched_status"], "reviewed");
    let snapshot_target = summary["snapshot_target"]
        .as_str()
        .expect("snapshot target path");
    let restore_target = summary["restore_target"]
        .as_str()
        .expect("restore target path");
    assert!(Path::new(snapshot_target).starts_with(&admin_dir));
    assert!(Path::new(restore_target).starts_with(&admin_dir));
    assert_ne!(snapshot_target, restore_target);
    assert!(Path::new(snapshot_target).exists());
    assert!(Path::new(restore_target).exists());
    assert!(Path::new(snapshot_target).join("manifest.tdb").exists());
    assert!(Path::new(restore_target).join("manifest.tdb").exists());
    let restored = TraceDb::open(restore_target).expect("open restored database");
    let restored_scan = restored
        .scan(RecordScanRequest::new("docs", "tenant-a").limit(10))
        .expect("scan restored database");
    assert_eq!(restored_scan.returned_count, 1);
    assert_eq!(summary["sql_module"], "not_implemented");
}

#[test]
fn sdk_quickstart_example_skips_admin_without_admin_dir() {
    let _guard = quickstart_example_lock();
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("engine");
    let url = start_quickstart_test_server(data_dir);

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace_root)
        .env_remove("TRACEDB_ADMIN_DIR")
        .env_remove("TRACEDB_IDEMPOTENCY_RETRIES")
        .args([
            "run",
            "-q",
            "-p",
            "tracedb-sdk",
            "--example",
            "quickstart",
            "--",
            "--url",
            &url,
            "--token",
            "dev-token",
        ])
        .output()
        .expect("run quickstart example");

    assert!(
        output.status.success(),
        "quickstart failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("quickstart emits json summary");

    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "rust-sdk-quickstart");
    assert_eq!(summary["server_url"], url);
    assert_eq!(summary["database_id"], Value::Null);
    assert_eq!(summary["branch_id"], Value::Null);
    assert_eq!(summary["table"], "docs");
    assert_eq!(summary["tenant_id"], "tenant-a");
    assert_eq!(summary["steps"]["health"], true);
    assert_eq!(summary["steps"]["catalog"], true);
    assert_eq!(summary["steps"]["metrics"], true);
    assert_eq!(summary["steps"]["schema_apply"], true);
    assert_eq!(summary["steps"]["batch_ingest"], true);
    assert_eq!(summary["steps"]["patch"], true);
    assert_eq!(summary["steps"]["query"], true);
    assert_eq!(summary["steps"]["scan"], true);
    assert_eq!(summary["steps"]["delete"], true);
    assert_eq!(summary["steps"]["compact"], false);
    assert_eq!(summary["steps"]["snapshot"], false);
    assert_eq!(summary["steps"]["restore"], false);
    assert_eq!(summary["steps"]["jobs"], true);
    assert_eq!(summary["health_ok"], true);
    assert!(summary["database_count"].as_u64().is_some());
    assert!(summary["branch_count"].as_u64().is_some());
    assert!(summary["metrics_latest_epoch"].as_u64().is_some());
    assert!(summary["admin_job_count"].as_u64().is_some());
    assert_eq!(summary["admin"]["requested"], false);
    assert_eq!(summary["admin"]["compact"], "skipped");
    assert_eq!(summary["admin"]["snapshot"], "skipped");
    assert_eq!(summary["admin"]["restore"], "skipped");
    assert_eq!(summary["idempotency_retries"], 0);
    assert_eq!(summary["idempotency_keys"], false);
    assert_eq!(summary["patched"], true);
    assert_eq!(summary["patched_status"], "reviewed");
    assert!(summary["snapshot_target"].is_null());
    assert!(summary["restore_target"].is_null());
    assert_eq!(summary["sql_module"], "not_implemented");
}

#[test]
fn sdk_quickstart_accepts_idempotency_retries_from_env() {
    let _guard = quickstart_example_lock();
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("engine");
    let url = start_quickstart_test_server(data_dir);

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace_root)
        .env_remove("TRACEDB_ADMIN_DIR")
        .env("TRACEDB_IDEMPOTENCY_RETRIES", "1")
        .args([
            "run",
            "-q",
            "-p",
            "tracedb-sdk",
            "--example",
            "quickstart",
            "--",
            "--url",
            &url,
            "--token",
            "dev-token",
        ])
        .output()
        .expect("run quickstart example");

    assert!(
        output.status.success(),
        "quickstart failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("quickstart emits json summary");

    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "rust-sdk-quickstart");
    assert_eq!(summary["server_url"], url);
    assert_eq!(summary["database_id"], Value::Null);
    assert_eq!(summary["branch_id"], Value::Null);
    assert_eq!(summary["table"], "docs");
    assert_eq!(summary["tenant_id"], "tenant-a");
    assert_eq!(summary["idempotency_retries"], 1);
    assert_eq!(summary["idempotency_keys"], true);
    assert_eq!(summary["steps"]["health"], true);
    assert_eq!(summary["steps"]["catalog"], true);
    assert_eq!(summary["steps"]["metrics"], true);
    assert_eq!(summary["steps"]["schema_apply"], true);
    assert_eq!(summary["steps"]["batch_ingest"], true);
    assert_eq!(summary["steps"]["patch"], true);
    assert_eq!(summary["steps"]["delete"], true);
    assert_eq!(summary["steps"]["compact"], false);
    assert_eq!(summary["steps"]["jobs"], true);
    assert_eq!(summary["admin"]["requested"], false);
    assert_eq!(summary["admin"]["compact"], "skipped");
    assert_eq!(summary["admin"]["snapshot"], "skipped");
    assert_eq!(summary["admin"]["restore"], "skipped");
    assert_eq!(summary["health_ok"], true);
    assert_eq!(summary["sql_module"], "not_implemented");
}

fn start_quickstart_test_server(data_dir: PathBuf) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let bind = addr.to_string();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &bind);
    });
    let url = format!("http://{addr}");
    wait_for_quickstart_ready(&url);
    url
}

fn wait_for_quickstart_ready(url: &str) {
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url.to_string(), "dev-token")
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
        "quickstart TraceDB HTTP test server did not become ready at {url}; last error: {}",
        last_error.unwrap_or_else(|| "no readiness attempt completed".to_string())
    );
}

fn quickstart_failure_summary(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "quickstart failure should emit parseable json summary: {error}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn sdk_quickstart_rejects_relative_admin_dir_before_http_request() {
    let _guard = quickstart_example_lock();
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace_root)
        .env_remove("TRACEDB_IDEMPOTENCY_RETRIES")
        .args([
            "run",
            "-q",
            "-p",
            "tracedb-sdk",
            "--example",
            "quickstart",
            "--",
            "--url",
            "http://127.0.0.1:1",
            "--admin-dir",
            "relative-admin",
        ])
        .output()
        .expect("run quickstart example");

    assert!(!output.status.success());
    let summary = quickstart_failure_summary(&output);
    assert_eq!(summary["ok"], false);
    assert_eq!(summary["mode"], "rust-sdk-quickstart");
    assert_eq!(summary["error"]["kind"], "configuration");
    assert!(
        summary["error"]["message"].as_str().is_some_and(
            |message| message.contains("--admin-dir must be an absolute server-side path")
        ),
        "unexpected failure summary: {summary}"
    );
    assert_eq!(summary["sql_module"], "not_implemented");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--admin-dir must be an absolute server-side path"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn sdk_quickstart_rejects_invalid_url_with_parseable_summary() {
    let _guard = quickstart_example_lock();
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace_root)
        .env_remove("TRACEDB_IDEMPOTENCY_RETRIES")
        .args([
            "run",
            "-q",
            "-p",
            "tracedb-sdk",
            "--example",
            "quickstart",
            "--",
            "--url",
            "not-http",
            "--token",
            "dev-token",
        ])
        .output()
        .expect("run quickstart example");

    assert!(!output.status.success());
    let summary = quickstart_failure_summary(&output);
    assert_eq!(summary["ok"], false);
    assert_eq!(summary["mode"], "rust-sdk-quickstart");
    assert_eq!(summary["server_url"], "not-http");
    assert_eq!(summary["error"]["kind"], "configuration");
    assert!(
        summary["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("invalid TraceDB URL not-http")),
        "unexpected failure summary: {summary}"
    );
    assert_eq!(summary["steps"]["ready"], false);
    assert_eq!(summary["sql_module"], "not_implemented");
}

#[test]
fn sdk_quickstart_rejects_invalid_idempotency_retries_before_http_request() {
    let _guard = quickstart_example_lock();
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace_root)
        .env_remove("TRACEDB_IDEMPOTENCY_RETRIES")
        .args([
            "run",
            "-q",
            "-p",
            "tracedb-sdk",
            "--example",
            "quickstart",
            "--",
            "--url",
            "http://127.0.0.1:1",
            "--idempotency-retries",
            "nope",
        ])
        .output()
        .expect("run quickstart example");

    assert!(!output.status.success());
    let summary = quickstart_failure_summary(&output);
    assert_eq!(summary["ok"], false);
    assert_eq!(summary["mode"], "rust-sdk-quickstart");
    assert_eq!(summary["error"]["kind"], "configuration");
    assert!(
        summary["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("--idempotency-retries must fit in 0..=255")),
        "unexpected failure summary: {summary}"
    );
    assert_eq!(summary["sql_module"], "not_implemented");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--idempotency-retries must fit in 0..=255"),
        "unexpected stderr: {stderr}"
    );
}
