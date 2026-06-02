use serde_json::Value;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

static PRODUCT_QUICKSTART_REPORT_LOCK: Mutex<()> = Mutex::new(());
static PRODUCT_REGRESSION_SMOKE_LOCK: Mutex<()> = Mutex::new(());
static DURABILITY_FAULTS_REPORT_LOCK: Mutex<()> = Mutex::new(());
static STORAGE_INDEX_JOBS_REPORT_LOCK: Mutex<()> = Mutex::new(());

fn read_json_file(path: &Path) -> Value {
    let body =
        std::fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_slice(&body)
        .unwrap_or_else(|error| panic!("parse {} as json: {error}", path.display()))
}

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
}

#[test]
fn get_positional_command_reads_table_tenant_and_record_id() {
    let temp = tempfile::tempdir().expect("tempdir");
    let record = serde_json::json!({
        "table": "docs",
        "tenant_id": "tenant-a",
        "id": "intro",
        "fields": { "body": "hello" }
    });

    let schema_path = write_docs_schema(temp.path());
    run_tracedb(temp.path(), &["schema", "apply", path_str(&schema_path)]);
    run_tracedb(temp.path(), &["put", &record.to_string()]);
    let output = run_tracedb(temp.path(), &["get", "docs", "tenant-a", "intro"]);

    assert_eq!(output["record"]["id"], "intro");
    assert_eq!(output["record"]["table"], "docs");
    assert_eq!(output["record"]["tenant_id"], "tenant-a");
}

#[test]
fn delete_positional_command_reads_table_tenant_and_record_id() {
    let temp = tempfile::tempdir().expect("tempdir");
    let record = serde_json::json!({
        "table": "docs",
        "tenant_id": "tenant-a",
        "id": "intro",
        "fields": { "body": "hello" }
    });

    let schema_path = write_docs_schema(temp.path());
    run_tracedb(temp.path(), &["schema", "apply", path_str(&schema_path)]);
    run_tracedb(temp.path(), &["put", &record.to_string()]);
    let output = run_tracedb(temp.path(), &["delete", "docs", "tenant-a", "intro"]);

    assert_eq!(output["deleted"], true);
}

#[test]
fn scan_positional_command_reads_table_tenant_and_limit() {
    let temp = tempfile::tempdir().expect("tempdir");

    let schema_path = write_docs_schema(temp.path());
    run_tracedb(temp.path(), &["schema", "apply", path_str(&schema_path)]);
    for id in ["intro", "ops"] {
        let record = serde_json::json!({
            "table": "docs",
            "tenant_id": "tenant-a",
            "id": id,
            "fields": { "body": id }
        });
        run_tracedb(temp.path(), &["put", &record.to_string()]);
    }

    let output = run_tracedb(temp.path(), &["scan", "docs", "tenant-a", "1"]);

    assert_eq!(output["returned_count"], 1);
    assert_eq!(output["records"][0]["table"], "docs");
    assert_eq!(output["records"][0]["tenant_id"], "tenant-a");
}

fn run_tracedb(data_dir: &Path, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("--data")
        .arg(data_dir)
        .args(args)
        .output()
        .expect("run tracedb");
    assert!(
        output.status.success(),
        "tracedb {:?} failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("tracedb json")
}

fn write_docs_schema(dir: &Path) -> std::path::PathBuf {
    let schema_path = dir.join("docs.schema.json");
    let schema = serde_json::json!({
        "name": "docs",
        "primary_id_column": "id",
        "tenant_id_column": "tenant",
        "scalar_columns": [],
        "text_indexed_columns": ["body"],
        "vector_columns": []
    });
    std::fs::write(&schema_path, schema.to_string()).expect("write schema");
    schema_path
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("utf-8 path")
}

#[test]
fn demo_command_exercises_local_product_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("--data")
        .arg(temp.path())
        .arg("demo")
        .output()
        .expect("run tracedb demo");
    assert!(
        output.status.success(),
        "demo failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("demo json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "embedded-local-demo");
    assert_eq!(summary["steps"]["schema_apply"], true);
    assert_eq!(summary["steps"]["batch_ingest"], true);
    assert_eq!(summary["steps"]["query"], true);
    assert_eq!(summary["steps"]["delete"], true);
    assert_eq!(summary["steps"]["snapshot"], true);
    assert_eq!(summary["steps"]["restore"], true);
    assert_eq!(summary["deleted_hidden"], true);
    assert_eq!(summary["sql_module"], "not_implemented");
    assert_eq!(summary["records_before_delete"], 3);
    assert_eq!(summary["records_after_restore"], 2);
    assert!(
        summary["query_result_ids"]
            .as_array()
            .is_some_and(|ids| !ids.is_empty()),
        "demo should return query ids: {summary}"
    );

    let verify = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("--data")
        .arg(temp.path())
        .arg("verify")
        .output()
        .expect("run tracedb verify");
    assert!(
        verify.status.success(),
        "verify failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&verify.stdout),
        String::from_utf8_lossy(&verify.stderr)
    );
    let verify_json: Value = serde_json::from_slice(&verify.stdout).expect("verify json");
    assert_eq!(verify_json["ok"], true);
}

#[test]
fn http_demo_command_exercises_local_http_sdk_product_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("--data")
        .arg(temp.path())
        .arg("http-demo")
        .output()
        .expect("run tracedb http-demo");
    assert!(
        output.status.success(),
        "http-demo failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("http-demo json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-http-api-demo");
    assert_eq!(summary["steps"]["server_start"], true);
    assert_eq!(summary["steps"]["ready"], true);
    assert_eq!(summary["steps"]["schema_apply"], true);
    assert_eq!(summary["steps"]["batch_ingest"], true);
    assert_eq!(summary["steps"]["scan"], true);
    assert_eq!(summary["steps"]["query"], true);
    assert_eq!(summary["steps"]["explain"], true);
    assert_eq!(summary["steps"]["delete"], true);
    assert_eq!(summary["steps"]["compact"], true);
    assert_eq!(summary["steps"]["snapshot"], true);
    assert_eq!(summary["steps"]["restore"], true);
    assert_eq!(summary["deleted_hidden"], true);
    assert_eq!(summary["records_scanned"], 3);
    assert_eq!(summary["records_after_restore"], 2);
    assert_eq!(summary["idempotency_retries"], 1);
    assert_eq!(summary["idempotency_keys"], true);
    assert_eq!(summary["sql_module"], "not_implemented");
    assert!(
        summary["server_url"]
            .as_str()
            .is_some_and(|url| url.starts_with("http://127.0.0.1:")),
        "http-demo should report loopback server url: {summary}"
    );
}

#[test]
fn product_regression_runs_local_product_gate() {
    let _smoke_lock = PRODUCT_REGRESSION_SMOKE_LOCK
        .lock()
        .expect("lock product regression smoke path");
    let temp = tempfile::tempdir().expect("tempdir");
    let report_file = temp.path().join("reports/product-regression.json");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(temp.path())
        .arg("--report-file")
        .arg(&report_file)
        .output()
        .expect("run tracedb product-regression");
    assert!(
        output.status.success(),
        "product-regression failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("product-regression json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(report_summary, summary);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["report_file"], report_file.display().to_string());
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    assert_eq!(summary["human_summary"]["status"], "passed");
    assert_eq!(
        summary["human_summary"]["message"],
        "local product regression passed: 4/4 steps"
    );
    assert_eq!(summary["human_summary"]["steps_passed"], 4);
    assert_eq!(summary["human_summary"]["steps_total"], 4);
    assert_eq!(summary["human_summary"]["failed_step"], Value::Null);
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 4);
    for step in [
        "embedded_demo",
        "embedded_verify",
        "http_demo",
        "local_doctor",
    ] {
        assert_eq!(
            summary["steps"][step]["ok"], true,
            "product-regression core step {step} should pass: {summary}"
        );
    }
    for removed_step in [
        "rust_sdk_quickstart",
        "python_sdk_smoke",
        "typescript_check",
        "typescript_http_smoke",
        "typescript_gateway_smoke",
    ] {
        assert!(
            !steps.contains_key(removed_step),
            "core product-regression should not run SDK step {removed_step}: {summary}"
        );
    }
    assert_eq!(
        summary["steps"]["embedded_demo"]["summary"]["sql_module"],
        "not_implemented"
    );
    assert_eq!(
        summary["steps"]["http_demo"]["summary"]["sql_module"],
        "not_implemented"
    );
    assert_eq!(
        summary["steps"]["local_doctor"]["summary"]["ready_wait"]["ok"],
        true
    );
}

#[test]
fn product_quickstart_runs_product_gate_with_default_report_file() {
    let _report_lock = PRODUCT_QUICKSTART_REPORT_LOCK
        .lock()
        .expect("lock product quickstart report path");
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("quickstart-data");
    let report_file = workspace_root().join("target/tracedb/product-quickstart.json");
    let _ = std::fs::remove_file(&report_file);

    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-quickstart")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("embedded_demo")
        .output()
        .expect("run tracedb product-quickstart");

    assert!(
        output.status.success(),
        "product-quickstart failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("product-quickstart json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(report_summary, summary);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["report_file"], report_file.display().to_string());
    assert_eq!(summary["only_step"], "embedded_demo");
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    assert_eq!(
        summary["human_summary"]["message"],
        "local product regression passed: 1/1 steps; only_step=embedded_demo"
    );
    assert_eq!(summary["steps"]["embedded_demo"]["ok"], true);
}

#[test]
fn product_quickstart_injected_failure_uses_default_report_file() {
    let _report_lock = PRODUCT_QUICKSTART_REPORT_LOCK
        .lock()
        .expect("lock product quickstart report path");
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("quickstart-failure-data");
    let report_file = workspace_root().join("target/tracedb/product-quickstart.json");
    let _ = std::fs::remove_file(&report_file);

    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-quickstart")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--inject-failure")
        .arg("embedded_demo")
        .output()
        .expect("run tracedb product-quickstart with injected failure");

    assert!(
        !output.status.success(),
        "injected product-quickstart failure should exit nonzero\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("product-quickstart failure json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(report_summary, summary);
    assert_eq!(summary["ok"], false);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["report_file"], report_file.display().to_string());
    assert_eq!(summary["failure_injection"], "embedded_demo");
    assert_eq!(summary["only_step"], Value::Null);
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    assert_eq!(summary["human_summary"]["status"], "failed");
    assert_eq!(
        summary["human_summary"]["message"],
        "local product regression failed: 0/1 steps passed; failed_step=embedded_demo"
    );
    assert_eq!(summary["human_summary"]["steps_passed"], 0);
    assert_eq!(summary["human_summary"]["steps_total"], 1);
    assert_eq!(summary["human_summary"]["failed_step"], "embedded_demo");
    assert_eq!(summary["steps"]["embedded_demo"]["ok"], false);
    assert_eq!(summary["steps"]["embedded_demo"]["injected_failure"], true);
    assert_eq!(
        summary["steps"]["embedded_demo"]["error"],
        "injected product-regression failure"
    );
}

#[test]
fn durability_faults_emit_machine_readable_default_report() {
    let _report_lock = DURABILITY_FAULTS_REPORT_LOCK
        .lock()
        .expect("lock durability faults report path");
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("durability-faults-data");
    let report_file = workspace_root().join("target/tracedb/durability-faults.json");
    let _ = std::fs::remove_file(&report_file);
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("durability-faults")
        .arg("--data-root")
        .arg(&data_root)
        .output()
        .expect("run tracedb durability-faults");
    assert!(
        output.status.success(),
        "durability-faults failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let summary: Value = serde_json::from_slice(&output.stdout).expect("durability faults json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(summary, report_summary);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-durability-faults");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["report_file"], report_file.display().to_string());
    assert_eq!(summary["data_root"], data_root.display().to_string());
    assert_eq!(summary["statuses"]["passed"], 8);
    assert_eq!(summary["statuses"]["failed"], 0);
    assert_eq!(summary["statuses"]["not_applicable"], 0);
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(
        summary["claims"]["cross_replica_exactly_once"],
        "not_claimed"
    );
    assert_eq!(
        summary["claims"]["tde_scope"],
        "local_artifacts_when_configured"
    );

    for scenario in [
        "wrong_master_key",
        "missing_master_key",
        "torn_wal_tail",
        "manifest_corruption",
        "checkpoint_corruption",
        "stale_lock_recovery",
        "encrypted_snapshot_restore",
        "wal_idempotency_replay_after_reopen",
    ] {
        assert_eq!(
            summary["scenarios"][scenario]["status"], "passed",
            "scenario {scenario} should pass: {summary}"
        );
        assert!(
            summary["scenarios"][scenario]["evidence"]
                .as_str()
                .is_some_and(|evidence| !evidence.is_empty()),
            "scenario {scenario} should include evidence: {summary}"
        );
    }
}

#[test]
fn durability_faults_injected_failure_exits_nonzero_and_preserves_json_report() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("durability-faults-failure-data");
    let report_file = temp.path().join("reports/durability-faults-failure.json");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("durability-faults")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--report-file")
        .arg(&report_file)
        .arg("--inject-failure")
        .arg("checkpoint_corruption")
        .output()
        .expect("run failing tracedb durability-faults");
    assert!(
        !output.status.success(),
        "injected durability-faults failure should exit nonzero\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("durability faults failure json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(summary, report_summary);
    assert_eq!(summary["ok"], false);
    assert_eq!(summary["statuses"]["failed"], 1);
    assert_eq!(
        summary["scenarios"]["checkpoint_corruption"]["status"],
        "failed"
    );
    assert!(
        summary["scenarios"]["checkpoint_corruption"]["error"]
            .as_str()
            .is_some_and(|error| error.contains("injected durability fault failure")),
        "failure scenario should preserve injected error: {summary}"
    );
}

#[test]
fn storage_index_jobs_emit_machine_readable_default_report() {
    let _report_lock = STORAGE_INDEX_JOBS_REPORT_LOCK
        .lock()
        .expect("lock storage index jobs report path");
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("storage-index-jobs-data");
    let report_file = workspace_root().join("target/tracedb/storage-index-jobs.json");
    let _ = std::fs::remove_file(&report_file);
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("storage-index-jobs")
        .arg("--data-root")
        .arg(&data_root)
        .output()
        .expect("run tracedb storage-index-jobs");
    assert!(
        output.status.success(),
        "storage-index-jobs failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let summary: Value = serde_json::from_slice(&output.stdout).expect("storage jobs json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(summary, report_summary);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-storage-index-jobs");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["report_file"], report_file.display().to_string());
    assert_eq!(summary["statuses"]["failed"], 0);
    for scenario in [
        "delta_writes",
        "binary_segment_roundtrip",
        "legacy_json_segment_read",
        "checksum_corruption",
        "encrypted_binary_artifacts",
        "bm25_query_parity",
        "greedy_nn_vector_parity",
        "bitmap_policy_filtering",
        "stale_sealed_candidate_hot_materialization",
        "vacuum_safety",
        "durable_enqueue_replay",
        "lease_expiry",
        "retry_dead_letter",
        "interrupted_compaction",
        "failed_index_build_recovery",
        "backup_job_failure",
        "restore_verification_job",
        "reopen_after_job_state_change",
    ] {
        assert_eq!(
            summary["scenarios"][scenario]["status"], "passed",
            "scenario {scenario} should pass: {summary}"
        );
        assert!(
            summary["scenarios"][scenario]["evidence"]
                .as_str()
                .is_some_and(|evidence| !evidence.is_empty()),
            "scenario {scenario} should include evidence: {summary}"
        );
    }
}

#[test]
fn product_quickstart_runs_core_gate_with_default_report_file() {
    let _smoke_lock = PRODUCT_REGRESSION_SMOKE_LOCK
        .lock()
        .expect("lock product regression smoke path");
    let _report_lock = PRODUCT_QUICKSTART_REPORT_LOCK
        .lock()
        .expect("lock product quickstart report path");
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("quickstart-core-data");
    let report_file = workspace_root().join("target/tracedb/product-quickstart.json");
    let _ = std::fs::remove_file(&report_file);

    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-quickstart")
        .arg("--data-root")
        .arg(&data_root)
        .output()
        .expect("run tracedb product-quickstart core gate");

    assert!(
        output.status.success(),
        "product-quickstart failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("product-quickstart json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(report_summary, summary);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["report_file"], report_file.display().to_string());
    assert_eq!(summary["only_step"], Value::Null);
    assert_eq!(summary["failure_injection"], Value::Null);
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    assert_eq!(summary["human_summary"]["status"], "passed");
    assert_eq!(
        summary["human_summary"]["message"],
        "local product regression passed: 4/4 steps"
    );
    assert_eq!(summary["human_summary"]["steps_passed"], 4);
    assert_eq!(summary["human_summary"]["steps_total"], 4);
    let steps = summary["steps"]
        .as_object()
        .expect("product quickstart steps object");
    assert_eq!(steps.len(), 4);
    for step in [
        "embedded_demo",
        "embedded_verify",
        "http_demo",
        "local_doctor",
    ] {
        assert_eq!(
            steps[step]["ok"], true,
            "product-quickstart core step {step} should pass: {summary}"
        );
    }
    for removed_step in [
        "rust_sdk_quickstart",
        "python_sdk_smoke",
        "typescript_check",
        "typescript_http_smoke",
        "typescript_gateway_smoke",
    ] {
        assert!(
            !steps.contains_key(removed_step),
            "product-quickstart should not include SDK step {removed_step}: {summary}"
        );
    }
}

#[test]
fn product_regression_injected_failure_exits_nonzero_and_preserves_json_summary() {
    let temp = tempfile::tempdir().expect("tempdir");
    let report_file = temp.path().join("reports/failure.json");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(temp.path())
        .arg("--report-file")
        .arg(&report_file)
        .arg("--only")
        .arg("embedded_demo")
        .arg("--inject-failure")
        .arg("embedded_demo")
        .output()
        .expect("run tracedb product-regression with injected failure");
    assert!(
        !output.status.success(),
        "injected product-regression failure should exit nonzero\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("product-regression failure json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(report_summary, summary);
    assert_eq!(summary["ok"], false);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["failure_injection"], "embedded_demo");
    assert_eq!(summary["only_step"], "embedded_demo");
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    assert_eq!(summary["human_summary"]["status"], "failed");
    assert_eq!(
        summary["human_summary"]["message"],
        "local product regression failed: 0/1 steps passed; failed_step=embedded_demo; only_step=embedded_demo"
    );
    assert_eq!(summary["human_summary"]["steps_passed"], 0);
    assert_eq!(summary["human_summary"]["steps_total"], 1);
    assert_eq!(summary["human_summary"]["failed_step"], "embedded_demo");
    assert_eq!(summary["steps"]["embedded_demo"]["ok"], false);
    assert_eq!(summary["steps"]["embedded_demo"]["injected_failure"], true);
    assert_eq!(
        summary["steps"]["embedded_demo"]["error"],
        "injected product-regression failure"
    );
}

#[test]
fn product_regression_rejects_removed_sdk_only_selectors() {
    for removed_step in [
        "rust_sdk_quickstart",
        "python_sdk_smoke",
        "typescript_check",
        "typescript_http_smoke",
        "typescript_gateway_smoke",
    ] {
        let temp = tempfile::tempdir().expect("tempdir");
        let data_root = temp.path().join(format!("removed-{removed_step}"));
        let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
            .arg("product-regression")
            .arg("--data-root")
            .arg(&data_root)
            .arg("--only")
            .arg(removed_step)
            .output()
            .unwrap_or_else(|error| {
                panic!("run tracedb product-regression --only {removed_step}: {error}")
            });
        assert!(
            !output.status.success(),
            "product-regression --only {removed_step} should be rejected after SDK split\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stdout.is_empty(),
            "removed SDK selector {removed_step} should fail before emitting product-regression JSON: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            !data_root.exists(),
            "removed SDK selector {removed_step} should fail during option parsing before creating data roots"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(removed_step)
                && stderr.contains("unknown product-regression --only step"),
            "stderr should explain removed/unknown SDK selector {removed_step}: {stderr}"
        );
    }
}

#[test]
fn product_regression_list_steps_reports_gate_steps_without_running_them() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("unused-product-data");
    let report_file = temp.path().join("reports/steps.json");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--list-steps")
        .arg("--report-file")
        .arg(&report_file)
        .output()
        .expect("run tracedb product-regression step list");
    assert!(
        output.status.success(),
        "product-regression --list-steps failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !data_root.exists(),
        "--list-steps should not create product regression data"
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("product-regression step list json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(report_summary, summary);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression-step-list");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["report_file"], report_file.display().to_string());
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    assert_eq!(summary["human_summary"]["status"], "listed");
    assert_eq!(
        summary["human_summary"]["message"],
        "local product regression steps listed: 4 steps; only_supported=4"
    );
    assert_eq!(summary["human_summary"]["steps_total"], 4);
    assert_eq!(summary["human_summary"]["only_supported"], 4);
    let steps = summary["steps"].as_array().expect("steps array");
    assert_eq!(steps.len(), 4);
    let step_names = steps
        .iter()
        .map(|step| step["name"].as_str().expect("step name"))
        .collect::<Vec<_>>();
    assert_eq!(
        step_names,
        [
            "embedded_demo",
            "embedded_verify",
            "http_demo",
            "local_doctor"
        ]
    );
    for step in steps {
        assert_eq!(
            step["only_supported"], true,
            "product-regression --list-steps should mark every listed step as --only supported: {summary}"
        );
    }
}

#[test]
fn product_regression_only_embedded_demo_runs_single_gate_step() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("only-embedded-demo");
    let report_file = temp.path().join("reports/embedded-demo.json");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("embedded_demo")
        .arg("--report-file")
        .arg(&report_file)
        .output()
        .expect("run tracedb product-regression single step");
    assert!(
        output.status.success(),
        "product-regression --only embedded_demo failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("product-regression only-step json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(report_summary, summary);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["only_step"], "embedded_demo");
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    assert_eq!(summary["human_summary"]["status"], "passed");
    assert_eq!(
        summary["human_summary"]["message"],
        "local product regression passed: 1/1 steps; only_step=embedded_demo"
    );
    assert_eq!(summary["human_summary"]["steps_passed"], 1);
    assert_eq!(summary["human_summary"]["steps_total"], 1);
    assert_eq!(summary["human_summary"]["failed_step"], Value::Null);
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 1);
    assert_eq!(summary["steps"]["embedded_demo"]["ok"], true);
    assert_eq!(
        summary["steps"]["embedded_demo"]["summary"]["sql_module"],
        "not_implemented"
    );
}

#[test]
fn product_regression_only_http_demo_runs_single_gate_step() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("only-http-demo");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("http_demo")
        .output()
        .expect("run tracedb product-regression http demo step");
    assert!(
        output.status.success(),
        "product-regression --only http_demo failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("product-regression http demo json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["only_step"], "http_demo");
    assert_eq!(summary["local_server_url"], Value::Null);
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 1);
    assert_eq!(summary["steps"]["http_demo"]["ok"], true);
    assert_eq!(
        summary["steps"]["http_demo"]["summary"]["sql_module"],
        "not_implemented"
    );
}

#[test]
fn product_regression_only_local_doctor_runs_single_gate_step() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("only-local-doctor");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("local_doctor")
        .output()
        .expect("run tracedb product-regression local doctor step");
    assert!(
        output.status.success(),
        "product-regression --only local_doctor failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("product-regression local doctor json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["only_step"], "local_doctor");
    assert!(
        summary["local_server_url"]
            .as_str()
            .is_some_and(|url| url.starts_with("http://127.0.0.1:")),
        "local_doctor should report managed local server url: {summary}"
    );
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 1);
    assert_eq!(summary["steps"]["local_doctor"]["ok"], true);
    assert_eq!(
        summary["steps"]["local_doctor"]["summary"]["mode"],
        "http-endpoint-diagnostics"
    );
    assert_eq!(
        summary["steps"]["local_doctor"]["summary"]["ready_wait"]["ok"],
        true
    );
    assert_eq!(
        summary["steps"]["local_doctor"]["summary"]["checks"]["ready"]["response"]["ready"],
        true
    );
    assert_eq!(
        summary["steps"]["local_doctor"]["summary"]["sql_module"],
        "not_implemented"
    );
}

#[test]
fn product_regression_only_embedded_verify_reuses_existing_embedded_demo_data() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("embedded-verify-target");
    let demo = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("embedded_demo")
        .output()
        .expect("seed embedded demo data");
    assert!(
        demo.status.success(),
        "product-regression --only embedded_demo failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&demo.stdout),
        String::from_utf8_lossy(&demo.stderr)
    );

    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("embedded_verify")
        .output()
        .expect("run tracedb product-regression embedded verify step");
    assert!(
        output.status.success(),
        "product-regression --only embedded_verify failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("product-regression embedded verify json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["only_step"], "embedded_verify");
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 1);
    assert_eq!(summary["steps"]["embedded_verify"]["ok"], true);
}

#[test]
fn doctor_http_reports_endpoint_diagnostics() {
    let temp = tempfile::tempdir().expect("tempdir");
    let bind = free_loopback_bind();
    let mut server = ServerChild {
        child: Command::new(env!("CARGO_BIN_EXE_tracedb"))
            .arg("--data")
            .arg(temp.path())
            .arg("serve")
            .arg(&bind)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start tracedb server"),
    };

    let url = format!("http://{bind}");
    let summary = wait_for_http_doctor(&url, &mut server.child);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "http-endpoint-diagnostics");
    assert_eq!(summary["server_url"], url);
    assert_eq!(summary["database_id"], "db_local");
    assert_eq!(summary["branch_id"], "db_local:main");
    assert_eq!(summary["checks"]["health"]["ok"], true);
    assert_eq!(
        summary["checks"]["health"]["response"]["service"],
        "tracedb-engine"
    );
    assert_eq!(summary["checks"]["ready"]["ok"], true);
    assert_eq!(summary["checks"]["ready"]["response"]["ready"], true);
    assert_eq!(summary["checks"]["databases"]["ok"], true);
    assert_eq!(summary["checks"]["branches"]["ok"], true);
    assert_eq!(summary["checks"]["metrics"]["ok"], true);
    assert_eq!(summary["checks"]["admin_jobs"]["ok"], true);
    assert_eq!(summary["sql_module"], "not_implemented");
}

#[test]
fn doctor_http_reads_endpoint_config_from_environment() {
    let temp = tempfile::tempdir().expect("tempdir");
    let bind = free_loopback_bind();
    let mut server = ServerChild {
        child: Command::new(env!("CARGO_BIN_EXE_tracedb"))
            .arg("--data")
            .arg(temp.path())
            .arg("serve")
            .arg(&bind)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start tracedb server"),
    };

    let url = format!("http://{bind}");
    let summary = wait_for_http_doctor_env(&url, &mut server.child);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["server_url"], url);
    assert_eq!(summary["database_id"], "db_local");
    assert_eq!(summary["branch_id"], "db_local:main");
    assert_eq!(summary["request_timeout_ms"], 500);
    assert_eq!(summary["safe_retries"], 0);
    assert_eq!(summary["ready_wait_timeout_ms"], 1000);
    assert_eq!(summary["ready_wait"]["ok"], true);
    assert_eq!(summary["checks"]["admin_jobs"]["ok"], true);
    assert_eq!(summary["sql_module"], "not_implemented");
}

#[test]
fn doctor_http_exits_nonzero_for_unhealthy_endpoint_and_preserves_json_summary() {
    let url = format!("http://{}", free_loopback_bind());
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("doctor")
        .arg("http")
        .arg("--url")
        .arg(&url)
        .arg("--token")
        .arg("dev-token")
        .arg("--timeout-ms")
        .arg("25")
        .arg("--safe-retries")
        .arg("0")
        .output()
        .expect("run tracedb doctor http against unavailable endpoint");

    assert!(
        !output.status.success(),
        "doctor http should fail the process when endpoint checks fail\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("doctor json");
    assert_eq!(summary["ok"], false);
    assert_eq!(summary["mode"], "http-endpoint-diagnostics");
    assert_eq!(summary["server_url"], url);
    assert_eq!(summary["checks"]["health"]["ok"], false);
    assert_eq!(summary["sql_module"], "not_implemented");
}

#[test]
fn doctor_http_reports_server_error_code_from_endpoint() {
    let (url, server) = start_static_error_server(
        6,
        r#"{"error":"engine unavailable","code":"engine_unavailable"}"#,
    );
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("doctor")
        .arg("http")
        .arg("--url")
        .arg(&url)
        .arg("--token")
        .arg("dev-token")
        .arg("--timeout-ms")
        .arg("500")
        .arg("--safe-retries")
        .arg("0")
        .output()
        .expect("run tracedb doctor http against coded error endpoint");
    server.join().expect("static error server thread");

    assert!(
        !output.status.success(),
        "doctor http should fail when endpoint returns coded errors\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("doctor json");
    assert_eq!(summary["ok"], false);
    assert_eq!(
        summary["checks"]["health"]["server_error"],
        "engine unavailable"
    );
    assert_eq!(
        summary["checks"]["health"]["server_error_code"],
        "engine_unavailable"
    );
}

#[test]
fn doctor_http_waits_for_readiness_before_checks() {
    let (url, server) = start_readiness_gate_server(1);
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("doctor")
        .arg("http")
        .arg("--url")
        .arg(&url)
        .arg("--token")
        .arg("dev-token")
        .arg("--timeout-ms")
        .arg("500")
        .arg("--safe-retries")
        .arg("0")
        .arg("--wait-ready-ms")
        .arg("1000")
        .output()
        .expect("run tracedb doctor http with readiness wait");
    server.join().expect("readiness gate server thread");

    assert!(
        output.status.success(),
        "doctor http should wait for readiness before endpoint checks\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("doctor json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["ready_wait_timeout_ms"], 1000);
    assert_eq!(summary["checks"]["ready"]["response"]["ready"], true);
}

struct ServerChild {
    child: Child,
}

impl Drop for ServerChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_loopback_bind() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let bind = listener.local_addr().expect("local addr").to_string();
    drop(listener);
    bind
}

fn start_static_error_server(
    request_count: usize,
    body: &'static str,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind static error server");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("static server addr")
    );
    let handle = thread::spawn(move || {
        for stream in listener.incoming().take(request_count) {
            let mut stream = stream.expect("accept static error request");
            let _ = read_request_headers(&mut stream);
            let response = format!(
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write static error response");
        }
    });
    (url, handle)
}

fn start_readiness_gate_server(readiness_failures: usize) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind readiness gate server");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("readiness gate server addr")
    );
    let handle = thread::spawn(move || {
        listener
            .set_nonblocking(true)
            .expect("set readiness gate server nonblocking");
        let deadline = Instant::now() + Duration::from_secs(5);
        let expected_requests = readiness_failures + 7;
        let mut handled_requests = 0;
        let mut ready_requests = 0;
        while handled_requests < expected_requests && Instant::now() < deadline {
            let (mut stream, _) = match listener.accept() {
                Ok(accepted) => accepted,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("accept readiness gate request: {error}"),
            };
            handled_requests += 1;
            stream
                .set_nonblocking(false)
                .expect("set readiness gate stream blocking");
            let request_line = read_request_headers(&mut stream);
            let path = request_line
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .split('?')
                .next()
                .unwrap_or("/");
            let body = match path {
                "/v1/ready" => {
                    ready_requests += 1;
                    if ready_requests <= readiness_failures {
                        r#"{"ready":false,"service":"tracedb-engine"}"#
                    } else {
                        r#"{"ready":true,"service":"tracedb-engine"}"#
                    }
                }
                "/v1/health" => r#"{"ok":true,"service":"tracedb-engine"}"#,
                "/v1/databases" => r#"{"databases":[{"database_id":"local"}]}"#,
                "/v1/branches" => r#"{"branches":[{"branch_id":"local:main"}]}"#,
                "/v1/metrics/public-safe" => r#"{"service":"tracedb-engine"}"#,
                "/v1/admin/jobs" => r#"{"jobs":[]}"#,
                _ => r#"{"error":"not found","code":"not_found"}"#,
            };
            let status = if path == "/" {
                "404 Not Found"
            } else {
                "200 OK"
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write readiness gate response");
        }
    });
    (url, handle)
}

fn read_request_headers(stream: &mut TcpStream) -> String {
    let mut buffer = [0u8; 256];
    let mut request = Vec::new();
    loop {
        let read = stream.read(&mut buffer).expect("read static error request");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&request)
        .lines()
        .next()
        .unwrap_or("")
        .to_string()
}

fn wait_for_http_doctor(url: &str, server: &mut Child) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_stdout = String::new();
    let mut last_stderr = String::new();
    while Instant::now() < deadline {
        if let Some(status) = server.try_wait().expect("poll tracedb server") {
            panic!("tracedb server exited before doctor check: {status}");
        }
        let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
            .arg("doctor")
            .arg("http")
            .arg("--url")
            .arg(url)
            .arg("--token")
            .arg("dev-token")
            .arg("--database-id")
            .arg("db_local")
            .arg("--branch-id")
            .arg("db_local:main")
            .arg("--timeout-ms")
            .arg("500")
            .output()
            .expect("run tracedb doctor http");
        last_stdout = String::from_utf8_lossy(&output.stdout).to_string();
        last_stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if output.status.success() {
            let summary: Value = serde_json::from_slice(&output.stdout).expect("doctor json");
            if summary["ok"] == true {
                return summary;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("doctor http did not report ok\nstdout:\n{last_stdout}\nstderr:\n{last_stderr}");
}

fn wait_for_http_doctor_env(url: &str, server: &mut Child) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_stdout = String::new();
    let mut last_stderr = String::new();
    while Instant::now() < deadline {
        if let Some(status) = server.try_wait().expect("poll tracedb server") {
            panic!("tracedb server exited before doctor check: {status}");
        }
        let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
            .arg("doctor")
            .arg("http")
            .env("TRACEDB_URL", url)
            .env("TRACEDB_TOKEN", "dev-token")
            .env("TRACEDB_DATABASE_ID", "db_local")
            .env("TRACEDB_BRANCH_ID", "db_local:main")
            .env("TRACEDB_TIMEOUT_MS", "500")
            .env("TRACEDB_SAFE_RETRIES", "0")
            .env("TRACEDB_WAIT_READY_MS", "1000")
            .output()
            .expect("run tracedb doctor http from environment");
        last_stdout = String::from_utf8_lossy(&output.stdout).to_string();
        last_stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if output.status.success() {
            let summary: Value = serde_json::from_slice(&output.stdout).expect("doctor json");
            if summary["ok"] == true {
                return summary;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("doctor http env did not report ok\nstdout:\n{last_stdout}\nstderr:\n{last_stderr}");
}
