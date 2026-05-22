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
    assert_eq!(summary["mode"], "local-http-sdk-demo");
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
        "local product regression passed: 8/8 steps"
    );
    assert_eq!(summary["human_summary"]["steps_passed"], 8);
    assert_eq!(summary["human_summary"]["steps_total"], 8);
    assert_eq!(summary["human_summary"]["failed_step"], Value::Null);
    for step in [
        "embedded_demo",
        "embedded_verify",
        "http_demo",
        "local_doctor",
        "rust_sdk_quickstart",
        "typescript_check",
        "typescript_http_smoke",
        "typescript_gateway_smoke",
    ] {
        assert_eq!(
            summary["steps"][step]["ok"], true,
            "product-regression step {step} should pass: {summary}"
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
fn product_quickstart_skip_typescript_uses_default_report_file_and_marks_reduced_evidence() {
    let _smoke_lock = PRODUCT_REGRESSION_SMOKE_LOCK
        .lock()
        .expect("lock product regression smoke path");
    let _report_lock = PRODUCT_QUICKSTART_REPORT_LOCK
        .lock()
        .expect("lock product quickstart report path");
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("quickstart-skip-typescript-data");
    let report_file = workspace_root().join("target/tracedb/product-quickstart.json");
    let _ = std::fs::remove_file(&report_file);

    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-quickstart")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--skip-typescript")
        .output()
        .expect("run tracedb product-quickstart without TypeScript tooling");

    assert!(
        output.status.success(),
        "product-quickstart --skip-typescript failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("product-quickstart skip json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(report_summary, summary);
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["report_file"], report_file.display().to_string());
    assert_eq!(summary["typescript_enabled"], false);
    assert_eq!(summary["only_step"], Value::Null);
    assert_eq!(summary["failure_injection"], Value::Null);
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    assert_eq!(summary["human_summary"]["status"], "passed");
    assert_eq!(
        summary["human_summary"]["message"],
        "local product regression passed: 5/5 steps"
    );
    assert_eq!(summary["human_summary"]["steps_passed"], 5);
    assert_eq!(summary["human_summary"]["steps_total"], 5);
    assert_eq!(summary["human_summary"]["failed_step"], Value::Null);

    let steps = summary["steps"]
        .as_object()
        .expect("product quickstart steps object");
    assert_eq!(steps.len(), 5);
    for step in [
        "embedded_demo",
        "embedded_verify",
        "http_demo",
        "local_doctor",
        "rust_sdk_quickstart",
    ] {
        assert_eq!(
            steps[step]["ok"], true,
            "product-quickstart --skip-typescript step {step} should pass: {summary}"
        );
    }
    for skipped_step in [
        "typescript_check",
        "typescript_http_smoke",
        "typescript_gateway_smoke",
    ] {
        assert!(
            !steps.contains_key(skipped_step),
            "product-quickstart --skip-typescript should skip {skipped_step}: {summary}"
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
        .arg("--skip-typescript")
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
fn product_regression_only_typescript_conflicts_with_skip_typescript() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("conflicting-typescript-only");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--skip-typescript")
        .arg("--only")
        .arg("typescript_check")
        .output()
        .expect("run tracedb product-regression with conflicting TypeScript flags");
    assert!(
        !output.status.success(),
        "product-regression --only typescript_check --skip-typescript should fail before running Node tooling\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "conflicting TypeScript flags should not emit product-regression JSON: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        !data_root.exists(),
        "conflicting TypeScript flags should fail during option parsing before creating data roots"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(
            "product-regression --only typescript_check conflicts with --skip-typescript"
        ),
        "stderr should explain conflicting TypeScript flags: {stderr}"
    );
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
        "local product regression steps listed: 8 steps; only_supported=8"
    );
    assert_eq!(summary["human_summary"]["steps_total"], 8);
    assert_eq!(summary["human_summary"]["only_supported"], 8);
    let steps = summary["steps"].as_array().expect("steps array");
    assert_eq!(steps.len(), 8);
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
            "local_doctor",
            "rust_sdk_quickstart",
            "typescript_check",
            "typescript_http_smoke",
            "typescript_gateway_smoke",
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
fn product_regression_only_rust_sdk_quickstart_runs_single_gate_step() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("only-rust-sdk-quickstart");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("rust_sdk_quickstart")
        .output()
        .expect("run tracedb product-regression Rust SDK quickstart step");
    assert!(
        output.status.success(),
        "product-regression --only rust_sdk_quickstart failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout)
        .expect("product-regression Rust SDK quickstart json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["only_step"], "rust_sdk_quickstart");
    assert!(
        summary["local_server_url"]
            .as_str()
            .is_some_and(|url| url.starts_with("http://127.0.0.1:")),
        "rust_sdk_quickstart should report managed local server url: {summary}"
    );
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 1);
    assert_eq!(summary["steps"]["rust_sdk_quickstart"]["ok"], true);
    let sdk_summary = &summary["steps"]["rust_sdk_quickstart"]["summary"];
    assert_eq!(sdk_summary["ok"], true);
    assert_eq!(sdk_summary["mode"], "rust-sdk-quickstart");
    assert!(
        sdk_summary["server_url"]
            .as_str()
            .is_some_and(|url| url.starts_with("http://127.0.0.1:")),
        "Rust SDK quickstart should report loopback server url: {sdk_summary}"
    );
    assert_eq!(sdk_summary["database_id"], Value::Null);
    assert_eq!(sdk_summary["branch_id"], Value::Null);
    assert_eq!(sdk_summary["table"], "docs");
    assert_eq!(sdk_summary["tenant_id"], "tenant-a");
    assert_eq!(sdk_summary["server_ready"], true);
    assert_eq!(sdk_summary["idempotency_retries"], 1);
    assert_eq!(sdk_summary["idempotency_keys"], true);
    assert_eq!(sdk_summary["steps"]["health"], true);
    assert_eq!(sdk_summary["steps"]["catalog"], true);
    assert_eq!(sdk_summary["steps"]["metrics"], true);
    assert_eq!(sdk_summary["steps"]["schema_apply"], true);
    assert_eq!(sdk_summary["steps"]["batch_ingest"], true);
    assert_eq!(sdk_summary["steps"]["patch"], true);
    assert_eq!(sdk_summary["steps"]["query"], true);
    assert_eq!(sdk_summary["steps"]["delete"], true);
    assert_eq!(sdk_summary["steps"]["compact"], true);
    assert_eq!(sdk_summary["steps"]["snapshot"], true);
    assert_eq!(sdk_summary["steps"]["restore"], true);
    assert_eq!(sdk_summary["steps"]["jobs"], true);
    assert_eq!(sdk_summary["health_ok"], true);
    assert!(sdk_summary["database_count"].as_u64().is_some());
    assert!(sdk_summary["branch_count"].as_u64().is_some());
    assert!(sdk_summary["metrics_latest_epoch"].as_u64().is_some());
    assert!(sdk_summary["admin_job_count"].as_u64().is_some());
    assert_eq!(sdk_summary["admin"]["requested"], true);
    assert_eq!(sdk_summary["admin"]["compact"], true);
    assert_eq!(sdk_summary["admin"]["snapshot"], true);
    assert_eq!(sdk_summary["admin"]["restore"], true);
    assert_eq!(sdk_summary["patched"], true);
    assert_eq!(sdk_summary["patched_status"], "reviewed");
    assert_eq!(sdk_summary["sql_module"], "not_implemented");
    let snapshot_target = sdk_summary["snapshot_target"]
        .as_str()
        .expect("snapshot target path");
    let restore_target = sdk_summary["restore_target"]
        .as_str()
        .expect("restore target path");
    assert!(Path::new(snapshot_target).starts_with(data_root.join("sdk-admin")));
    assert!(Path::new(restore_target).starts_with(data_root.join("sdk-admin")));
}

#[test]
fn product_regression_rust_sdk_quickstart_failure_preserves_child_summary() {
    let temp = tempfile::tempdir().expect("tempdir");
    let report_file = temp.path().join("failing-rust-sdk-quickstart.json");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .current_dir(temp.path())
        .arg("product-regression")
        .arg("--data-root")
        .arg("relative-product-regression-root")
        .arg("--report-file")
        .arg(&report_file)
        .arg("--only")
        .arg("rust_sdk_quickstart")
        .output()
        .expect("run tracedb product-regression failing Rust SDK quickstart step");
    assert!(
        !output.status.success(),
        "product-regression --only rust_sdk_quickstart with child config failure should exit nonzero\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout)
        .expect("product-regression failing Rust SDK quickstart json");
    let report_summary = read_json_file(&report_file);
    assert_eq!(report_summary, summary);
    assert_eq!(summary["ok"], false);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["report_file"], report_file.display().to_string());
    assert_eq!(summary["only_step"], "rust_sdk_quickstart");
    assert_eq!(summary["failure_injection"], Value::Null);
    assert_eq!(summary["human_summary"]["status"], "failed");
    assert_eq!(
        summary["human_summary"]["failed_step"],
        "rust_sdk_quickstart"
    );
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 1);
    let step = &summary["steps"]["rust_sdk_quickstart"];
    assert_eq!(step["ok"], false);
    assert_eq!(step["exit_code"], 1);
    assert!(
        step["stderr_tail"].as_str().is_some_and(
            |stderr| stderr.contains("--admin-dir must be an absolute server-side path")
        ),
        "failing Rust SDK child should retain stderr tail: {summary}"
    );
    let sdk_summary = &step["summary"];
    assert_eq!(sdk_summary["ok"], false);
    assert_eq!(sdk_summary["mode"], "rust-sdk-quickstart");
    assert_eq!(sdk_summary["phase"], "config");
    assert_eq!(sdk_summary["error"]["kind"], "configuration");
    assert!(
        sdk_summary["error"]["message"].as_str().is_some_and(
            |message| message.contains("--admin-dir must be an absolute server-side path")
        ),
        "failing Rust SDK child summary should preserve the quickstart error: {summary}"
    );
    assert_eq!(sdk_summary["admin"]["requested"], true);
    assert_eq!(sdk_summary["steps"]["ready"], false);
    assert_eq!(sdk_summary["sql_module"], "not_implemented");
}

#[test]
fn product_regression_only_typescript_check_runs_single_gate_step() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("only-typescript-check");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("typescript_check")
        .output()
        .expect("run tracedb product-regression TypeScript check step");
    assert!(
        output.status.success(),
        "product-regression --only typescript_check failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("product-regression TypeScript check json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["only_step"], "typescript_check");
    assert_eq!(summary["local_server_url"], Value::Null);
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 1);
    assert_eq!(summary["steps"]["typescript_check"]["ok"], true);
    assert_eq!(
        summary["steps"]["typescript_check"]["command"],
        "npm run check"
    );
    assert!(
        summary["steps"]["typescript_check"]["cwd"]
            .as_str()
            .is_some_and(|cwd| cwd.ends_with("clients/typescript")),
        "typescript_check should run inside clients/typescript: {summary}"
    );
}

#[test]
fn product_regression_only_typescript_http_smoke_runs_single_gate_step() {
    let _smoke_lock = PRODUCT_REGRESSION_SMOKE_LOCK
        .lock()
        .expect("lock product regression smoke path");
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("only-typescript-http-smoke");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("typescript_http_smoke")
        .output()
        .expect("run tracedb product-regression TypeScript HTTP smoke step");
    assert!(
        output.status.success(),
        "product-regression --only typescript_http_smoke failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout)
        .expect("product-regression TypeScript HTTP smoke json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["only_step"], "typescript_http_smoke");
    assert_eq!(summary["local_server_url"], Value::Null);
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 1);
    assert_eq!(summary["steps"]["typescript_http_smoke"]["ok"], true);
    assert_eq!(
        summary["steps"]["typescript_http_smoke"]["command"],
        "npm run http-smoke"
    );
    assert!(
        summary["steps"]["typescript_http_smoke"]["cwd"]
            .as_str()
            .is_some_and(|cwd| cwd.ends_with("clients/typescript")),
        "typescript_http_smoke should run inside clients/typescript: {summary}"
    );
    let smoke_summary = &summary["steps"]["typescript_http_smoke"]["summary"];
    assert_eq!(smoke_summary["ok"], true);
    assert_eq!(smoke_summary["mode"], "local-http-typescript-smoke");
    assert_eq!(smoke_summary["steps"]["schema_apply"], true);
    assert_eq!(smoke_summary["steps"]["batch_ingest"], true);
    assert_eq!(smoke_summary["steps"]["query"], true);
    assert_eq!(smoke_summary["steps"]["explain"], true);
    assert_eq!(smoke_summary["steps"]["delete"], true);
    assert_eq!(smoke_summary["steps"]["compact"], true);
    assert_eq!(smoke_summary["steps"]["snapshot"], true);
    assert_eq!(smoke_summary["steps"]["restore"], true);
    assert_eq!(smoke_summary["records_inserted"], 3);
    assert_eq!(smoke_summary["records_scanned"], 3);
    assert_eq!(smoke_summary["deleted_hidden"], true);
    assert_eq!(smoke_summary["sql_module"], "not_implemented");
}

#[test]
fn product_regression_only_typescript_gateway_smoke_runs_single_gate_step() {
    let _smoke_lock = PRODUCT_REGRESSION_SMOKE_LOCK
        .lock()
        .expect("lock product regression smoke path");
    let temp = tempfile::tempdir().expect("tempdir");
    let data_root = temp.path().join("only-typescript-gateway-smoke");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(&data_root)
        .arg("--only")
        .arg("typescript_gateway_smoke")
        .output()
        .expect("run tracedb product-regression TypeScript gateway smoke step");
    assert!(
        output.status.success(),
        "product-regression --only typescript_gateway_smoke failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout)
        .expect("product-regression TypeScript gateway smoke json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["only_step"], "typescript_gateway_smoke");
    assert_eq!(summary["local_server_url"], Value::Null);
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    let steps = summary["steps"].as_object().expect("steps object");
    assert_eq!(steps.len(), 1);
    assert_eq!(summary["steps"]["typescript_gateway_smoke"]["ok"], true);
    assert_eq!(
        summary["steps"]["typescript_gateway_smoke"]["command"],
        "npm run gateway-smoke"
    );
    assert!(
        summary["steps"]["typescript_gateway_smoke"]["cwd"]
            .as_str()
            .is_some_and(|cwd| cwd.ends_with("clients/typescript")),
        "typescript_gateway_smoke should run inside clients/typescript: {summary}"
    );
    let smoke_summary = &summary["steps"]["typescript_gateway_smoke"]["summary"];
    assert_eq!(smoke_summary["ok"], true);
    assert_eq!(smoke_summary["mode"], "local-gateway-typescript-smoke");
    assert_eq!(smoke_summary["token_required"], true);
    assert_eq!(smoke_summary["token_enforcement"], true);
    assert_eq!(smoke_summary["routing_enforcement"], true);
    assert_eq!(smoke_summary["database_id"], "db_local");
    assert_eq!(smoke_summary["branch_id"], "db_local:main");
    assert_eq!(
        smoke_summary["quickstart_mode"],
        "typescript-endpoint-quickstart"
    );
    assert_eq!(smoke_summary["quickstart_steps"]["patch"], true);
    assert_eq!(smoke_summary["patched"], true);
    assert_eq!(smoke_summary["patched_status"], "reviewed");
    assert_eq!(smoke_summary["deleted_hidden"], true);
    assert_eq!(smoke_summary["sql_module"], "not_implemented");
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
