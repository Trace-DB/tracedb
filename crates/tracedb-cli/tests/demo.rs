use serde_json::Value;
use std::process::Command;

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
