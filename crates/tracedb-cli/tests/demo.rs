use serde_json::Value;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

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
