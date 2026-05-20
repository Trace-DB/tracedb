use serde_json::Value;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
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
fn product_regression_runs_local_product_gate() {
    let temp = tempfile::tempdir().expect("tempdir");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(temp.path())
        .output()
        .expect("run tracedb product-regression");
    assert!(
        output.status.success(),
        "product-regression failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("product-regression json");
    assert_eq!(summary["ok"], true);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
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
fn product_regression_injected_failure_exits_nonzero_and_preserves_json_summary() {
    let temp = tempfile::tempdir().expect("tempdir");
    let output = Command::new(env!("CARGO_BIN_EXE_tracedb"))
        .arg("product-regression")
        .arg("--data-root")
        .arg(temp.path())
        .arg("--skip-typescript")
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
    assert_eq!(summary["ok"], false);
    assert_eq!(summary["mode"], "local-product-regression");
    assert_eq!(summary["scope"], "local_only");
    assert_eq!(summary["failure_injection"], "embedded_demo");
    assert_eq!(summary["claims"]["sql_module"], "not_implemented");
    assert_eq!(summary["claims"]["managed_cloud"], "not_checked");
    assert_eq!(summary["claims"]["benchmark"], "not_checked");
    assert_eq!(summary["steps"]["embedded_demo"]["ok"], false);
    assert_eq!(summary["steps"]["embedded_demo"]["injected_failure"], true);
    assert_eq!(
        summary["steps"]["embedded_demo"]["error"],
        "injected product-regression failure"
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
