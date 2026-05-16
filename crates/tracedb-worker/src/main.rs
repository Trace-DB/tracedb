#![forbid(unsafe_code)]

use tracedb_jobs::{JobCatalog, JobKind, WorkerId};

static NEXT_REQUEST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn main() {
    if std::env::var("TRACEDB_WORKER_ONCE").as_deref() != Ok("true") {
        if let Err(error) = serve_healthcheck_worker() {
            eprintln!("tracedb-worker: {error}");
            std::process::exit(1);
        }
        return;
    }
    run_once_or_exit();
}

fn run_once_or_exit() {
    let engine_url = std::env::var("TRACEDB_ENGINE_URL")
        .or_else(|_| std::env::var("TRACEDB_WORKER_ENGINE_URL"))
        .unwrap_or_else(|_| "http://tracedb-engine.railway.internal:8080".to_string());
    let mut jobs = JobCatalog::default();
    let _ = jobs.enqueue(JobKind::VerifyDatabase, "startup", "startup-verify");
    match tracedb_worker::run_once_through_engine_api(
        &mut jobs,
        WorkerId::new("worker-main"),
        &engine_url,
    ) {
        Ok(report) => println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "engine_url": report.engine_url,
                "used_private_engine_api": report.used_private_engine_api,
                "engine_health_checked": report.engine_health_checked,
                "engine_status_code": report.engine_status_code,
                "leased_job_id": report.leased_job_id,
            })
        ),
        Err(error) => {
            eprintln!("tracedb-worker: {error}");
            std::process::exit(1);
        }
    }
}

fn serve_healthcheck_worker() -> std::io::Result<()> {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    let bind = bind_addr_from_env();
    let engine_url = std::env::var("TRACEDB_ENGINE_URL")
        .or_else(|_| std::env::var("TRACEDB_WORKER_ENGINE_URL"))
        .unwrap_or_else(|_| "http://tracedb-engine.railway.internal:8080".to_string());
    let listener = TcpListener::bind(bind)?;
    listener.set_nonblocking(true)?;

    loop {
        match listener.accept() {
            Ok((mut stream, _addr)) => {
                let mut buffer = [0u8; 4096];
                let read = stream.read(&mut buffer)?;
                let request = String::from_utf8_lossy(&buffer[..read]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");
                let request_id = header_value(&request, "x-request-id")
                    .map(str::to_string)
                    .unwrap_or_else(next_request_id);
                println!(
                    "{}",
                    serde_json::json!({
                        "service": "tracedb-worker",
                        "request_id": request_id,
                        "path": path,
                    })
                );
                let (status, body) = if matches!(path, "/health" | "/ready") {
                    match tracedb_worker::probe_private_engine_health(&engine_url) {
                        Ok(status) => (
                            "200 OK",
                            serde_json::json!({
                                "ok": true,
                                "ready": true,
                                "service": "tracedb-worker",
                                "mode": "queue-worker",
                                "mutates_through_private_engine_api": true,
                                "engine_health_checked": true,
                                "engine_status_code": status,
                            })
                            .to_string(),
                        ),
                        Err(error) => (
                            "503 Service Unavailable",
                            serde_json::json!({
                                "ok": false,
                                "service": "tracedb-worker",
                                "mode": "queue-worker",
                                "mutates_through_private_engine_api": true,
                                "engine_health_checked": true,
                                "error": error,
                            })
                            .to_string(),
                        ),
                    }
                } else if path == "/metrics" {
                    (
                        "200 OK",
                        serde_json::json!({
                            "service": "tracedb-worker",
                            "worker_loops": 0,
                            "leased_jobs": 0,
                            "completed_jobs": 0,
                            "engine_url": engine_url,
                        })
                        .to_string(),
                    )
                } else {
                    (
                        "404 Not Found",
                        serde_json::json!({ "error": "not found" }).to_string(),
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes())?;
                stream.flush()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => return Err(error),
        }
    }
}

fn header_value<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().skip(1).find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

fn next_request_id() -> String {
    format!(
        "worker-{}-{}",
        std::process::id(),
        NEXT_REQUEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

fn bind_addr_from_env() -> String {
    std::env::var("TRACEDB_BIND").unwrap_or_else(|_| {
        std::env::var("PORT")
            .map(|port| format!("0.0.0.0:{port}"))
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
    })
}
