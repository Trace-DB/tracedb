#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::TcpStream;
use tracedb_jobs::{JobCatalog, TraceJob, WorkerId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerRunReport {
    pub leased_job_id: Option<String>,
    pub engine_url: String,
    pub used_private_engine_api: bool,
    pub engine_health_checked: bool,
    pub engine_status_code: u16,
}

pub fn run_once_through_engine_api(
    _jobs: &mut JobCatalog,
    worker_id: WorkerId,
    engine_url: &str,
) -> Result<WorkerRunReport, String> {
    let token = engine_internal_token_from_env();
    let status = probe_private_engine_health_with_token(engine_url, token.as_deref())?;
    let leased =
        lease_private_engine_job(engine_url, token.as_deref(), &worker_id)?.map(|job| job.job_id);
    Ok(WorkerRunReport {
        leased_job_id: leased,
        engine_url: engine_url.to_string(),
        used_private_engine_api: true,
        engine_health_checked: true,
        engine_status_code: status,
    })
}

pub fn probe_private_engine_health(engine_url: &str) -> Result<u16, String> {
    probe_private_engine_health_with_token(engine_url, engine_internal_token_from_env().as_deref())
}

pub fn probe_private_engine_health_with_token(
    engine_url: &str,
    internal_token: Option<&str>,
) -> Result<u16, String> {
    let target = EngineTarget::parse(engine_url)?;
    if !target.is_private() {
        return Err("worker must use private engine API".to_string());
    }
    let response = http_request(&target, "GET", "/internal/health", None, internal_token)?;
    let status = parse_status(&response)?;
    if status != 200 {
        return Err(format!("private engine health returned HTTP {status}"));
    }
    let response_text = String::from_utf8_lossy(&response);
    if !response_text.contains("\"ok\":true") && !response_text.contains("\"ok\": true") {
        return Err("private engine health response was not ok".to_string());
    }
    Ok(status)
}

fn lease_private_engine_job(
    engine_url: &str,
    internal_token: Option<&str>,
    worker_id: &WorkerId,
) -> Result<Option<TraceJob>, String> {
    let target = EngineTarget::parse(engine_url)?;
    if !target.is_private() {
        return Err("worker must use private engine API".to_string());
    }
    let body = serde_json::json!({
        "worker_id": worker_id.0,
        "kind": "VerifyDatabase",
        "lease_ms": 30000_u64,
    })
    .to_string();
    let response = http_request(
        &target,
        "POST",
        "/internal/jobs/lease",
        Some(&body),
        internal_token,
    )?;
    let status = parse_status(&response)?;
    if status != 200 {
        return Err(format!("private engine job lease returned HTTP {status}"));
    }
    let response_text = String::from_utf8_lossy(&response);
    let body = response_text
        .split("\r\n\r\n")
        .nth(1)
        .or_else(|| response_text.split("\n\n").nth(1))
        .ok_or_else(|| "private engine job lease response body missing".to_string())?;
    let value: serde_json::Value = serde_json::from_str(body).map_err(|error| error.to_string())?;
    if !value
        .get("leased")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let job = serde_json::from_value(value.get("job").cloned().unwrap_or_default())
        .map_err(|error| error.to_string())?;
    Ok(Some(job))
}

fn http_request(
    target: &EngineTarget,
    method: &str,
    path: &str,
    body: Option<&str>,
    internal_token: Option<&str>,
) -> Result<Vec<u8>, String> {
    let mut stream = TcpStream::connect(&target.address).map_err(|error| error.to_string())?;
    let path = target.join(path);
    let token_header = internal_token
        .map(|token| format!("x-tracedb-engine-token: {token}\r\n"))
        .unwrap_or_default();
    let body = body.unwrap_or_default();
    let content_type = if body.is_empty() {
        String::new()
    } else {
        "content-type: application/json\r\n".to_string()
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nhost: {}\r\n{token_header}{content_type}content-length: {}\r\nconnection: close\r\n\r\n{}",
        target.host,
        body.len(),
        body
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;
    stream.flush().map_err(|error| error.to_string())?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| error.to_string())?;
    Ok(response)
}

fn engine_internal_token_from_env() -> Option<String> {
    std::env::var("TRACEDB_ENGINE_INTERNAL_TOKEN")
        .ok()
        .or_else(|| std::env::var("TRACEDB_ENGINE_TOKEN").ok())
        .filter(|token| !token.trim().is_empty())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EngineTarget {
    host: String,
    address: String,
    base_path: String,
}

impl EngineTarget {
    fn parse(url: &str) -> Result<Self, String> {
        let without_scheme = url
            .strip_prefix("http://")
            .ok_or_else(|| "worker engine URL must use http://".to_string())?;
        let (host, base_path) = without_scheme
            .split_once('/')
            .map(|(host, path)| (host, format!("/{path}")))
            .unwrap_or((without_scheme, "/".to_string()));
        if host.trim().is_empty() {
            return Err("worker engine URL host cannot be empty".to_string());
        }
        let address = if host.contains(':') {
            host.to_string()
        } else {
            format!("{host}:80")
        };
        Ok(Self {
            host: host.to_string(),
            address,
            base_path,
        })
    }

    fn is_private(&self) -> bool {
        self.host.contains(".railway.internal")
            || self.host.starts_with("127.0.0.1")
            || self.host.starts_with("localhost")
            || self.host.starts_with("[::1]")
            || self.host == "tracedb-engine"
            || self.host.ends_with(".local")
    }

    fn join(&self, path: &str) -> String {
        if self.base_path == "/" {
            path.to_string()
        } else {
            format!(
                "{}/{}",
                self.base_path.trim_end_matches('/'),
                path.trim_start_matches('/')
            )
        }
    }
}

fn parse_status(response: &[u8]) -> Result<u16, String> {
    let text = String::from_utf8_lossy(response);
    let status_line = text.lines().next().unwrap_or_default();
    status_line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| "private engine response status was invalid".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn worker_leases_job_through_private_engine_jobs_api() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        listener
            .set_nonblocking(false)
            .expect("listener blocking mode");
        let addr = listener.local_addr().expect("addr");
        let seen_paths = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_paths_for_thread = Arc::clone(&seen_paths);
        thread::spawn(move || {
            for _ in 0..2 {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("read timeout");
                let mut buffer = [0u8; 4096];
                let read = stream.read(&mut buffer).expect("read request");
                let request = String::from_utf8_lossy(&buffer[..read]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                seen_paths_for_thread.lock().unwrap().push(path.clone());
                let body = if path == "/internal/jobs/lease" {
                    serde_json::json!({
                        "leased": true,
                        "job": {
                            "job_id": "job:verify_database:startup-verify",
                            "kind": "VerifyDatabase",
                            "target": "startup",
                            "idempotency_key": "startup-verify",
                            "lease_owner": "worker-test",
                            "attempts": 1,
                            "max_attempts": 3,
                            "status": "Leased",
                            "last_error": null,
                            "lease_token": "lease-token",
                            "lease_expires_at_ms": 1000
                        }
                    })
                    .to_string()
                } else {
                    serde_json::json!({ "ok": true }).to_string()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write response");
            }
        });

        let mut jobs = JobCatalog::default();
        let report = run_once_through_engine_api(
            &mut jobs,
            WorkerId::new("worker-test"),
            &format!("http://127.0.0.1:{}", addr.port()),
        )
        .expect("worker report");

        assert_eq!(
            report.leased_job_id.as_deref(),
            Some("job:verify_database:startup-verify")
        );
        assert!(seen_paths
            .lock()
            .unwrap()
            .iter()
            .any(|path| path == "/internal/jobs/lease"));
    }
}
