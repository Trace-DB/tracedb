#![forbid(unsafe_code)]

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tracedb_jobs::{JobCatalog, TraceJob, WorkerId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerRunReport {
    pub leased_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_job_id: Option<String>,
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
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(run_once_through_engine_api_async(
            _jobs, worker_id, engine_url,
        ))
}

pub async fn run_once_through_engine_api_async(
    _jobs: &mut JobCatalog,
    worker_id: WorkerId,
    engine_url: &str,
) -> Result<WorkerRunReport, String> {
    let client = private_engine_client()?;
    let token = engine_internal_token_from_env();
    let status =
        probe_private_engine_health_with_client(&client, engine_url, token.as_deref()).await?;
    let leased =
        lease_private_engine_job(&client, engine_url, token.as_deref(), &worker_id).await?;
    let mut heartbeat_job_id = None;
    let mut completed_job_id = None;
    let mut failed_job_id = None;
    if let Some(job) = leased.as_ref() {
        if let Some(lease_token) = job.lease_token.as_deref() {
            let heartbeat = heartbeat_private_engine_job(
                &client,
                engine_url,
                token.as_deref(),
                job,
                lease_token,
            )
            .await?;
            heartbeat_job_id = heartbeat.map(|job| job.job_id);
            match execute_private_engine_job(&client, engine_url, token.as_deref(), job).await? {
                WorkerJobOutcome::Succeeded => {
                    let completed = complete_private_engine_job(
                        &client,
                        engine_url,
                        token.as_deref(),
                        job,
                        lease_token,
                    )
                    .await?;
                    completed_job_id = completed.map(|job| job.job_id);
                }
                WorkerJobOutcome::Failed { error, permanent } => {
                    let failed = fail_private_engine_job(
                        &client,
                        engine_url,
                        token.as_deref(),
                        job,
                        lease_token,
                        &error,
                        permanent,
                    )
                    .await?;
                    failed_job_id = failed.map(|job| job.job_id);
                }
            }
        }
    }
    Ok(WorkerRunReport {
        leased_job_id: leased.map(|job| job.job_id),
        heartbeat_job_id,
        completed_job_id,
        failed_job_id,
        engine_url: engine_url.to_string(),
        used_private_engine_api: true,
        engine_health_checked: true,
        engine_status_code: status,
    })
}

pub fn probe_private_engine_health(engine_url: &str) -> Result<u16, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(probe_private_engine_health_async(engine_url))
}

pub async fn probe_private_engine_health_async(engine_url: &str) -> Result<u16, String> {
    let client = private_engine_client()?;
    probe_private_engine_health_with_client(
        &client,
        engine_url,
        engine_internal_token_from_env().as_deref(),
    )
    .await
}

pub async fn probe_private_engine_health_with_token(
    engine_url: &str,
    internal_token: Option<&str>,
) -> Result<u16, String> {
    let client = private_engine_client()?;
    probe_private_engine_health_with_client(&client, engine_url, internal_token).await
}

async fn probe_private_engine_health_with_client(
    client: &reqwest::Client,
    engine_url: &str,
    internal_token: Option<&str>,
) -> Result<u16, String> {
    let target = EngineTarget::parse(engine_url)?;
    if !target.is_private() {
        return Err("worker must use private engine API".to_string());
    }
    let response = engine_request(
        client,
        &target,
        "GET",
        "/internal/health",
        None,
        internal_token,
    )
    .await?;
    let status = response.status();
    if status != StatusCode::OK {
        return Err(format!("private engine health returned HTTP {status}"));
    }
    let value: Value = response
        .json()
        .await
        .map_err(|error| format!("private engine health response was not JSON: {error}"))?;
    if !value.get("ok").and_then(Value::as_bool).unwrap_or(false) {
        return Err("private engine health response was not ok".to_string());
    }
    Ok(status.as_u16())
}

enum WorkerJobOutcome {
    Succeeded,
    Failed { error: String, permanent: bool },
}

async fn execute_private_engine_job(
    client: &reqwest::Client,
    engine_url: &str,
    internal_token: Option<&str>,
    job: &TraceJob,
) -> Result<WorkerJobOutcome, String> {
    match &job.kind {
        tracedb_jobs::JobKind::VerifyDatabase => {
            probe_private_engine_health_with_client(client, engine_url, internal_token).await?;
            Ok(WorkerJobOutcome::Succeeded)
        }
        tracedb_jobs::JobKind::CompactSegment => {
            let target = EngineTarget::parse(engine_url)?;
            if !target.is_private() {
                return Err("worker must use private engine API".to_string());
            }
            let response = engine_request(
                client,
                &target,
                "POST",
                "/v1/admin/compact",
                Some(serde_json::json!({})),
                internal_token,
            )
            .await?;
            if response.status() != StatusCode::OK {
                return Ok(WorkerJobOutcome::Failed {
                    error: format!("compact job returned HTTP {}", response.status()),
                    permanent: false,
                });
            }
            Ok(WorkerJobOutcome::Succeeded)
        }
        other => Ok(WorkerJobOutcome::Failed {
            error: format!("worker does not implement job kind {other:?}"),
            permanent: false,
        }),
    }
}

async fn lease_private_engine_job(
    client: &reqwest::Client,
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
    });
    let response = engine_request(
        client,
        &target,
        "POST",
        "/internal/jobs/lease",
        Some(body),
        internal_token,
    )
    .await?;
    let status = response.status();
    if status != StatusCode::OK {
        return Err(format!("private engine job lease returned HTTP {status}"));
    }
    let value: Value = response.json().await.map_err(|error| error.to_string())?;
    if !value
        .get("leased")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let Some(job_value) = value.get("job").filter(|value| value.is_object()) else {
        return Ok(None);
    };
    let job = serde_json::from_value(job_value.clone()).map_err(|error| error.to_string())?;
    Ok(Some(job))
}

async fn heartbeat_private_engine_job(
    client: &reqwest::Client,
    engine_url: &str,
    internal_token: Option<&str>,
    job: &TraceJob,
    lease_token: &str,
) -> Result<Option<TraceJob>, String> {
    job_command(
        client,
        engine_url,
        internal_token,
        "/internal/jobs/heartbeat",
        serde_json::json!({
            "job_id": job.job_id,
            "lease_token": lease_token,
            "lease_ms": 30000_u64,
        }),
        "private engine job heartbeat",
    )
    .await
}

async fn complete_private_engine_job(
    client: &reqwest::Client,
    engine_url: &str,
    internal_token: Option<&str>,
    job: &TraceJob,
    lease_token: &str,
) -> Result<Option<TraceJob>, String> {
    job_command(
        client,
        engine_url,
        internal_token,
        "/internal/jobs/complete",
        serde_json::json!({
            "job_id": job.job_id,
            "lease_token": lease_token,
        }),
        "private engine job complete",
    )
    .await
}

async fn fail_private_engine_job(
    client: &reqwest::Client,
    engine_url: &str,
    internal_token: Option<&str>,
    job: &TraceJob,
    lease_token: &str,
    error: &str,
    permanent: bool,
) -> Result<Option<TraceJob>, String> {
    job_command(
        client,
        engine_url,
        internal_token,
        "/internal/jobs/fail",
        serde_json::json!({
            "job_id": job.job_id,
            "lease_token": lease_token,
            "error": error,
            "permanent": permanent,
        }),
        "private engine job fail",
    )
    .await
}

async fn job_command(
    client: &reqwest::Client,
    engine_url: &str,
    internal_token: Option<&str>,
    path: &str,
    body: Value,
    label: &str,
) -> Result<Option<TraceJob>, String> {
    let target = EngineTarget::parse(engine_url)?;
    if !target.is_private() {
        return Err("worker must use private engine API".to_string());
    }
    let response =
        engine_request(client, &target, "POST", path, Some(body), internal_token).await?;
    let status = response.status();
    if status != StatusCode::OK {
        return Err(format!("{label} returned HTTP {status}"));
    }
    let value: Value = response.json().await.map_err(|error| error.to_string())?;
    let Some(job_value) = value.get("job").filter(|value| value.is_object()) else {
        return Ok(None);
    };
    serde_json::from_value(job_value.clone())
        .map(Some)
        .map_err(|error| error.to_string())
}

async fn engine_request(
    client: &reqwest::Client,
    target: &EngineTarget,
    method: &str,
    path: &str,
    body: Option<Value>,
    internal_token: Option<&str>,
) -> Result<reqwest::Response, String> {
    let method =
        reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?;
    let mut request = client.request(method, target.join(path));
    if let Some(token) = internal_token {
        request = request.header("x-tracedb-engine-token", token);
    }
    if let Some(body) = body {
        request = request.json(&body);
    } else {
        request = request.header("content-length", "0");
    }
    request.send().await.map_err(|error| error.to_string())
}

fn private_engine_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .pool_max_idle_per_host(16)
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| error.to_string())
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
    base_path: String,
    base_url: String,
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
        Ok(Self {
            host: host.to_string(),
            base_path,
            base_url: format!("http://{host}"),
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
        let path = if self.base_path == "/" {
            path.to_string()
        } else {
            format!(
                "{}/{}",
                self.base_path.trim_end_matches('/'),
                path.trim_start_matches('/')
            )
        };
        format!("{}{}", self.base_url, path)
    }
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
    fn worker_leases_heartbeats_and_completes_job_through_async_private_engine_api() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        listener
            .set_nonblocking(false)
            .expect("listener blocking mode");
        let addr = listener.local_addr().expect("addr");
        let seen_paths = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_paths_for_thread = Arc::clone(&seen_paths);
        let server = thread::spawn(move || {
            for _ in 0..5 {
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
                let body = match path.as_str() {
                    "/internal/jobs/lease" => serde_json::json!({
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
                    }),
                    "/internal/jobs/heartbeat" => serde_json::json!({
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
                            "lease_expires_at_ms": 2000
                        }
                    }),
                    "/internal/health" => serde_json::json!({
                        "ok": true
                    }),
                    "/internal/jobs/complete" => serde_json::json!({
                        "job": {
                            "job_id": "job:verify_database:startup-verify",
                            "kind": "VerifyDatabase",
                            "target": "startup",
                            "idempotency_key": "startup-verify",
                            "lease_owner": "worker-test",
                            "attempts": 1,
                            "max_attempts": 3,
                            "status": "Succeeded",
                            "last_error": null,
                            "lease_token": "lease-token"
                        }
                    }),
                    _ => serde_json::json!({ "ok": true }),
                }
                .to_string();
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
        let report = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(run_once_through_engine_api_async(
                &mut jobs,
                WorkerId::new("worker-test"),
                &format!("http://127.0.0.1:{}", addr.port()),
            ))
            .expect("worker report");

        assert_eq!(
            report.leased_job_id.as_deref(),
            Some("job:verify_database:startup-verify")
        );
        assert_eq!(
            report.heartbeat_job_id.as_deref(),
            Some("job:verify_database:startup-verify")
        );
        assert_eq!(
            report.completed_job_id.as_deref(),
            Some("job:verify_database:startup-verify")
        );
        server.join().expect("server thread");
        assert_eq!(
            seen_paths.lock().unwrap().as_slice(),
            [
                "/internal/health",
                "/internal/jobs/lease",
                "/internal/jobs/heartbeat",
                "/internal/health",
                "/internal/jobs/complete"
            ]
        );
    }

    #[test]
    fn worker_fails_unimplemented_job_kind_instead_of_completing_without_work() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let seen_paths = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_paths_for_thread = Arc::clone(&seen_paths);
        let server = thread::spawn(move || {
            for _ in 0..4 {
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
                let body = match path.as_str() {
                    "/internal/jobs/lease" => serde_json::json!({
                        "leased": true,
                        "job": {
                            "job_id": "job:backup_database:backup-prod",
                            "kind": "BackupDatabase",
                            "target": "backup:prod",
                            "idempotency_key": "backup-prod",
                            "lease_owner": "worker-test",
                            "attempts": 1,
                            "max_attempts": 3,
                            "status": "Leased",
                            "last_error": null,
                            "lease_token": "lease-token",
                            "lease_expires_at_ms": 1000
                        }
                    }),
                    "/internal/jobs/heartbeat" => serde_json::json!({
                        "job": {
                            "job_id": "job:backup_database:backup-prod",
                            "kind": "BackupDatabase",
                            "target": "backup:prod",
                            "idempotency_key": "backup-prod",
                            "lease_owner": "worker-test",
                            "attempts": 1,
                            "max_attempts": 3,
                            "status": "Leased",
                            "last_error": null,
                            "lease_token": "lease-token",
                            "lease_expires_at_ms": 2000
                        }
                    }),
                    "/internal/jobs/fail" => serde_json::json!({
                        "job": {
                            "job_id": "job:backup_database:backup-prod",
                            "kind": "BackupDatabase",
                            "target": "backup:prod",
                            "idempotency_key": "backup-prod",
                            "lease_owner": "worker-test",
                            "attempts": 1,
                            "max_attempts": 3,
                            "status": "FailedRetryable",
                            "last_error": "worker does not implement job kind BackupDatabase",
                            "lease_token": "lease-token"
                        }
                    }),
                    _ => serde_json::json!({ "ok": true }),
                }
                .to_string();
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
        let report = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(run_once_through_engine_api_async(
                &mut jobs,
                WorkerId::new("worker-test"),
                &format!("http://127.0.0.1:{}", addr.port()),
            ))
            .expect("worker report");

        assert_eq!(report.completed_job_id, None);
        assert_eq!(
            report.failed_job_id.as_deref(),
            Some("job:backup_database:backup-prod")
        );
        server.join().expect("server thread");
        assert_eq!(
            seen_paths.lock().unwrap().as_slice(),
            [
                "/internal/health",
                "/internal/jobs/lease",
                "/internal/jobs/heartbeat",
                "/internal/jobs/fail"
            ]
        );
    }

    #[test]
    fn worker_exposes_async_private_health_probe() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let addr = listener.local_addr().expect("addr");
        let seen_paths = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_paths_for_thread = Arc::clone(&seen_paths);
        let server = thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut buffer = [0u8; 4096];
            let read = stream.read(&mut buffer).expect("read request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_string();
            seen_paths_for_thread.lock().unwrap().push(path);
            let body = serde_json::json!({ "ok": true }).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });

        let status = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(probe_private_engine_health_async(&format!(
                "http://127.0.0.1:{}",
                addr.port()
            )))
            .expect("health");

        assert_eq!(status, 200);
        server.join().expect("server thread");
        assert!(seen_paths
            .lock()
            .unwrap()
            .iter()
            .any(|path| path == "/internal/health"));
    }
}
