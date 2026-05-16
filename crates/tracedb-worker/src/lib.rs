#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::TcpStream;
use tracedb_jobs::{JobCatalog, JobKind, WorkerId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerRunReport {
    pub leased_job_id: Option<String>,
    pub engine_url: String,
    pub used_private_engine_api: bool,
    pub engine_health_checked: bool,
    pub engine_status_code: u16,
}

pub fn run_once_through_engine_api(
    jobs: &mut JobCatalog,
    worker_id: WorkerId,
    engine_url: &str,
) -> Result<WorkerRunReport, String> {
    let status = probe_private_engine_health(engine_url)?;
    let leased = jobs
        .lease_next(worker_id, JobKind::VerifyDatabase)?
        .map(|job| job.job_id);
    Ok(WorkerRunReport {
        leased_job_id: leased,
        engine_url: engine_url.to_string(),
        used_private_engine_api: true,
        engine_health_checked: true,
        engine_status_code: status,
    })
}

pub fn probe_private_engine_health(engine_url: &str) -> Result<u16, String> {
    let target = EngineTarget::parse(engine_url)?;
    if !target.is_private() {
        return Err("worker must use private engine API".to_string());
    }
    let mut stream = TcpStream::connect(&target.address).map_err(|error| error.to_string())?;
    let path = target.join("/internal/health");
    let request = format!(
        "GET {path} HTTP/1.1\r\nhost: {}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        target.host
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;
    stream.flush().map_err(|error| error.to_string())?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| error.to_string())?;
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
