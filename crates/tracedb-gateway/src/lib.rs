#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tracedb_catalog::Catalog;
use tracedb_metering::{MeterKind, UsageMeter};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatewayRequest {
    pub database_id: String,
    pub branch_id: String,
    pub path: String,
    pub bearer_token: Option<String>,
}

impl GatewayRequest {
    pub fn query(database_id: impl Into<String>, branch_id: impl Into<String>) -> Self {
        Self {
            database_id: database_id.into(),
            branch_id: branch_id.into(),
            path: "/v1/query".to_string(),
            bearer_token: None,
        }
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EngineTarget {
    pub service_name: String,
    pub url: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatewayResponse {
    pub engine_target: EngineTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineHttpResponse {
    pub status_code: u16,
    pub reason: String,
    pub content_type: String,
    pub body: Vec<u8>,
}

impl EngineHttpResponse {
    fn to_http_response(&self) -> String {
        format!(
            "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\n\r\n{}",
            self.status_code,
            self.reason,
            self.content_type,
            self.body.len(),
            String::from_utf8_lossy(&self.body)
        )
    }
}

#[derive(Clone, Debug)]
pub struct Gateway {
    catalog: Catalog,
    required_token: Option<String>,
    engine_url: String,
}

impl Gateway {
    pub fn new(catalog: Catalog, required_token: impl Into<String>) -> Self {
        Self {
            catalog,
            required_token: Some(required_token.into()),
            engine_url: "http://tracedb-engine.railway.internal:8080".to_string(),
        }
    }

    pub fn open(catalog: Catalog) -> Self {
        Self {
            catalog,
            required_token: None,
            engine_url: "http://tracedb-engine.railway.internal:8080".to_string(),
        }
    }

    pub fn with_engine_url(mut self, engine_url: impl Into<String>) -> Self {
        self.engine_url = engine_url.into();
        self
    }

    pub fn route(
        &self,
        request: GatewayRequest,
        meter: &mut UsageMeter,
    ) -> Result<GatewayResponse, String> {
        if self
            .required_token
            .as_ref()
            .is_some_and(|required| request.bearer_token.as_deref() != Some(required.as_str()))
        {
            return Err("invalid api token".to_string());
        }
        let Some(branch) = self.catalog.branch(&request.branch_id) else {
            return Err(format!("unknown branch {}", request.branch_id));
        };
        if branch.database_id != request.database_id {
            return Err(format!(
                "branch {} does not belong to database {}",
                request.branch_id, request.database_id
            ));
        }
        meter.record(MeterKind::Request, 1);
        Ok(GatewayResponse {
            engine_target: EngineTarget {
                service_name: "tracedb-engine".to_string(),
                url: self.engine_url.clone(),
            },
        })
    }
}

#[derive(Clone, Debug)]
pub struct GatewayServerConfig {
    pub bind: String,
    pub engine_url: String,
    pub required_token: Option<String>,
    pub catalog: Catalog,
    pub meter: Arc<Mutex<UsageMeter>>,
    pub rate_limit_enabled: bool,
    pub rate_limit_requests: u64,
}

impl GatewayServerConfig {
    pub fn from_env() -> Self {
        let engine_url = std::env::var("TRACEDB_ENGINE_URL")
            .unwrap_or_else(|_| "http://tracedb-engine.railway.internal:8080".to_string());
        Self {
            bind: bind_addr_from_env(),
            engine_url: engine_url.clone(),
            required_token: if std::env::var("TRACEDB_REQUIRE_API_KEY")
                .map(|value| value == "true")
                .unwrap_or(false)
            {
                Some(std::env::var("TRACEDB_API_TOKEN").unwrap_or_else(|_| "dev-token".to_string()))
            } else {
                None
            },
            catalog: load_gateway_catalog(&engine_url),
            meter: Arc::new(Mutex::new(UsageMeter::default())),
            rate_limit_enabled: std::env::var("TRACEDB_RATE_LIMIT_ENABLED")
                .map(|value| value == "true")
                .unwrap_or(false),
            rate_limit_requests: std::env::var("TRACEDB_RATE_LIMIT_REQUESTS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(60_000),
        }
    }
}

pub fn serve(config: GatewayServerConfig) -> std::io::Result<()> {
    let listener = TcpListener::bind(&config.bind)?;
    for stream in listener.incoming() {
        let stream = stream?;
        let config = config.clone();
        thread::spawn(move || {
            let _ = handle_gateway(stream, config);
        });
    }
    Ok(())
}

fn handle_gateway(mut stream: TcpStream, config: GatewayServerConfig) -> std::io::Result<()> {
    let request = read_request(&mut stream)?;
    let response = handle_gateway_request_text(&request, config);
    stream.write_all(response.as_bytes())?;
    stream.flush()
}

pub fn handle_gateway_request_text(request: &str, config: GatewayServerConfig) -> String {
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let (path, query) = split_request_target(target);
    let content_type = header_value(request, "content-type").unwrap_or("application/json");
    let request_id = header_value(request, "x-request-id")
        .map(str::to_string)
        .unwrap_or_else(next_request_id);
    let idempotency_key = header_value(request, "idempotency-key").map(str::to_string);
    log_request("tracedb-gateway", &request_id, method, path);
    let body = request
        .split("\r\n\r\n")
        .nth(1)
        .or_else(|| request.split("\n\n").nth(1))
        .unwrap_or_default()
        .as_bytes()
        .to_vec();
    let bearer_token = header_value(request, "authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_string);

    match (method, path) {
        ("GET", "/health") | ("GET", "/v1/health") => ok(json!({
            "ok": true,
            "service": "tracedb-gateway",
            "engine_url": config.engine_url,
            "catalog_databases": config.catalog.databases().count(),
            "metered_requests": config.meter.lock().unwrap().total(MeterKind::Request),
        })),
        ("GET", "/ready") | ("GET", "/v1/ready") => {
            match proxy_engine_request(
                &config.engine_url,
                "GET",
                "/internal/health",
                &[],
                "application/json",
            ) {
                Ok(response) if (200..300).contains(&response.status_code) => ok(json!({
                    "ok": true,
                    "ready": true,
                    "service": "tracedb-gateway",
                    "engine_url": config.engine_url,
                    "engine_health_checked": true,
                    "engine_status_code": response.status_code,
                    "catalog_databases": config.catalog.databases().count(),
                    "metered_requests": config.meter.lock().unwrap().total(MeterKind::Request),
                })),
                Ok(response) => service_unavailable(json!({
                    "ok": false,
                    "ready": false,
                    "service": "tracedb-gateway",
                    "engine_url": config.engine_url,
                    "engine_health_checked": true,
                    "engine_status_code": response.status_code,
                })),
                Err(error) => service_unavailable(json!({
                    "ok": false,
                    "ready": false,
                    "service": "tracedb-gateway",
                    "engine_url": config.engine_url,
                    "engine_health_checked": true,
                    "error": error.to_string(),
                })),
            }
        }
        ("GET", "/v1/databases") => ok(json!({
            "gateway": true,
            "databases": config.catalog.databases().collect::<Vec<_>>(),
        })),
        ("GET", "/v1/branches") => ok(json!({
            "gateway": true,
            "branches": config.catalog.branches().collect::<Vec<_>>(),
        })),
        ("GET", "/metrics") | ("GET", "/v1/metrics/public-safe") => ok(json!({
            "gateway": true,
            "service": "tracedb-gateway",
            "requests": config.meter.lock().unwrap().total(MeterKind::Request),
            "rate_limit_enabled": config.rate_limit_enabled,
            "rate_limit_requests": config.rate_limit_requests,
        })),
        ("POST", "/v1/query")
        | ("POST", "/v1/explain")
        | ("POST", "/v1/insert")
        | ("POST", "/v1/schema/apply")
        | ("POST", "/v1/records/put")
        | ("POST", "/v1/records/put-batch")
        | ("POST", "/v1/records/patch")
        | ("POST", "/v1/records/delete")
        | ("POST", "/v1/records/get")
        | ("POST", "/v1/records/scan")
        | ("POST", "/v1/admin/compact")
        | ("POST", "/v1/admin/snapshot")
        | ("POST", "/v1/admin/restore")
        | ("GET", "/v1/admin/jobs") => {
            match authorize_route_and_meter(&config, path, &body, bearer_token, query) {
                Ok(target) => proxy_or_gateway_error(
                    &target.url,
                    method,
                    path,
                    &body,
                    content_type,
                    Some(&request_id),
                    idempotency_key.as_deref(),
                ),
                Err(GatewayRuntimeError::Unauthorized) => unauthorized(),
                Err(GatewayRuntimeError::RateLimited) => too_many_requests(),
                Err(GatewayRuntimeError::BadRequest(message)) => bad_request(message),
            }
        }
        _ => not_found(),
    }
}

fn authorize_route_and_meter(
    config: &GatewayServerConfig,
    path: &str,
    body: &[u8],
    bearer_token: Option<String>,
    query: Option<&str>,
) -> Result<EngineTarget, GatewayRuntimeError> {
    let (database_id, branch_id) = gateway_ids_from_request(body, query)?;
    let gateway = match &config.required_token {
        Some(token) => Gateway::new(config.catalog.clone(), token.clone()),
        None => Gateway::open(config.catalog.clone()),
    }
    .with_engine_url(config.engine_url.clone());
    let mut meter = config.meter.lock().unwrap();
    if config.rate_limit_enabled && meter.total(MeterKind::Request) >= config.rate_limit_requests {
        return Err(GatewayRuntimeError::RateLimited);
    }
    let response = gateway
        .route(
            GatewayRequest {
                database_id,
                branch_id,
                path: path.to_string(),
                bearer_token,
            },
            &mut meter,
        )
        .map_err(|error| {
            if error == "invalid api token" {
                GatewayRuntimeError::Unauthorized
            } else {
                GatewayRuntimeError::BadRequest(error)
            }
        })?;
    Ok(response.engine_target)
}

fn gateway_ids_from_request(
    body: &[u8],
    query: Option<&str>,
) -> Result<(String, String), GatewayRuntimeError> {
    if body.is_empty() {
        let database_id =
            query_value(query, "database_id").unwrap_or_else(|| "db_local".to_string());
        let branch_id =
            query_value(query, "branch_id").unwrap_or_else(|| format!("{database_id}:main"));
        return Ok((database_id, branch_id));
    }
    let value = serde_json::from_slice::<Value>(body)
        .map_err(|error| GatewayRuntimeError::BadRequest(error.to_string()))?;
    let database_id = value
        .get("database_id")
        .and_then(Value::as_str)
        .unwrap_or("db_local")
        .to_string();
    let branch_id = value
        .get("branch_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("{database_id}:main"));
    Ok((database_id, branch_id))
}

fn split_request_target(target: &str) -> (&str, Option<&str>) {
    target
        .split_once('?')
        .map(|(path, query)| (path, Some(query)))
        .unwrap_or((target, None))
}

fn query_value(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| value.to_string())
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GatewayRuntimeError {
    Unauthorized,
    RateLimited,
    BadRequest(String),
}

fn load_gateway_catalog(engine_url: &str) -> Catalog {
    let path = std::env::var_os("TRACEDB_CATALOG_PATH")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("TRACEDB_CATALOG_URL")
                .ok()
                .and_then(|url| url.strip_prefix("file://").map(PathBuf::from))
        });
    if let Some(path) = path {
        if let Ok(catalog) = Catalog::load(path) {
            return catalog;
        }
    }
    let mut catalog = Catalog::default();
    let _ = catalog.create_database("local-org", "local-project", "local", "local");
    let _ = catalog.create_branch("db_local", "main", None);
    let _ = engine_url;
    catalog
}

pub fn proxy_engine_request(
    engine_url: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
) -> std::io::Result<EngineHttpResponse> {
    proxy_engine_request_with_id(engine_url, method, path, body, content_type, None, None)
}

fn proxy_engine_request_with_id(
    engine_url: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    request_id: Option<&str>,
    idempotency_key: Option<&str>,
) -> std::io::Result<EngineHttpResponse> {
    let target = HttpTarget::parse(engine_url)?;
    let route = target.join(path);
    let mut stream = TcpStream::connect(&target.address)?;
    let request_id_header = request_id
        .map(|request_id| format!("x-request-id: {request_id}\r\n"))
        .unwrap_or_default();
    let idempotency_key_header = idempotency_key
        .map(|idempotency_key| format!("idempotency-key: {idempotency_key}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "{method} {route} HTTP/1.1\r\nhost: {}\r\ncontent-type: {content_type}\r\n{request_id_header}{idempotency_key_header}content-length: {}\r\nconnection: close\r\n\r\n{}",
        target.host,
        body.len(),
        String::from_utf8_lossy(body)
    );
    stream.write_all(request.as_bytes())?;
    stream.flush()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    parse_engine_response(&response)
}

fn proxy_or_gateway_error(
    engine_url: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    request_id: Option<&str>,
    idempotency_key: Option<&str>,
) -> String {
    match proxy_engine_request_with_id(
        engine_url,
        method,
        path,
        body,
        content_type,
        request_id,
        idempotency_key,
    ) {
        Ok(response) => response.to_http_response(),
        Err(error) => bad_gateway(format!("engine proxy failed: {error}")),
    }
}

fn next_request_id() -> String {
    format!(
        "gateway-{}-{}",
        std::process::id(),
        NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn log_request(service: &str, request_id: &str, method: &str, path: &str) {
    println!(
        "{}",
        json!({
            "service": service,
            "request_id": request_id,
            "method": method,
            "path": path,
        })
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HttpTarget {
    host: String,
    address: String,
    base_path: String,
}

impl HttpTarget {
    fn parse(url: &str) -> std::io::Result<Self> {
        let without_scheme = url.strip_prefix("http://").ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "gateway engine proxy only supports http:// URLs",
            )
        })?;
        let (host, base_path) = without_scheme
            .split_once('/')
            .map(|(host, path)| (host, format!("/{path}")))
            .unwrap_or((without_scheme, "/".to_string()));
        if host.trim().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "engine URL host cannot be empty",
            ));
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

fn parse_engine_response(response: &[u8]) -> std::io::Result<EngineHttpResponse> {
    let delimiter = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| (pos, 4))
        .or_else(|| {
            response
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|pos| (pos, 2))
        })
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "engine response did not include headers",
            )
        })?;
    let headers = String::from_utf8_lossy(&response[..delimiter.0]);
    let status_line = headers.lines().next().unwrap_or_default();
    let mut status_parts = status_line.split_whitespace();
    let _http = status_parts.next();
    let status_code = status_parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "engine response status code is invalid",
            )
        })?;
    let reason = status_parts.collect::<Vec<_>>().join(" ");
    let content_type = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-type")
                .then(|| value.trim().to_string())
        })
        .unwrap_or_else(|| "application/octet-stream".to_string());
    Ok(EngineHttpResponse {
        status_code,
        reason: if reason.is_empty() {
            "OK".to_string()
        } else {
            reason
        },
        content_type,
        body: response[(delimiter.0 + delimiter.1)..].to_vec(),
    })
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let (header_end, delimiter_len);
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before headers",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some((end, len)) = find_header_end(&buffer) {
            header_end = end;
            delimiter_len = len;
            break;
        }
        if buffer.len() > 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request headers exceed 1MiB",
            ));
        }
    }
    let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let body_start = header_end + delimiter_len;
    while buffer.len() < body_start + content_length {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before full body",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if content_length > 16 * 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request body exceeds 16MiB",
            ));
        }
    }
    Ok(String::from_utf8_lossy(&buffer[..body_start + content_length]).to_string())
}

fn find_header_end(buffer: &[u8]) -> Option<(usize, usize)> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| (pos, 4))
        .or_else(|| {
            buffer
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|pos| (pos, 2))
        })
}

fn header_value<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

fn ok(value: serde_json::Value) -> String {
    let body = value.to_string();
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn service_unavailable(value: serde_json::Value) -> String {
    let body = value.to_string();
    format!(
        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn unauthorized() -> String {
    let body = json!({ "error": "invalid api token" }).to_string();
    format!(
        "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn not_found() -> String {
    let body = json!({ "error": "not found" }).to_string();
    format!(
        "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn bad_gateway(message: String) -> String {
    let body = json!({ "error": message }).to_string();
    format!(
        "HTTP/1.1 502 Bad Gateway\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn bad_request(message: String) -> String {
    let body = json!({ "error": message }).to_string();
    format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn too_many_requests() -> String {
    let body = json!({ "error": "rate limit exceeded" }).to_string();
    format!(
        "HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn bind_addr_from_env() -> String {
    std::env::var("TRACEDB_BIND").unwrap_or_else(|_| {
        std::env::var("PORT")
            .map(|port| format!("0.0.0.0:{port}"))
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
    })
}
