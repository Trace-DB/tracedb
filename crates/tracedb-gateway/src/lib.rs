#![forbid(unsafe_code)]

use axum::body::{to_bytes, Body, Bytes};
use axum::error_handling::HandleErrorLayer;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, Uri};
use axum::routing::any;
use axum::{BoxError, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::convert::Infallible;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tower::limit::ConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
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
    pub engine_internal_token: Option<String>,
    pub required_token: Option<String>,
    pub catalog: Catalog,
    pub meter: Arc<Mutex<UsageMeter>>,
    pub rate_limit_enabled: bool,
    pub rate_limit_requests: u64,
    pub request_timeout: Duration,
    pub max_concurrent_requests: usize,
}

impl GatewayServerConfig {
    pub fn from_env() -> Self {
        let engine_url = std::env::var("TRACEDB_ENGINE_URL")
            .unwrap_or_else(|_| "http://tracedb-engine.railway.internal:8080".to_string());
        Self {
            bind: bind_addr_from_env(),
            engine_url: engine_url.clone(),
            engine_internal_token: engine_internal_token_from_env(),
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
            request_timeout: env_duration_ms("TRACEDB_REQUEST_TIMEOUT_MS", 30_000),
            max_concurrent_requests: env_usize("TRACEDB_MAX_CONCURRENT_REQUESTS", 1024).max(1),
        }
    }
}

pub fn serve(config: GatewayServerConfig) -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(serve_async(config))
}

pub async fn serve_async(config: GatewayServerConfig) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    serve_tokio_listener(listener, config).await
}

pub async fn serve_tokio_listener(
    listener: tokio::net::TcpListener,
    config: GatewayServerConfig,
) -> std::io::Result<()> {
    let app = Router::new()
        .fallback(any(handle_axum_gateway_request))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(runtime_handle_error))
                .layer(LoadShedLayer::new())
                .layer(ConcurrencyLimitLayer::new(config.max_concurrent_requests))
                .layer(TimeoutLayer::new(config.request_timeout)),
        )
        .with_state(config);
    axum::serve(listener, app).await
}

async fn runtime_handle_error(error: BoxError) -> Response<Body> {
    if error.is::<tower::timeout::error::Elapsed>() {
        return response_from_http_text(runtime_error_response(RuntimeRouteError::Timeout));
    }
    if error.is::<tower::load_shed::error::Overloaded>() {
        return response_from_http_text(runtime_error_response(RuntimeRouteError::Overloaded));
    }
    response_from_http_text(runtime_error_response(RuntimeRouteError::Internal(
        error.to_string(),
    )))
}

async fn handle_axum_gateway_request(
    State(config): State<GatewayServerConfig>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response<Body>, Infallible> {
    let body = match to_bytes(body, 16 * 1024 * 1024).await {
        Ok(body) => body,
        Err(error) => {
            return Ok(response_from_http_text(bad_request(format!(
                "failed to read request body: {error}"
            ))));
        }
    };
    let request = request_text_from_parts(&method, &uri, &headers, &body);
    Ok(response_from_http_text(handle_gateway_request_text(
        &request, config,
    )))
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
                config.engine_internal_token.as_deref(),
                &[],
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
        _ if is_proxied_gateway_route(method, path) => {
            match authorize_route_and_meter(&config, path, &body, bearer_token, query) {
                Ok(authorized) => {
                    let mut actor_headers = authorized.actor_headers;
                    actor_headers.push(("x-tracedb-request-id".to_string(), request_id.clone()));
                    proxy_or_gateway_error(
                        &authorized.target.url,
                        method,
                        path,
                        &body,
                        content_type,
                        Some(&request_id),
                        idempotency_key.as_deref(),
                        config.engine_internal_token.as_deref(),
                        &actor_headers,
                    )
                }
                Err(GatewayRuntimeError::Unauthorized) => unauthorized(),
                Err(GatewayRuntimeError::RateLimited) => too_many_requests(),
                Err(GatewayRuntimeError::BadRequest(message)) => bad_request(message),
            }
        }
        _ => not_found(),
    }
}

fn is_proxied_gateway_route(method: &str, path: &str) -> bool {
    matches!(
        (method, path),
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
            | ("POST", "/v1/traceql")
            | ("POST", "/v1/graphql")
            | ("GET", "/v1/graphql/schema")
            | ("POST", "/v1/admin/compact")
            | ("POST", "/v1/admin/snapshot")
            | ("POST", "/v1/admin/restore")
            | ("GET", "/v1/admin/jobs")
    )
}

fn authorize_route_and_meter(
    config: &GatewayServerConfig,
    path: &str,
    body: &[u8],
    bearer_token: Option<String>,
    query: Option<&str>,
) -> Result<AuthorizedGatewayRoute, GatewayRuntimeError> {
    let mut ids = gateway_ids_from_request(body, query)?;
    ids.bearer_token = bearer_token;
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
                database_id: ids.database_id.clone(),
                branch_id: ids.branch_id.clone(),
                path: path.to_string(),
                bearer_token: ids.bearer_token.clone(),
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
    Ok(AuthorizedGatewayRoute {
        target: response.engine_target,
        actor_headers: ids.actor_headers(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorizedGatewayRoute {
    target: EngineTarget,
    actor_headers: Vec<(String, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GatewayRequestIds {
    database_id: String,
    branch_id: String,
    tenant_id: String,
    bearer_token: Option<String>,
}

impl GatewayRequestIds {
    fn actor_headers(&self) -> Vec<(String, String)> {
        vec![
            (
                "x-tracedb-database-id".to_string(),
                self.database_id.clone(),
            ),
            ("x-tracedb-branch-id".to_string(), self.branch_id.clone()),
            ("x-tracedb-tenant-id".to_string(), self.tenant_id.clone()),
            (
                "x-tracedb-token-identity".to_string(),
                if self.bearer_token.is_some() {
                    "bearer".to_string()
                } else {
                    "anonymous".to_string()
                },
            ),
        ]
    }
}

fn gateway_ids_from_request(
    body: &[u8],
    query: Option<&str>,
) -> Result<GatewayRequestIds, GatewayRuntimeError> {
    if body.is_empty() {
        let database_id =
            query_value(query, "database_id").unwrap_or_else(|| "db_local".to_string());
        let branch_id =
            query_value(query, "branch_id").unwrap_or_else(|| format!("{database_id}:main"));
        let tenant_id = query_value(query, "tenant_id").unwrap_or_else(|| "local".to_string());
        return Ok(GatewayRequestIds {
            database_id,
            branch_id,
            tenant_id,
            bearer_token: None,
        });
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
    let tenant_id = value
        .get("tenant_id")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("records")
                .and_then(Value::as_array)
                .and_then(|records| records.first())
                .and_then(|record| record.get("tenant_id"))
                .and_then(Value::as_str)
        })
        .unwrap_or("local")
        .to_string();
    Ok(GatewayRequestIds {
        database_id,
        branch_id,
        tenant_id,
        bearer_token: None,
    })
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
    engine_internal_token: Option<&str>,
    actor_headers: &[(String, String)],
) -> std::io::Result<EngineHttpResponse> {
    proxy_engine_request_with_id(
        engine_url,
        method,
        path,
        body,
        content_type,
        None,
        None,
        engine_internal_token,
        actor_headers,
    )
}

fn proxy_engine_request_with_id(
    engine_url: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    request_id: Option<&str>,
    idempotency_key: Option<&str>,
    engine_internal_token: Option<&str>,
    actor_headers: &[(String, String)],
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
    let engine_token_header = engine_internal_token
        .map(|token| format!("x-tracedb-engine-token: {token}\r\n"))
        .unwrap_or_default();
    let actor_header_text = actor_headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    let request = format!(
        "{method} {route} HTTP/1.1\r\nhost: {}\r\ncontent-type: {content_type}\r\n{request_id_header}{idempotency_key_header}{engine_token_header}{actor_header_text}content-length: {}\r\nconnection: close\r\n\r\n{}",
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
    engine_internal_token: Option<&str>,
    actor_headers: &[(String, String)],
) -> String {
    match proxy_engine_request_with_id(
        engine_url,
        method,
        path,
        body,
        content_type,
        request_id,
        idempotency_key,
        engine_internal_token,
        actor_headers,
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

#[cfg(test)]
fn request_log_fields(service: &str, request_id: &str, method: &str, path: &str) -> Value {
    json!({
        "service": service,
        "request_id": request_id,
        "method": method,
        "path": path,
    })
}

fn log_request(service: &str, request_id: &str, method: &str, path: &str) {
    tracing::info!(
        service = service,
        request_id = request_id,
        method = method,
        path = path,
        "request"
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RuntimeRouteError {
    Timeout,
    Overloaded,
    Internal(String),
}

fn runtime_error_response(error: RuntimeRouteError) -> String {
    match error {
        RuntimeRouteError::Timeout => http_json_response(
            "504 Gateway Timeout",
            json!({ "error": "request timed out", "code": "timeout" }),
        ),
        RuntimeRouteError::Overloaded => http_json_response(
            "503 Service Unavailable",
            json!({ "error": "request capacity exceeded", "code": "overloaded" }),
        ),
        RuntimeRouteError::Internal(message) => http_json_response(
            "500 Internal Server Error",
            json!({ "error": message, "code": "internal_error" }),
        ),
    }
}

fn http_json_response(status: &str, body: Value) -> String {
    let body = body.to_string();
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
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

fn request_text_from_parts(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> String {
    let mut request = format!("{method} {uri} HTTP/1.1\r\n");
    for (name, value) in headers {
        if name.as_str().eq_ignore_ascii_case("content-length") {
            continue;
        }
        if let Ok(value) = value.to_str() {
            request.push_str(name.as_str());
            request.push_str(": ");
            request.push_str(value);
            request.push_str("\r\n");
        }
    }
    request.push_str("content-length: ");
    request.push_str(&body.len().to_string());
    request.push_str("\r\n\r\n");
    request.push_str(&String::from_utf8_lossy(body));
    request
}

fn response_from_http_text(response: String) -> Response<Body> {
    let Some((head, body)) = response
        .split_once("\r\n\r\n")
        .or_else(|| response.split_once("\n\n"))
    else {
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(response))
            .expect("internal response");
    };
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .and_then(|status| StatusCode::from_u16(status).ok())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = Response::builder().status(status);
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let Ok(name) = HeaderName::from_bytes(name.trim().as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_str(value.trim()) else {
            continue;
        };
        builder = builder.header(name, value);
    }
    builder
        .header("connection", "close")
        .body(Body::from(body.to_string()))
        .expect("response builder")
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
    let body = json!({ "error": "invalid api token", "code": "unauthorized" }).to_string();
    format!(
        "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn not_found() -> String {
    let body = json!({ "error": "not found", "code": "not_found" }).to_string();
    format!(
        "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn bad_gateway(message: String) -> String {
    let body = json!({ "error": message, "code": "bad_gateway" }).to_string();
    format!(
        "HTTP/1.1 502 Bad Gateway\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn bad_request(message: String) -> String {
    let body = json!({ "error": message, "code": "bad_request" }).to_string();
    format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn too_many_requests() -> String {
    let body = json!({ "error": "rate limit exceeded", "code": "rate_limited" }).to_string();
    format!(
        "HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn bind_addr_from_env() -> String {
    std::env::var("TRACEDB_BIND").unwrap_or_else(|_| {
        std::env::var("PORT")
            .map(|port| format!("[::]:{port}"))
            .unwrap_or_else(|_| "[::]:8080".to_string())
    })
}

fn engine_internal_token_from_env() -> Option<String> {
    std::env::var("TRACEDB_ENGINE_INTERNAL_TOKEN")
        .ok()
        .or_else(|| std::env::var("TRACEDB_ENGINE_TOKEN").ok())
        .filter(|token| !token.trim().is_empty())
}

fn env_duration_ms(name: &str, default_ms: u64) -> Duration {
    Duration::from_millis(
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(default_ms),
    )
}

fn env_usize(name: &str, default_value: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_proxies_traceql_and_graphql_adapter_routes() {
        assert!(is_proxied_gateway_route("POST", "/v1/traceql"));
        assert!(is_proxied_gateway_route("POST", "/v1/graphql"));
        assert!(is_proxied_gateway_route("GET", "/v1/graphql/schema"));
    }

    #[test]
    fn gateway_injects_actor_context_and_private_engine_token() {
        let engine = spawn_header_echo_engine();
        let mut catalog = Catalog::default();
        let database = catalog
            .create_database("org-a", "project-a", "memory", "us-west")
            .expect("database");
        let branch = catalog
            .create_branch(&database.database_id, "main", None)
            .expect("branch");
        let meter = Arc::new(Mutex::new(UsageMeter::default()));
        let config = GatewayServerConfig {
            bind: "127.0.0.1:0".to_string(),
            engine_url: engine,
            engine_internal_token: Some("engine-secret".to_string()),
            required_token: Some("public-secret".to_string()),
            catalog,
            meter,
            rate_limit_enabled: false,
            rate_limit_requests: 10,
            request_timeout: Duration::from_secs(30),
            max_concurrent_requests: 1024,
        };
        let body = json!({
            "database_id": database.database_id,
            "branch_id": branch.branch_id,
            "tenant_id": "tenant-a",
            "table": "docs",
            "top_k": 1,
            "freshness": "Strict"
        })
        .to_string();
        let request = format!(
            "POST /v1/query HTTP/1.1\r\ncontent-type: application/json\r\nauthorization: Bearer public-secret\r\nx-request-id: request-123\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );

        let response = handle_gateway_request_text(&request, config);

        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "gateway response should be OK: {response}"
        );
        assert!(response.contains("\"x-tracedb-engine-token\":\"engine-secret\""));
        assert!(response.contains("\"x-tracedb-database-id\":\"db_"));
        assert!(response.contains("\"x-tracedb-branch-id\":\""));
        assert!(response.contains("\"x-tracedb-tenant-id\":\"tenant-a\""));
        assert!(response.contains("\"x-tracedb-request-id\":\"request-123\""));
        assert!(response.contains("\"x-tracedb-token-identity\":\"bearer\""));
    }

    #[tokio::test]
    async fn axum_entrypoint_rejects_oversized_body() {
        let config = GatewayServerConfig {
            bind: "127.0.0.1:0".to_string(),
            engine_url: "http://127.0.0.1:1".to_string(),
            engine_internal_token: Some("engine-secret".to_string()),
            required_token: Some("public-secret".to_string()),
            catalog: Catalog::default(),
            meter: Arc::new(Mutex::new(UsageMeter::default())),
            rate_limit_enabled: false,
            rate_limit_requests: 10,
            request_timeout: Duration::from_secs(30),
            max_concurrent_requests: 1024,
        };
        let response = handle_axum_gateway_request(
            State(config),
            Method::POST,
            "/v1/query".parse().expect("uri"),
            HeaderMap::new(),
            Body::from(vec![b'x'; 16 * 1024 * 1024 + 1]),
        )
        .await
        .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 4096)
            .await
            .expect("body bytes");
        let body = String::from_utf8_lossy(&body);
        assert!(
            body.contains("failed to read request body"),
            "oversized Axum body should be rejected by production path: {body}"
        );
    }

    #[test]
    fn gateway_runtime_errors_are_stable_json() {
        let timeout = runtime_error_response(RuntimeRouteError::Timeout);
        assert!(timeout.starts_with("HTTP/1.1 504 Gateway Timeout"));
        assert!(timeout.contains("\"code\":\"timeout\""));

        let overload = runtime_error_response(RuntimeRouteError::Overloaded);
        assert!(overload.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(overload.contains("\"code\":\"overloaded\""));
    }

    #[test]
    fn gateway_request_log_fields_are_structured_for_tracing() {
        let fields = request_log_fields("tracedb-gateway", "request-1", "POST", "/v1/query");
        assert_eq!(fields["service"], json!("tracedb-gateway"));
        assert_eq!(fields["request_id"], json!("request-1"));
        assert_eq!(fields["method"], json!("POST"));
        assert_eq!(fields["path"], json!("/v1/query"));
    }

    fn spawn_header_echo_engine() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind engine");
        let addr = listener.local_addr().expect("engine addr");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept engine request");
            let mut buffer = [0u8; 8192];
            let read = stream.read(&mut buffer).expect("read engine request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let mut echoed = serde_json::Map::new();
            for line in request.lines().skip(1) {
                let Some((name, value)) = line.split_once(':') else {
                    continue;
                };
                let name = name.trim().to_ascii_lowercase();
                if name.starts_with("x-tracedb-") {
                    echoed.insert(name, json!(value.trim()));
                }
            }
            let body = Value::Object(echoed).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });
        format!("http://{addr}")
    }
}
