#![forbid(unsafe_code)]

use axum::body::{to_bytes, Body, Bytes};
use axum::error_handling::HandleErrorLayer;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, Uri};
use axum::routing::any;
use axum::{BoxError, Router};
use reqwest::header;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::convert::Infallible;
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
    fn to_http_response_text(&self) -> String {
        format!(
            "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\n\r\n{}",
            self.status_code,
            self.reason,
            self.content_type,
            self.body.len(),
            String::from_utf8_lossy(&self.body)
        )
    }

    fn to_axum_response(&self) -> Response<Body> {
        let status =
            StatusCode::from_u16(self.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        Response::builder()
            .status(status)
            .header("content-type", self.content_type.as_str())
            .header("connection", "close")
            .body(Body::from(self.body.clone()))
            .expect("gateway response builder")
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
    pub http_client: reqwest::Client,
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
            http_client: reqwest::Client::new(),
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
    Ok(
        handle_gateway_request_parts(config, method, uri, headers, body)
            .await
            .to_axum_response(),
    )
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
    let inbound_actor_headers = gateway_actor_header_overrides(request);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("gateway compatibility runtime");
    runtime
        .block_on(handle_gateway_request_core(
            config,
            method,
            path,
            query,
            GatewayInboundHeaders {
                content_type: content_type.to_string(),
                request_id,
                idempotency_key,
                bearer_token,
                actor_headers: inbound_actor_headers,
            },
            Bytes::from(body),
        ))
        .to_http_response_text()
}

async fn handle_gateway_request_parts(
    config: GatewayServerConfig,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> EngineHttpResponse {
    let method = method.as_str().to_string();
    let path = uri.path().to_string();
    let query = uri.query().map(str::to_string);
    let request_id = header_map_value(&headers, "x-request-id").unwrap_or_else(next_request_id);
    log_request("tracedb-gateway", &request_id, &method, &path);
    let inbound = GatewayInboundHeaders {
        content_type: header_map_value(&headers, "content-type")
            .unwrap_or_else(|| "application/json".to_string()),
        request_id,
        idempotency_key: header_map_value(&headers, "idempotency-key"),
        bearer_token: header_map_value(&headers, "authorization")
            .and_then(|value| value.strip_prefix("Bearer ").map(str::to_string)),
        actor_headers: gateway_actor_header_overrides_from_header_map(&headers),
    };
    handle_gateway_request_core(config, &method, &path, query.as_deref(), inbound, body).await
}

#[derive(Clone, Debug)]
struct GatewayInboundHeaders {
    content_type: String,
    request_id: String,
    idempotency_key: Option<String>,
    bearer_token: Option<String>,
    actor_headers: GatewayActorHeaderOverrides,
}

async fn handle_gateway_request_core(
    config: GatewayServerConfig,
    method: &str,
    path: &str,
    query: Option<&str>,
    inbound: GatewayInboundHeaders,
    body: Bytes,
) -> EngineHttpResponse {
    if !is_auth_exempt_gateway_route(method, path) {
        if let Err(error) = require_auth(&config, inbound.bearer_token.as_deref()) {
            return error;
        }
    }

    match (method, path) {
        ("GET", "/health") | ("GET", "/healthz") | ("GET", "/v1/health") => gateway_ok(json!({
            "ok": true,
            "service": "tracedb-gateway",
            "engine_url": config.engine_url,
            "catalog_databases": config.catalog.databases().count(),
            "metered_requests": config.meter.lock().unwrap().total(MeterKind::Request),
        })),
        ("GET", "/ready") | ("GET", "/v1/ready") => {
            match proxy_engine_request(
                &config.http_client,
                &config.engine_url,
                "GET",
                "/internal/health",
                &[],
                "application/json",
                config.engine_internal_token.as_deref(),
                &[],
            )
            .await
            {
                Ok(response) if (200..300).contains(&response.status_code) => gateway_ok(json!({
                    "ok": true,
                    "ready": true,
                    "service": "tracedb-gateway",
                    "engine_url": config.engine_url,
                    "engine_health_checked": true,
                    "engine_status_code": response.status_code,
                    "catalog_databases": config.catalog.databases().count(),
                    "metered_requests": config.meter.lock().unwrap().total(MeterKind::Request),
                })),
                Ok(response) => gateway_json_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    json!({
                        "ok": false,
                        "ready": false,
                        "service": "tracedb-gateway",
                        "engine_url": config.engine_url,
                        "engine_health_checked": true,
                        "engine_status_code": response.status_code,
                    }),
                ),
                Err(error) => gateway_json_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    json!({
                        "ok": false,
                        "ready": false,
                        "service": "tracedb-gateway",
                        "engine_url": config.engine_url,
                        "engine_health_checked": true,
                        "error": error.to_string(),
                    }),
                ),
            }
        }
        ("GET", "/v1/databases") => gateway_ok(json!({
            "gateway": true,
            "databases": config.catalog.databases().collect::<Vec<_>>(),
        })),
        ("GET", "/v1/branches") => gateway_ok(json!({
            "gateway": true,
            "branches": config.catalog.branches().collect::<Vec<_>>(),
        })),
        ("GET", "/metrics") | ("GET", "/v1/metrics/public-safe") => gateway_ok(json!({
            "gateway": true,
            "service": "tracedb-gateway",
            "requests": config.meter.lock().unwrap().total(MeterKind::Request),
            "rate_limit_enabled": config.rate_limit_enabled,
            "rate_limit_requests": config.rate_limit_requests,
        })),
        _ if is_proxied_gateway_route(method, path) => {
            match authorize_route_and_meter(
                &config,
                path,
                &body,
                inbound.bearer_token,
                query,
                &inbound.actor_headers,
            ) {
                Ok(authorized) => {
                    let mut actor_headers = authorized.actor_headers;
                    actor_headers.push((
                        "x-tracedb-request-id".to_string(),
                        inbound.request_id.clone(),
                    ));
                    let proxy_path = query
                        .map(|query| format!("{path}?{query}"))
                        .unwrap_or_else(|| path.to_string());
                    proxy_or_gateway_error(
                        &config.http_client,
                        &authorized.target.url,
                        method,
                        &proxy_path,
                        &body,
                        &inbound.content_type,
                        Some(&inbound.request_id),
                        inbound.idempotency_key.as_deref(),
                        config.engine_internal_token.as_deref(),
                        &actor_headers,
                    )
                    .await
                }
                Err(GatewayRuntimeError::Unauthorized) => gateway_unauthorized(),
                Err(GatewayRuntimeError::RateLimited) => gateway_too_many_requests(),
                Err(GatewayRuntimeError::BadRequest(message)) => gateway_bad_request(message),
            }
        }
        _ => gateway_not_found(),
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

fn is_auth_exempt_gateway_route(method: &str, path: &str) -> bool {
    matches!(
        (method, path),
        ("GET", "/health")
            | ("GET", "/healthz")
            | ("GET", "/v1/health")
            | ("GET", "/ready")
            | ("GET", "/v1/ready")
    )
}

fn require_auth(
    config: &GatewayServerConfig,
    bearer_token: Option<&str>,
) -> Result<(), EngineHttpResponse> {
    if config
        .required_token
        .as_ref()
        .is_some_and(|required| bearer_token != Some(required.as_str()))
    {
        Err(gateway_unauthorized())
    } else {
        Ok(())
    }
}

fn authorize_route_and_meter(
    config: &GatewayServerConfig,
    path: &str,
    body: &[u8],
    bearer_token: Option<String>,
    query: Option<&str>,
    actor_headers: &GatewayActorHeaderOverrides,
) -> Result<AuthorizedGatewayRoute, GatewayRuntimeError> {
    let mut ids = gateway_ids_from_request(body, query, actor_headers)?;
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
    token_identity: Option<String>,
    policy_epoch: Option<String>,
    scopes: Option<String>,
}

impl GatewayRequestIds {
    fn actor_headers(&self) -> Vec<(String, String)> {
        let mut headers = vec![
            (
                "x-tracedb-database-id".to_string(),
                self.database_id.clone(),
            ),
            ("x-tracedb-branch-id".to_string(), self.branch_id.clone()),
            ("x-tracedb-tenant-id".to_string(), self.tenant_id.clone()),
            (
                "x-tracedb-token-identity".to_string(),
                self.token_identity.clone().unwrap_or_else(|| {
                    if self.bearer_token.is_some() {
                        "bearer".to_string()
                    } else {
                        "anonymous".to_string()
                    }
                }),
            ),
        ];
        if let Some(policy_epoch) = &self.policy_epoch {
            headers.push(("x-tracedb-policy-epoch".to_string(), policy_epoch.clone()));
        }
        if let Some(scopes) = &self.scopes {
            headers.push(("x-tracedb-scopes".to_string(), scopes.clone()));
        }
        headers
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct GatewayActorHeaderOverrides {
    database_id: Option<String>,
    branch_id: Option<String>,
    tenant_id: Option<String>,
    token_identity: Option<String>,
    policy_epoch: Option<String>,
    scopes: Option<String>,
}

fn gateway_ids_from_request(
    body: &[u8],
    query: Option<&str>,
    actor_headers: &GatewayActorHeaderOverrides,
) -> Result<GatewayRequestIds, GatewayRuntimeError> {
    if body.is_empty() {
        let database_id = actor_headers
            .database_id
            .clone()
            .or_else(|| query_value(query, "database_id"))
            .unwrap_or_else(|| "db_local".to_string());
        let branch_id = actor_headers
            .branch_id
            .clone()
            .or_else(|| query_value(query, "branch_id"))
            .unwrap_or_else(|| format!("{database_id}:main"));
        let tenant_id = actor_headers
            .tenant_id
            .clone()
            .or_else(|| query_value(query, "tenant_id"))
            .unwrap_or_else(|| "local".to_string());
        return Ok(GatewayRequestIds {
            database_id,
            branch_id,
            tenant_id,
            bearer_token: None,
            token_identity: actor_headers.token_identity.clone(),
            policy_epoch: actor_headers.policy_epoch.clone(),
            scopes: actor_headers.scopes.clone(),
        });
    }
    if let (Some(database_id), Some(branch_id), Some(tenant_id)) = (
        actor_headers.database_id.clone(),
        actor_headers.branch_id.clone(),
        actor_headers.tenant_id.clone(),
    ) {
        return Ok(GatewayRequestIds {
            database_id,
            branch_id,
            tenant_id,
            bearer_token: None,
            token_identity: actor_headers.token_identity.clone(),
            policy_epoch: actor_headers.policy_epoch.clone(),
            scopes: actor_headers.scopes.clone(),
        });
    }
    let value = serde_json::from_slice::<Value>(body)
        .map_err(|error| GatewayRuntimeError::BadRequest(error.to_string()))?;
    let body_database_id = value.get("database_id").and_then(Value::as_str);
    let database_id = actor_headers
        .database_id
        .clone()
        .or_else(|| body_database_id.map(str::to_string))
        .unwrap_or_else(|| "db_local".to_string());
    let branch_id = actor_headers
        .branch_id
        .clone()
        .or_else(|| {
            value
                .get("branch_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("{database_id}:main"));
    let body_tenant_id = value
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
        .map(str::to_string);
    let tenant_id = actor_headers
        .tenant_id
        .clone()
        .or(body_tenant_id)
        .unwrap_or_else(|| "local".to_string());
    Ok(GatewayRequestIds {
        database_id,
        branch_id,
        tenant_id,
        bearer_token: None,
        token_identity: actor_headers.token_identity.clone(),
        policy_epoch: actor_headers.policy_epoch.clone(),
        scopes: actor_headers.scopes.clone(),
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

fn gateway_actor_header_overrides(request: &str) -> GatewayActorHeaderOverrides {
    GatewayActorHeaderOverrides {
        database_id: header_value(request, "x-tracedb-database-id").map(str::to_string),
        branch_id: header_value(request, "x-tracedb-branch-id").map(str::to_string),
        tenant_id: header_value(request, "x-tracedb-tenant-id").map(str::to_string),
        token_identity: header_value(request, "x-tracedb-token-identity").map(str::to_string),
        policy_epoch: header_value(request, "x-tracedb-policy-epoch").map(str::to_string),
        scopes: header_value(request, "x-tracedb-scopes").map(str::to_string),
    }
}

fn gateway_actor_header_overrides_from_header_map(
    headers: &HeaderMap,
) -> GatewayActorHeaderOverrides {
    GatewayActorHeaderOverrides {
        database_id: header_map_value(headers, "x-tracedb-database-id"),
        branch_id: header_map_value(headers, "x-tracedb-branch-id"),
        tenant_id: header_map_value(headers, "x-tracedb-tenant-id"),
        token_identity: header_map_value(headers, "x-tracedb-token-identity"),
        policy_epoch: header_map_value(headers, "x-tracedb-policy-epoch"),
        scopes: header_map_value(headers, "x-tracedb-scopes"),
    }
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

pub async fn proxy_engine_request(
    client: &reqwest::Client,
    engine_url: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    engine_internal_token: Option<&str>,
    actor_headers: &[(String, String)],
) -> std::io::Result<EngineHttpResponse> {
    proxy_engine_request_with_id(
        client,
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
    .await
}

async fn proxy_engine_request_with_id(
    client: &reqwest::Client,
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
    let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid proxy method: {error}"),
        )
    })?;
    let url = join_engine_url(engine_url, path)?;
    let mut request = client
        .request(method, url)
        .header(header::CONTENT_TYPE, content_type)
        .body(body.to_vec());
    if let Some(request_id) = request_id {
        request = request.header("x-request-id", request_id);
    }
    if let Some(idempotency_key) = idempotency_key {
        request = request.header("idempotency-key", idempotency_key);
    }
    if let Some(token) = engine_internal_token {
        request = request.header("x-tracedb-engine-token", token);
    }
    for (name, value) in actor_headers {
        let header_name = header::HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid proxy header name {name}: {error}"),
            )
        })?;
        request = request.header(header_name, value);
    }
    let response = request
        .send()
        .await
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error.to_string()))?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let body = response
        .bytes()
        .await
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error.to_string()))?
        .to_vec();
    Ok(EngineHttpResponse {
        status_code: status.as_u16(),
        reason: status.canonical_reason().unwrap_or("OK").to_string(),
        content_type,
        body,
    })
}

async fn proxy_or_gateway_error(
    client: &reqwest::Client,
    engine_url: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    request_id: Option<&str>,
    idempotency_key: Option<&str>,
    engine_internal_token: Option<&str>,
    actor_headers: &[(String, String)],
) -> EngineHttpResponse {
    match proxy_engine_request_with_id(
        client,
        engine_url,
        method,
        path,
        body,
        content_type,
        request_id,
        idempotency_key,
        engine_internal_token,
        actor_headers,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => gateway_bad_gateway(format!("engine proxy failed: {error}")),
    }
}

fn join_engine_url(engine_url: &str, path: &str) -> std::io::Result<String> {
    let base = engine_url.trim_end_matches('/');
    if base.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "engine URL cannot be empty",
        ));
    }
    Ok(format!("{}/{}", base, path.trim_start_matches('/')))
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

fn header_map_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn gateway_ok(value: serde_json::Value) -> EngineHttpResponse {
    gateway_json_response(StatusCode::OK, value)
}

fn gateway_unauthorized() -> EngineHttpResponse {
    gateway_json_response(
        StatusCode::UNAUTHORIZED,
        json!({ "error": "invalid api token", "code": "unauthorized" }),
    )
}

fn gateway_not_found() -> EngineHttpResponse {
    gateway_json_response(
        StatusCode::NOT_FOUND,
        json!({ "error": "not found", "code": "not_found" }),
    )
}

fn gateway_bad_gateway(message: String) -> EngineHttpResponse {
    gateway_json_response(
        StatusCode::BAD_GATEWAY,
        json!({ "error": message, "code": "bad_gateway" }),
    )
}

fn gateway_bad_request(message: String) -> EngineHttpResponse {
    gateway_json_response(
        StatusCode::BAD_REQUEST,
        json!({ "error": message, "code": "bad_request" }),
    )
}

fn gateway_too_many_requests() -> EngineHttpResponse {
    gateway_json_response(
        StatusCode::TOO_MANY_REQUESTS,
        json!({ "error": "rate limit exceeded", "code": "rate_limited" }),
    )
}

fn gateway_json_response(status: StatusCode, value: serde_json::Value) -> EngineHttpResponse {
    EngineHttpResponse {
        status_code: status.as_u16(),
        reason: status.canonical_reason().unwrap_or("OK").to_string(),
        content_type: "application/json".to_string(),
        body: value.to_string().into_bytes(),
    }
}

fn bad_request(message: String) -> String {
    let body = json!({ "error": message, "code": "bad_request" }).to_string();
    format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
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
    use std::io::{Read, Write};

    #[test]
    fn gateway_proxies_traceql_and_graphql_adapter_routes() {
        assert!(is_proxied_gateway_route("POST", "/v1/traceql"));
        assert!(is_proxied_gateway_route("POST", "/v1/graphql"));
        assert!(is_proxied_gateway_route("GET", "/v1/graphql/schema"));
    }

    #[test]
    fn gateway_requires_auth_for_metadata_routes() {
        let config = GatewayServerConfig {
            bind: "127.0.0.1:0".to_string(),
            engine_url: "http://127.0.0.1:1".to_string(),
            http_client: reqwest::Client::new(),
            engine_internal_token: Some("engine-secret".to_string()),
            required_token: Some("public-secret".to_string()),
            catalog: Catalog::default(),
            meter: Arc::new(Mutex::new(UsageMeter::default())),
            rate_limit_enabled: false,
            rate_limit_requests: 10,
            request_timeout: Duration::from_secs(30),
            max_concurrent_requests: 1024,
        };

        let databases =
            handle_gateway_request_text("GET /v1/databases HTTP/1.1\r\n\r\n", config.clone());
        let branches =
            handle_gateway_request_text("GET /v1/branches HTTP/1.1\r\n\r\n", config.clone());
        let metrics = handle_gateway_request_text("GET /metrics HTTP/1.1\r\n\r\n", config.clone());
        let health = handle_gateway_request_text("GET /health HTTP/1.1\r\n\r\n", config.clone());
        let authorized = handle_gateway_request_text(
            "GET /v1/databases HTTP/1.1\r\nauthorization: Bearer public-secret\r\n\r\n",
            config,
        );

        assert!(databases.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(branches.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(metrics.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(health.starts_with("HTTP/1.1 200 OK"));
        assert!(authorized.starts_with("HTTP/1.1 200 OK"));
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
            http_client: reqwest::Client::new(),
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

    #[test]
    fn gateway_preserves_inbound_actor_context_for_command_surfaces() {
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
            http_client: reqwest::Client::new(),
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
            "query": "FROM docs\nTENANT tenant-a\nLIMIT 1"
        })
        .to_string();
        let request = format!(
            "POST /v1/traceql HTTP/1.1\r\ncontent-type: application/json\r\nauthorization: Bearer public-secret\r\nx-request-id: request-456\r\nx-tracedb-database-id: {}\r\nx-tracedb-branch-id: {}\r\nx-tracedb-tenant-id: tenant-a\r\nx-tracedb-token-identity: smoke-token\r\nx-tracedb-policy-epoch: 7\r\nx-tracedb-scopes: records:read,records:write\r\ncontent-length: {}\r\n\r\n{}",
            database.database_id,
            branch.branch_id,
            body.len(),
            body
        );

        let response = handle_gateway_request_text(&request, config);

        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "gateway response should be OK: {response}"
        );
        assert!(response.contains("\"x-tracedb-database-id\":\"db_"));
        assert!(response.contains("\"x-tracedb-branch-id\":\""));
        assert!(response.contains("\"x-tracedb-tenant-id\":\"tenant-a\""));
        assert!(response.contains("\"x-tracedb-token-identity\":\"smoke-token\""));
        assert!(response.contains("\"x-tracedb-policy-epoch\":\"7\""));
        assert!(response.contains("\"x-tracedb-scopes\":\"records:read,records:write\""));
        assert!(response.contains("\"x-tracedb-request-id\":\"request-456\""));
    }

    #[tokio::test]
    async fn axum_gateway_proxies_binary_body_without_utf8_loss() {
        let engine = spawn_binary_echo_engine();
        let mut catalog = Catalog::default();
        let database = catalog
            .create_database("org-a", "project-a", "memory", "us-west")
            .expect("database");
        let branch = catalog
            .create_branch(&database.database_id, "main", None)
            .expect("branch");
        let config = GatewayServerConfig {
            bind: "127.0.0.1:0".to_string(),
            engine_url: engine,
            http_client: reqwest::Client::new(),
            engine_internal_token: Some("engine-secret".to_string()),
            required_token: Some("public-secret".to_string()),
            catalog,
            meter: Arc::new(Mutex::new(UsageMeter::default())),
            rate_limit_enabled: false,
            rate_limit_requests: 10,
            request_timeout: Duration::from_secs(30),
            max_concurrent_requests: 1024,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer public-secret"),
        );
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/octet-stream"),
        );
        headers.insert(
            "x-tracedb-database-id",
            HeaderValue::from_str(&database.database_id).expect("database header"),
        );
        headers.insert(
            "x-tracedb-branch-id",
            HeaderValue::from_str(&branch.branch_id).expect("branch header"),
        );
        headers.insert("x-tracedb-tenant-id", HeaderValue::from_static("tenant-a"));
        let payload = vec![0, 159, 146, 150, 255, b't', b'd', b'b'];

        let response = handle_axum_gateway_request(
            State(config),
            Method::POST,
            "/v1/query".parse().expect("uri"),
            headers,
            Body::from(payload.clone()),
        )
        .await
        .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096)
            .await
            .expect("body bytes");
        assert_eq!(body.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn axum_entrypoint_rejects_oversized_body() {
        let config = GatewayServerConfig {
            bind: "127.0.0.1:0".to_string(),
            engine_url: "http://127.0.0.1:1".to_string(),
            http_client: reqwest::Client::new(),
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

    fn spawn_binary_echo_engine() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind engine");
        let addr = listener.local_addr().expect("engine addr");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept engine request");
            let mut buffer = [0u8; 8192];
            let read = stream.read(&mut buffer).expect("read engine request");
            let request = &buffer[..read];
            let header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
                .expect("header boundary");
            let body = &request[header_end..];
            let headers = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: {}\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).expect("write headers");
            stream.write_all(body).expect("write body");
        });
        format!("http://{addr}")
    }
}
