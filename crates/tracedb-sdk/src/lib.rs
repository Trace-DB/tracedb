#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;
use tracedb_features::FeatureFreshnessMode;
use tracedb_query::{
    HybridExplain, HybridQuery, HybridQueryRow, RecordDeleteRequest, RecordGetRequest, RecordInput,
    RecordOutput, RecordPatchRequest, RecordPutBatchRequest, RecordScanOutput, RecordScanRequest,
    TableSchema, WritePathTiming,
};

pub type TraceDbClientResult<T> = std::result::Result<T, TraceDbClientError>;

#[derive(Debug)]
pub enum TraceDbClientError {
    InvalidUrl(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Timeout {
        method: String,
        path: String,
        timeout_ms: u64,
    },
    InvalidResponse {
        method: String,
        path: String,
        message: String,
    },
    HttpStatus {
        method: String,
        path: String,
        status: u16,
        body: String,
    },
}

impl Display for TraceDbClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUrl(url) => write!(f, "invalid TraceDB URL {url}"),
            Self::Io(error) => write!(f, "TraceDB HTTP I/O error: {error}"),
            Self::Json(error) => write!(f, "TraceDB JSON error: {error}"),
            Self::Timeout {
                method,
                path,
                timeout_ms,
            } => write!(
                f,
                "TraceDB HTTP request {method} {path} timed out after {timeout_ms} ms"
            ),
            Self::InvalidResponse {
                method,
                path,
                message,
            } => write!(
                f,
                "invalid TraceDB HTTP response for {method} {path}: {message}"
            ),
            Self::HttpStatus {
                method,
                path,
                status,
                body,
            } => {
                write!(
                    f,
                    "TraceDB HTTP request {method} {path} failed with status {status}: {body}"
                )
            }
        }
    }
}

impl Error for TraceDbClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::InvalidUrl(_)
            | Self::Timeout { .. }
            | Self::InvalidResponse { .. }
            | Self::HttpStatus { .. } => None,
        }
    }
}

impl From<std::io::Error> for TraceDbClientError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for TraceDbClientError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceDbClientConfig {
    pub url: String,
    pub token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_id: Option<String>,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

impl TraceDbClientConfig {
    pub fn managed(url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: token.into(),
            database_id: None,
            branch_id: None,
            request_timeout_ms: default_request_timeout_ms(),
        }
    }

    pub fn with_database(mut self, database_id: impl Into<String>) -> Self {
        self.database_id = Some(database_id.into());
        self
    }

    pub fn with_branch(mut self, branch_id: impl Into<String>) -> Self {
        self.branch_id = Some(branch_id.into());
        self
    }

    pub fn with_database_branch(
        self,
        database_id: impl Into<String>,
        branch_id: impl Into<String>,
    ) -> Self {
        self.with_database(database_id).with_branch(branch_id)
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout_ms = timeout_ms(timeout);
        self
    }

    fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms.max(1))
    }
}

#[derive(Clone, Debug)]
pub struct TraceDbClient {
    pub config: TraceDbClientConfig,
}

impl TraceDbClient {
    pub fn new(config: TraceDbClientConfig) -> Self {
        Self { config }
    }

    pub fn ready(&self) -> TraceDbClientResult<Value> {
        self.get_json("/v1/ready")
    }

    pub fn ready_typed(&self) -> TraceDbClientResult<ReadyResponse> {
        self.get_typed("/v1/ready")
    }

    pub fn health(&self) -> TraceDbClientResult<Value> {
        self.get_json("/v1/health")
    }

    pub fn apply_schema(&self, schema: &TableSchema) -> TraceDbClientResult<Value> {
        self.post_json("/v1/schema/apply", schema)
    }

    pub fn apply_schema_typed(&self, schema: &TableSchema) -> TraceDbClientResult<EpochResponse> {
        self.post_typed("/v1/schema/apply", schema)
    }

    pub fn put(&self, record: &RecordInput) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/put", record)
    }

    pub fn put_typed(&self, record: &RecordInput) -> TraceDbClientResult<EpochResponse> {
        self.post_typed("/v1/records/put", record)
    }

    pub fn put_batch(&self, request: &RecordPutBatchRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/put-batch", request)
    }

    pub fn put_batch_typed(
        &self,
        request: &RecordPutBatchRequest,
    ) -> TraceDbClientResult<PutBatchResponse> {
        self.post_typed("/v1/records/put-batch", request)
    }

    pub fn patch(&self, request: &RecordPatchRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/patch", request)
    }

    pub fn patch_typed(&self, request: &RecordPatchRequest) -> TraceDbClientResult<EpochResponse> {
        self.post_typed("/v1/records/patch", request)
    }

    pub fn delete(&self, request: &RecordDeleteRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/delete", request)
    }

    pub fn delete_typed(
        &self,
        request: &RecordDeleteRequest,
    ) -> TraceDbClientResult<DeleteResponse> {
        self.post_typed("/v1/records/delete", request)
    }

    pub fn get(&self, request: &RecordGetRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/get", request)
    }

    pub fn get_record_typed(
        &self,
        request: &RecordGetRequest,
    ) -> TraceDbClientResult<GetRecordResponse> {
        self.post_typed("/v1/records/get", request)
    }

    pub fn scan(&self, request: &RecordScanRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/scan", request)
    }

    pub fn scan_typed(&self, request: &RecordScanRequest) -> TraceDbClientResult<RecordScanOutput> {
        self.post_typed("/v1/records/scan", request)
    }

    pub fn query(&self, query: &HybridQuery) -> TraceDbClientResult<Value> {
        self.post_json("/v1/query", query)
    }

    pub fn query_typed(&self, query: &HybridQuery) -> TraceDbClientResult<QueryResponse> {
        self.post_typed("/v1/query", query)
    }

    pub fn explain(&self, query: &HybridQuery) -> TraceDbClientResult<Value> {
        self.post_json("/v1/explain", query)
    }

    pub fn explain_typed(&self, query: &HybridQuery) -> TraceDbClientResult<HybridExplain> {
        self.post_typed("/v1/explain", query)
    }

    pub fn compact(&self) -> TraceDbClientResult<Value> {
        self.post_json("/v1/admin/compact", &json!({}))
    }

    pub fn compact_typed(&self) -> TraceDbClientResult<CompactResponse> {
        self.post_typed("/v1/admin/compact", &json!({}))
    }

    pub fn request_json(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> TraceDbClientResult<Value> {
        let target = HttpTarget::parse(&self.config.url)?;
        let request_path = target.path(path);
        let body_bytes = self.request_body_bytes(body)?;
        let timeout = self.config.request_timeout();
        let mut stream = target.connect(method, &request_path, timeout)?;
        let mut request = format!(
            "{method} {request_path} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nConnection: close\r\nContent-Length: {}\r\n",
            target.authority,
            body_bytes.len()
        );
        if !self.config.token.is_empty() {
            request.push_str(&format!("Authorization: Bearer {}\r\n", self.config.token));
        }
        if !body_bytes.is_empty() {
            request.push_str("Content-Type: application/json\r\n");
        }
        request.push_str("\r\n");
        stream
            .write_all(request.as_bytes())
            .map_err(|error| map_request_io_error(method, &request_path, timeout, error))?;
        if !body_bytes.is_empty() {
            stream
                .write_all(&body_bytes)
                .map_err(|error| map_request_io_error(method, &request_path, timeout, error))?;
        }
        stream
            .flush()
            .map_err(|error| map_request_io_error(method, &request_path, timeout, error))?;
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .map_err(|error| map_request_io_error(method, &request_path, timeout, error))?;
        parse_response(method, &request_path, &response)
    }

    pub fn table(&self, table: impl Into<String>) -> QueryBuilder {
        QueryBuilder {
            table: table.into(),
            tenant_id: None,
            text_field: None,
            text_query: None,
            vector_field: None,
            vector: None,
            freshness: FeatureFreshnessMode::Strict,
            limit: 10,
        }
    }

    fn get_json(&self, path: &str) -> TraceDbClientResult<Value> {
        self.request_json("GET", path, None)
    }

    fn get_typed<T: for<'de> Deserialize<'de>>(&self, path: &str) -> TraceDbClientResult<T> {
        decode_typed("GET", path, self.get_json(path)?)
    }

    fn post_json<T: Serialize>(&self, path: &str, body: &T) -> TraceDbClientResult<Value> {
        let value = serde_json::to_value(body)?;
        self.request_json("POST", path, Some(&value))
    }

    fn post_typed<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &T,
    ) -> TraceDbClientResult<R> {
        decode_typed("POST", path, self.post_json(path, body)?)
    }

    fn request_body_bytes(&self, body: Option<&Value>) -> TraceDbClientResult<Vec<u8>> {
        let Some(body) = body else {
            return Ok(Vec::new());
        };
        let mut body = body.clone();
        self.inject_route_metadata(&mut body);
        Ok(serde_json::to_vec(&body)?)
    }

    fn inject_route_metadata(&self, body: &mut Value) {
        let Value::Object(body) = body else {
            return;
        };
        if let Some(database_id) = &self.config.database_id {
            body.entry("database_id".to_string())
                .or_insert_with(|| Value::String(database_id.clone()));
        }
        if !body.contains_key("branch_id") {
            let branch_id = self.config.branch_id.clone().or_else(|| {
                self.config.database_id.as_ref().and_then(|_| {
                    body.get("database_id")
                        .and_then(Value::as_str)
                        .map(|database_id| format!("{database_id}:main"))
                })
            });
            if let Some(branch_id) = branch_id {
                body.insert("branch_id".to_string(), Value::String(branch_id));
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub ready: bool,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub latest_epoch: Option<u64>,
    #[serde(default)]
    pub durable_epoch: Option<u64>,
    #[serde(default)]
    pub recovery_state: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpochResponse {
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PutBatchResponse {
    pub epoch: u64,
    pub record_count: usize,
    #[serde(default)]
    pub write_timing: Option<WritePathTiming>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub deleted: bool,
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetRecordResponse {
    pub record: Option<RecordOutput>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    pub results: Vec<HybridQueryRow>,
    #[serde(default)]
    pub explain: Option<HybridExplain>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompactResponse {
    pub compacted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HttpTarget {
    authority: String,
    host: String,
    port: u16,
    base_path: String,
}

impl HttpTarget {
    fn parse(url: &str) -> TraceDbClientResult<Self> {
        let without_scheme = url
            .strip_prefix("http://")
            .ok_or_else(|| TraceDbClientError::InvalidUrl(url.to_string()))?;
        let (authority, base_path) = without_scheme
            .split_once('/')
            .map(|(authority, path)| (authority, format!("/{path}")))
            .unwrap_or((without_scheme, String::new()));
        if authority.is_empty() {
            return Err(TraceDbClientError::InvalidUrl(url.to_string()));
        }
        let (host, port) = if let Some((host, port)) = authority.rsplit_once(':') {
            let parsed_port = port
                .parse::<u16>()
                .map_err(|_| TraceDbClientError::InvalidUrl(url.to_string()))?;
            (host.to_string(), parsed_port)
        } else {
            (authority.to_string(), 80)
        };
        if host.is_empty() {
            return Err(TraceDbClientError::InvalidUrl(url.to_string()));
        }
        Ok(Self {
            authority: authority.to_string(),
            host,
            port,
            base_path,
        })
    }

    fn connect(
        &self,
        method: &str,
        path: &str,
        timeout: Duration,
    ) -> TraceDbClientResult<TcpStream> {
        let socket_addr = self.socket_addr(method, path, timeout)?;
        let stream = TcpStream::connect_timeout(&socket_addr, timeout)
            .map_err(|error| map_request_io_error(method, path, timeout, error))?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|error| map_request_io_error(method, path, timeout, error))?;
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|error| map_request_io_error(method, path, timeout, error))?;
        Ok(stream)
    }

    fn socket_addr(
        &self,
        method: &str,
        path: &str,
        timeout: Duration,
    ) -> TraceDbClientResult<SocketAddr> {
        (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|error| map_request_io_error(method, path, timeout, error))?
            .next()
            .ok_or_else(|| TraceDbClientError::InvalidUrl(self.authority.clone()))
    }

    fn path(&self, path: &str) -> String {
        if self.base_path.is_empty() {
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

fn default_request_timeout_ms() -> u64 {
    30_000
}

fn timeout_ms(timeout: Duration) -> u64 {
    timeout.as_millis().clamp(1, u64::MAX as u128) as u64
}

fn map_request_io_error(
    method: &str,
    path: &str,
    timeout: Duration,
    error: std::io::Error,
) -> TraceDbClientError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) {
        TraceDbClientError::Timeout {
            method: method.to_string(),
            path: path.to_string(),
            timeout_ms: timeout_ms(timeout),
        }
    } else {
        TraceDbClientError::Io(error)
    }
}

fn parse_response(method: &str, path: &str, response: &str) -> TraceDbClientResult<Value> {
    let (head, body) =
        response
            .split_once("\r\n\r\n")
            .ok_or_else(|| TraceDbClientError::InvalidResponse {
                method: method.to_string(),
                path: path.to_string(),
                message: "missing header boundary".to_string(),
            })?;
    let status_line = head
        .lines()
        .next()
        .ok_or_else(|| TraceDbClientError::InvalidResponse {
            method: method.to_string(),
            path: path.to_string(),
            message: "missing status line".to_string(),
        })?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| TraceDbClientError::InvalidResponse {
            method: method.to_string(),
            path: path.to_string(),
            message: "missing status code".to_string(),
        })?
        .parse::<u16>()
        .map_err(|_| TraceDbClientError::InvalidResponse {
            method: method.to_string(),
            path: path.to_string(),
            message: status_line.to_string(),
        })?;
    if !(200..300).contains(&status) {
        return Err(TraceDbClientError::HttpStatus {
            method: method.to_string(),
            path: path.to_string(),
            status,
            body: body.to_string(),
        });
    }
    if body.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(body).map_err(|error| TraceDbClientError::InvalidResponse {
        method: method.to_string(),
        path: path.to_string(),
        message: format!("invalid JSON body: {error}"),
    })
}

fn decode_typed<T: for<'de> Deserialize<'de>>(
    method: &str,
    path: &str,
    value: Value,
) -> TraceDbClientResult<T> {
    serde_json::from_value(value).map_err(|error| TraceDbClientError::InvalidResponse {
        method: method.to_string(),
        path: path.to_string(),
        message: format!("invalid JSON shape: {error}"),
    })
}

#[derive(Clone, Debug)]
pub struct QueryBuilder {
    table: String,
    tenant_id: Option<String>,
    text_field: Option<String>,
    text_query: Option<String>,
    vector_field: Option<String>,
    vector: Option<Vec<f32>>,
    freshness: FeatureFreshnessMode,
    limit: usize,
}

impl QueryBuilder {
    pub fn tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    pub fn match_text(mut self, field: impl Into<String>, query: impl Into<String>) -> Self {
        self.text_field = Some(field.into());
        self.text_query = Some(query.into());
        self
    }

    pub fn near(mut self, field: impl Into<String>, vector: Vec<f32>) -> Self {
        self.vector_field = Some(field.into());
        self.vector = Some(vector);
        self
    }

    pub fn freshness(mut self, freshness: FeatureFreshnessMode) -> Self {
        self.freshness = freshness;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn build(self) -> TraceQueryRequest {
        let freshness = match self.freshness {
            FeatureFreshnessMode::Strict => "Strict",
            FeatureFreshnessMode::Lazy
            | FeatureFreshnessMode::OnRead
            | FeatureFreshnessMode::AllowStale => "Lazy",
        };
        TraceQueryRequest {
            table: self.table,
            tenant_id: self.tenant_id.unwrap_or_default(),
            text: self.text_query,
            vector: self.vector,
            top_k: self.limit,
            freshness: freshness.to_string(),
            explain: true,
        }
    }

    pub fn put(self, id: impl Into<String>) -> RecordPutBuilder {
        RecordPutBuilder {
            table: self.table,
            tenant_id: self.tenant_id.unwrap_or_default(),
            id: id.into(),
            fields: Map::new(),
        }
    }

    pub fn scan(self) -> RecordScanBuilder {
        RecordScanBuilder {
            table: self.table,
            tenant_id: self.tenant_id.unwrap_or_default(),
            limit: 100,
        }
    }

    pub fn delete(self, id: impl Into<String>) -> RecordDeleteBuilder {
        RecordDeleteBuilder {
            table: self.table,
            tenant_id: self.tenant_id.unwrap_or_default(),
            id: id.into(),
            tombstone: "user_delete".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TraceQueryRequest {
    pub table: String,
    pub tenant_id: String,
    pub text: Option<String>,
    pub vector: Option<Vec<f32>>,
    pub top_k: usize,
    pub freshness: String,
    pub explain: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TraceHttpRequest {
    pub path: String,
    pub body: Value,
}

#[derive(Clone, Debug)]
pub struct RecordPutBuilder {
    table: String,
    tenant_id: String,
    id: String,
    fields: Map<String, Value>,
}

impl RecordPutBuilder {
    pub fn field(mut self, key: impl Into<String>, value: Value) -> Self {
        self.fields.insert(key.into(), value);
        self
    }

    pub fn fields(mut self, fields: Map<String, Value>) -> Self {
        self.fields.extend(fields);
        self
    }

    pub fn build(mut self) -> TraceHttpRequest {
        self.fields
            .entry("id".to_string())
            .or_insert_with(|| Value::String(self.id.clone()));
        self.fields
            .entry("tenant".to_string())
            .or_insert_with(|| Value::String(self.tenant_id.clone()));
        TraceHttpRequest {
            path: "/v1/records/put".to_string(),
            body: json!({
                "table": self.table,
                "id": self.id,
                "tenant_id": self.tenant_id,
                "fields": self.fields,
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RecordScanBuilder {
    table: String,
    tenant_id: String,
    limit: usize,
}

impl RecordScanBuilder {
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn build(self) -> TraceHttpRequest {
        TraceHttpRequest {
            path: "/v1/records/scan".to_string(),
            body: json!({
                "table": self.table,
                "tenant_id": self.tenant_id,
                "limit": self.limit,
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RecordDeleteBuilder {
    table: String,
    tenant_id: String,
    id: String,
    tombstone: String,
}

impl RecordDeleteBuilder {
    pub fn tombstone(mut self, tombstone: impl Into<String>) -> Self {
        self.tombstone = tombstone.into();
        self
    }

    pub fn build(self) -> TraceHttpRequest {
        TraceHttpRequest {
            path: "/v1/records/delete".to_string(),
            body: json!({
                "table": self.table,
                "tenant_id": self.tenant_id,
                "id": self.id,
                "tombstone": self.tombstone,
            }),
        }
    }
}
