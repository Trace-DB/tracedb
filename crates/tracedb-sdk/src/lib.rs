#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::env;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracedb_features::FeatureFreshnessMode;
use tracedb_query::{
    FreshnessMode, HybridExplain, HybridQuery, HybridQueryRow, RecordDeleteRequest,
    RecordGetRequest, RecordInput, RecordOutput, RecordPatchRequest, RecordPutBatchRequest,
    RecordScanOutput, RecordScanRequest, TableSchema, WritePathTiming,
};

pub type TraceDbClientResult<T> = std::result::Result<T, TraceDbClientError>;

#[derive(Debug)]
pub enum TraceDbClientError {
    InvalidUrl(String),
    InvalidConfig {
        variable: String,
        message: String,
    },
    InvalidRequest {
        method: String,
        path: String,
        message: String,
    },
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
            Self::InvalidConfig { variable, message } => {
                write!(f, "invalid TraceDB SDK config for {variable}: {message}")
            }
            Self::InvalidRequest {
                method,
                path,
                message,
            } => write!(
                f,
                "invalid TraceDB HTTP request for {method} {path}: {message}"
            ),
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
            | Self::InvalidConfig { .. }
            | Self::InvalidRequest { .. }
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

impl TraceDbClientError {
    pub fn error_response(&self) -> Option<ErrorResponse> {
        match self {
            Self::HttpStatus { body, .. } => serde_json::from_str::<ErrorResponse>(body).ok(),
            _ => None,
        }
    }

    pub fn server_error(&self) -> Option<String> {
        let Self::HttpStatus { body, .. } = self else {
            return None;
        };
        serde_json::from_str::<ErrorResponse>(body)
            .ok()
            .map(|response| response.error)
    }

    pub fn server_error_code(&self) -> Option<String> {
        let Self::HttpStatus { body, .. } = self else {
            return None;
        };
        serde_json::from_str::<ErrorResponse>(body)
            .ok()
            .and_then(|response| response.code)
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
    #[serde(default)]
    pub safe_retries: u8,
    #[serde(default)]
    pub idempotency_retries: u8,
}

impl TraceDbClientConfig {
    pub fn managed(url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: token.into(),
            database_id: None,
            branch_id: None,
            request_timeout_ms: default_request_timeout_ms(),
            safe_retries: 0,
            idempotency_retries: 0,
        }
    }

    pub fn from_env() -> TraceDbClientResult<Self> {
        Self::from_env_vars(env::vars())
    }

    pub fn from_env_vars<K, V, I>(vars: I) -> TraceDbClientResult<Self>
    where
        K: Into<String>,
        V: Into<String>,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut url = None;
        let mut token = None;
        let mut database_id = None;
        let mut branch_id = None;
        let mut timeout_ms = None;
        let mut safe_retries = None;
        let mut idempotency_retries = None;

        for (key, value) in vars {
            let key = key.into();
            let value = value.into();
            match key.as_str() {
                "TRACEDB_URL" => url = Some(value),
                "TRACEDB_TOKEN" => token = Some(value),
                "TRACEDB_DATABASE_ID" => database_id = Some(value),
                "TRACEDB_BRANCH_ID" => branch_id = Some(value),
                "TRACEDB_TIMEOUT_MS" => timeout_ms = Some(value),
                "TRACEDB_SAFE_RETRIES" => safe_retries = Some(value),
                "TRACEDB_IDEMPOTENCY_RETRIES" => idempotency_retries = Some(value),
                _ => {}
            }
        }

        let url = required_env("TRACEDB_URL", url)?;
        let mut config = Self::managed(url, token.unwrap_or_default());
        if let Some(database_id) = optional_env("TRACEDB_DATABASE_ID", database_id)? {
            config = config.with_database(database_id);
        }
        if let Some(branch_id) = optional_env("TRACEDB_BRANCH_ID", branch_id)? {
            config = config.with_branch(branch_id);
        }
        if let Some(timeout_ms) = optional_positive_u64_env("TRACEDB_TIMEOUT_MS", timeout_ms)? {
            config.request_timeout_ms = timeout_ms;
        }
        if let Some(retries) = optional_u8_env("TRACEDB_SAFE_RETRIES", safe_retries)? {
            config.safe_retries = retries;
        }
        if let Some(retries) = optional_u8_env("TRACEDB_IDEMPOTENCY_RETRIES", idempotency_retries)?
        {
            config.idempotency_retries = retries;
        }
        Ok(config)
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

    pub fn with_safe_retries(mut self, retries: u8) -> Self {
        self.safe_retries = retries;
        self
    }

    pub fn with_idempotency_retries(mut self, retries: u8) -> Self {
        self.idempotency_retries = retries;
        self
    }

    fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms.max(1))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceDbRequestOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_context: Option<TraceDbActorContext>,
}

impl TraceDbRequestOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn with_actor_context(mut self, actor_context: TraceDbActorContext) -> Self {
        self.actor_context = Some(actor_context);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceDbActorContext {
    pub tenant_id: String,
    pub database_id: String,
    pub branch_id: String,
    pub token_identity: String,
    pub request_id: String,
    #[serde(default)]
    pub policy_epoch: u64,
    #[serde(default)]
    pub scopes: Vec<String>,
}

impl TraceDbActorContext {
    pub fn new(
        tenant_id: impl Into<String>,
        database_id: impl Into<String>,
        branch_id: impl Into<String>,
        token_identity: impl Into<String>,
        request_id: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            database_id: database_id.into(),
            branch_id: branch_id.into(),
            token_identity: token_identity.into(),
            request_id: request_id.into(),
            policy_epoch: 0,
            scopes: Vec::new(),
        }
    }

    pub fn with_policy_epoch(mut self, policy_epoch: u64) -> Self {
        self.policy_epoch = policy_epoch;
        self
    }

    pub fn with_scopes(mut self, scopes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }
}

#[derive(Clone, Debug)]
pub struct TraceDbClient {
    pub config: TraceDbClientConfig,
}

pub type TraceDb = TraceDbClient;

impl TraceDbClient {
    pub fn new(config: TraceDbClientConfig) -> Self {
        Self { config }
    }

    pub fn connect(config: TraceDbClientConfig) -> TraceDbClientResult<Self> {
        HttpTarget::parse(&config.url)?;
        Ok(Self::new(config))
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

    pub fn health_typed(&self) -> TraceDbClientResult<HealthResponse> {
        self.get_typed("/v1/health")
    }

    pub fn list_databases(&self) -> TraceDbClientResult<Value> {
        self.get_json("/v1/databases")
    }

    pub fn list_databases_typed(&self) -> TraceDbClientResult<DatabasesResponse> {
        self.get_typed("/v1/databases")
    }

    pub fn list_branches(&self) -> TraceDbClientResult<Value> {
        self.get_json("/v1/branches")
    }

    pub fn list_branches_typed(&self) -> TraceDbClientResult<BranchesResponse> {
        self.get_typed("/v1/branches")
    }

    pub fn public_safe_metrics(&self) -> TraceDbClientResult<Value> {
        self.get_json("/v1/metrics/public-safe")
    }

    pub fn public_safe_metrics_typed(&self) -> TraceDbClientResult<MetricsResponse> {
        self.get_typed("/v1/metrics/public-safe")
    }

    pub fn apply_schema(&self, schema: &TableSchema) -> TraceDbClientResult<Value> {
        self.post_json("/v1/schema/apply", schema)
    }

    pub fn apply_schema_with_options(
        &self,
        schema: &TableSchema,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        self.post_json_with_options("/v1/schema/apply", schema, options)
    }

    pub fn apply_schema_typed(&self, schema: &TableSchema) -> TraceDbClientResult<EpochResponse> {
        self.post_typed("/v1/schema/apply", schema)
    }

    pub fn apply_schema_typed_with_options(
        &self,
        schema: &TableSchema,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<EpochResponse> {
        self.post_typed_with_options("/v1/schema/apply", schema, options)
    }

    pub fn put(&self, record: &RecordInput) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/put", record)
    }

    pub fn put_with_options(
        &self,
        record: &RecordInput,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        self.post_json_with_options("/v1/records/put", record, options)
    }

    pub fn put_typed(&self, record: &RecordInput) -> TraceDbClientResult<EpochResponse> {
        self.post_typed("/v1/records/put", record)
    }

    pub fn put_typed_with_options(
        &self,
        record: &RecordInput,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<EpochResponse> {
        self.post_typed_with_options("/v1/records/put", record, options)
    }

    pub fn put_batch(&self, request: &RecordPutBatchRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/put-batch", request)
    }

    pub fn put_batch_with_options(
        &self,
        request: &RecordPutBatchRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        self.post_json_with_options("/v1/records/put-batch", request, options)
    }

    pub fn put_batch_typed(
        &self,
        request: &RecordPutBatchRequest,
    ) -> TraceDbClientResult<PutBatchResponse> {
        self.post_typed("/v1/records/put-batch", request)
    }

    pub fn put_batch_typed_with_options(
        &self,
        request: &RecordPutBatchRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<PutBatchResponse> {
        self.post_typed_with_options("/v1/records/put-batch", request, options)
    }

    pub fn patch(&self, request: &RecordPatchRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/patch", request)
    }

    pub fn patch_with_options(
        &self,
        request: &RecordPatchRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        self.post_json_with_options("/v1/records/patch", request, options)
    }

    pub fn patch_typed(&self, request: &RecordPatchRequest) -> TraceDbClientResult<EpochResponse> {
        self.post_typed("/v1/records/patch", request)
    }

    pub fn patch_typed_with_options(
        &self,
        request: &RecordPatchRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<EpochResponse> {
        self.post_typed_with_options("/v1/records/patch", request, options)
    }

    pub fn delete(&self, request: &RecordDeleteRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/records/delete", request)
    }

    pub fn delete_with_options(
        &self,
        request: &RecordDeleteRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        self.post_json_with_options("/v1/records/delete", request, options)
    }

    pub fn delete_typed(
        &self,
        request: &RecordDeleteRequest,
    ) -> TraceDbClientResult<DeleteResponse> {
        self.post_typed("/v1/records/delete", request)
    }

    pub fn delete_typed_with_options(
        &self,
        request: &RecordDeleteRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<DeleteResponse> {
        self.post_typed_with_options("/v1/records/delete", request, options)
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

    pub fn traceql(&self, query: impl Into<String>) -> TraceDbClientResult<Value> {
        let request = TraceQlQueryRequest::new(query);
        self.traceql_request(&request)
    }

    pub fn traceql_request(&self, request: &TraceQlQueryRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/traceql", request)
    }

    pub fn traceql_typed(&self, query: impl Into<String>) -> TraceDbClientResult<QueryResponse> {
        let request = TraceQlQueryRequest::new(query);
        self.traceql_request_typed(&request)
    }

    pub fn traceql_request_typed(
        &self,
        request: &TraceQlQueryRequest,
    ) -> TraceDbClientResult<QueryResponse> {
        self.post_typed("/v1/traceql", request)
    }

    pub fn graphql(&self, query: impl Into<String>) -> TraceDbClientResult<Value> {
        let request = GraphQlQueryRequest::new(query);
        self.graphql_request(&request)
    }

    pub fn graphql_request(&self, request: &GraphQlQueryRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/graphql", request)
    }

    pub fn graphql_typed(&self, query: impl Into<String>) -> TraceDbClientResult<GraphQlResponse> {
        let request = GraphQlQueryRequest::new(query);
        self.graphql_request_typed(&request)
    }

    pub fn graphql_request_typed(
        &self,
        request: &GraphQlQueryRequest,
    ) -> TraceDbClientResult<GraphQlResponse> {
        self.post_typed("/v1/graphql", request)
    }

    pub fn bounded_graphql(&self, query: impl Into<String>) -> TraceDbClientResult<Value> {
        let request = GraphQlQueryRequest::new(query);
        self.bounded_graphql_request(&request)
    }

    pub fn bounded_graphql_request(
        &self,
        request: &GraphQlQueryRequest,
    ) -> TraceDbClientResult<Value> {
        self.post_json("/v1/graphql/bounded", request)
    }

    pub fn bounded_graphql_typed(
        &self,
        query: impl Into<String>,
    ) -> TraceDbClientResult<QueryResponse> {
        let request = GraphQlQueryRequest::new(query);
        self.bounded_graphql_request_typed(&request)
    }

    pub fn bounded_graphql_request_typed(
        &self,
        request: &GraphQlQueryRequest,
    ) -> TraceDbClientResult<QueryResponse> {
        self.post_typed("/v1/graphql/bounded", request)
    }

    pub fn graphql_schema(&self) -> TraceDbClientResult<Value> {
        self.get_json("/v1/graphql/schema")
    }

    pub fn graphql_schema_typed(&self) -> TraceDbClientResult<GraphQlSchemaResponse> {
        self.get_typed("/v1/graphql/schema")
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

    pub fn compact_with_options(
        &self,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        self.post_json_with_options("/v1/admin/compact", &json!({}), options)
    }

    pub fn compact_typed(&self) -> TraceDbClientResult<CompactResponse> {
        self.post_typed("/v1/admin/compact", &json!({}))
    }

    pub fn compact_typed_with_options(
        &self,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<CompactResponse> {
        self.post_typed_with_options("/v1/admin/compact", &json!({}), options)
    }

    pub fn list_admin_jobs(&self) -> TraceDbClientResult<Value> {
        self.get_json("/v1/admin/jobs")
    }

    pub fn list_admin_jobs_typed(&self) -> TraceDbClientResult<JobsResponse> {
        self.get_typed("/v1/admin/jobs")
    }

    pub fn snapshot(&self, request: &SnapshotRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/admin/snapshot", request)
    }

    pub fn snapshot_with_options(
        &self,
        request: &SnapshotRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        self.post_json_with_options("/v1/admin/snapshot", request, options)
    }

    pub fn snapshot_typed(
        &self,
        request: &SnapshotRequest,
    ) -> TraceDbClientResult<SnapshotResponse> {
        self.post_typed("/v1/admin/snapshot", request)
    }

    pub fn snapshot_typed_with_options(
        &self,
        request: &SnapshotRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<SnapshotResponse> {
        self.post_typed_with_options("/v1/admin/snapshot", request, options)
    }

    pub fn restore(&self, request: &RestoreRequest) -> TraceDbClientResult<Value> {
        self.post_json("/v1/admin/restore", request)
    }

    pub fn restore_with_options(
        &self,
        request: &RestoreRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        self.post_json_with_options("/v1/admin/restore", request, options)
    }

    pub fn restore_typed(&self, request: &RestoreRequest) -> TraceDbClientResult<RestoreResponse> {
        self.post_typed("/v1/admin/restore", request)
    }

    pub fn restore_typed_with_options(
        &self,
        request: &RestoreRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<RestoreResponse> {
        self.post_typed_with_options("/v1/admin/restore", request, options)
    }

    pub fn request_json(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> TraceDbClientResult<Value> {
        self.request_json_with_options(method, path, body, &TraceDbRequestOptions::default())
    }

    pub fn request_json_with_options(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        let attempts = self.request_attempts(method, path, options);
        for attempt in 0..attempts {
            match self.request_json_once(method, path, body, options) {
                Ok(value) => return Ok(value),
                Err(error) if is_retryable_error(&error) && attempt + 1 < attempts => {
                    thread::sleep(retry_backoff_delay(attempt));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("request attempts should be at least one")
    }

    fn request_attempts(&self, method: &str, path: &str, options: &TraceDbRequestOptions) -> u8 {
        if is_idempotent_retry_request(method, path)
            && options
                .idempotency_key
                .as_deref()
                .is_some_and(|key| !key.is_empty())
        {
            self.config.idempotency_retries.saturating_add(1)
        } else if is_retry_safe_request(method, path) {
            self.config.safe_retries.saturating_add(1)
        } else {
            1
        }
    }

    fn request_json_once(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        let target = HttpTarget::parse(&self.config.url)?;
        let request_path = target.path(path);
        let body_bytes = self.request_body_bytes(body)?;
        let timeout = self.config.request_timeout();
        let idempotency_key_header = idempotency_key_header(method, &request_path, options)?;
        let mut stream = target.connect(method, &request_path, timeout)?;
        let mut request = format!(
            "{method} {request_path} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nConnection: close\r\nContent-Length: {}\r\n",
            target.authority,
            body_bytes.len()
        );
        if !self.config.token.is_empty() {
            request.push_str(&format!("Authorization: Bearer {}\r\n", self.config.token));
        }
        request.push_str(&idempotency_key_header);
        request.push_str(&self.actor_headers(options)?);
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
        if response.is_empty() {
            return Err(TraceDbClientError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "empty HTTP response",
            )));
        }
        parse_response(method, &request_path, &response)
    }

    pub fn table(&self, table: impl Into<String>) -> TableHandle {
        QueryBuilder {
            client_config: Some(self.config.clone()),
            table: table.into(),
            tenant_id: None,
            text_field: None,
            text_query: None,
            vector_field: None,
            vector: None,
            scalar_eq: Map::new(),
            freshness: FeatureFreshnessMode::Strict,
            limit: 10,
            cursor: None,
            explain: true,
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

    fn post_json_with_options<T: Serialize>(
        &self,
        path: &str,
        body: &T,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        let value = serde_json::to_value(body)?;
        self.request_json_with_options("POST", path, Some(&value), options)
    }

    fn post_typed<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &T,
    ) -> TraceDbClientResult<R> {
        decode_typed("POST", path, self.post_json(path, body)?)
    }

    fn post_typed_with_options<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &T,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<R> {
        decode_typed(
            "POST",
            path,
            self.post_json_with_options(path, body, options)?,
        )
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

    fn actor_headers(&self, options: &TraceDbRequestOptions) -> TraceDbClientResult<String> {
        let mut headers = String::new();
        for (name, value) in self.actor_header_pairs(options)? {
            headers.push_str(&header_line(name, &value)?);
        }
        Ok(headers)
    }

    fn actor_header_pairs(
        &self,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Vec<(&'static str, String)>> {
        let mut headers = Vec::new();
        if let Some(actor) = &options.actor_context {
            headers.push(("x-tracedb-tenant-id", actor.tenant_id.clone()));
            headers.push(("x-tracedb-database-id", actor.database_id.clone()));
            headers.push(("x-tracedb-branch-id", actor.branch_id.clone()));
            headers.push(("x-tracedb-token-identity", actor.token_identity.clone()));
            headers.push(("x-tracedb-request-id", actor.request_id.clone()));
            headers.push(("x-tracedb-policy-epoch", actor.policy_epoch.to_string()));
            if !actor.scopes.is_empty() {
                headers.push(("x-tracedb-scopes", actor.scopes.join(",")));
            }
        } else {
            if let Some(database_id) = &self.config.database_id {
                headers.push(("x-tracedb-database-id", database_id.clone()));
            }
            if let Some(branch_id) = &self.config.branch_id {
                headers.push(("x-tracedb-branch-id", branch_id.clone()));
            }
        }
        for (name, value) in &headers {
            validate_header_value(name, value)?;
        }
        Ok(headers)
    }
}

#[derive(Clone, Debug)]
pub struct TraceDbAsyncClient {
    inner: TraceDbClient,
    http_client: reqwest::Client,
}

impl TraceDbAsyncClient {
    pub fn new(config: TraceDbClientConfig) -> Self {
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(16)
            .build()
            .expect("TraceDB async HTTP client configuration is valid");
        Self {
            inner: TraceDbClient::new(config),
            http_client,
        }
    }

    pub fn from_blocking(client: TraceDbClient) -> Self {
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(16)
            .build()
            .expect("TraceDB async HTTP client configuration is valid");
        Self {
            inner: client,
            http_client,
        }
    }

    pub fn blocking_client(&self) -> &TraceDbClient {
        &self.inner
    }

    pub async fn request_json(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> TraceDbClientResult<Value> {
        self.request_json_with_options(method, path, body, &TraceDbRequestOptions::default())
            .await
    }

    pub async fn request_json_with_options(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        let attempts = self.inner.request_attempts(method, path, options);
        for attempt in 0..attempts {
            match self.request_json_once(method, path, body, options).await {
                Ok(value) => return Ok(value),
                Err(error) if is_retryable_error(&error) && attempt + 1 < attempts => {
                    tokio::time::sleep(retry_backoff_delay(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("request attempts should be at least one")
    }

    pub async fn ready(&self) -> TraceDbClientResult<Value> {
        self.request_json("GET", "/v1/ready", None).await
    }

    pub async fn ready_typed(&self) -> TraceDbClientResult<ReadyResponse> {
        self.get_typed("/v1/ready").await
    }

    pub async fn health(&self) -> TraceDbClientResult<Value> {
        self.request_json("GET", "/v1/health", None).await
    }

    pub async fn health_typed(&self) -> TraceDbClientResult<HealthResponse> {
        self.get_typed("/v1/health").await
    }

    pub async fn list_databases_typed(&self) -> TraceDbClientResult<DatabasesResponse> {
        self.get_typed("/v1/databases").await
    }

    pub async fn list_branches_typed(&self) -> TraceDbClientResult<BranchesResponse> {
        self.get_typed("/v1/branches").await
    }

    pub async fn public_safe_metrics_typed(&self) -> TraceDbClientResult<MetricsResponse> {
        self.get_typed("/v1/metrics/public-safe").await
    }

    pub async fn list_admin_jobs_typed(&self) -> TraceDbClientResult<JobsResponse> {
        self.get_typed("/v1/admin/jobs").await
    }

    pub async fn apply_schema_typed(
        &self,
        schema: &TableSchema,
    ) -> TraceDbClientResult<EpochResponse> {
        self.post_typed("/v1/schema/apply", schema).await
    }

    pub async fn apply_schema_typed_with_options(
        &self,
        schema: &TableSchema,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<EpochResponse> {
        self.post_typed_with_options("/v1/schema/apply", schema, options)
            .await
    }

    pub async fn put_typed(&self, record: &RecordInput) -> TraceDbClientResult<EpochResponse> {
        self.post_typed("/v1/records/put", record).await
    }

    pub async fn put_typed_with_options(
        &self,
        record: &RecordInput,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<EpochResponse> {
        self.post_typed_with_options("/v1/records/put", record, options)
            .await
    }

    pub async fn put_batch_typed(
        &self,
        request: &RecordPutBatchRequest,
    ) -> TraceDbClientResult<PutBatchResponse> {
        self.post_typed("/v1/records/put-batch", request).await
    }

    pub async fn put_batch_typed_with_options(
        &self,
        request: &RecordPutBatchRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<PutBatchResponse> {
        self.post_typed_with_options("/v1/records/put-batch", request, options)
            .await
    }

    pub async fn patch_typed(
        &self,
        request: &RecordPatchRequest,
    ) -> TraceDbClientResult<EpochResponse> {
        self.post_typed("/v1/records/patch", request).await
    }

    pub async fn patch_typed_with_options(
        &self,
        request: &RecordPatchRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<EpochResponse> {
        self.post_typed_with_options("/v1/records/patch", request, options)
            .await
    }

    pub async fn delete_typed(
        &self,
        request: &RecordDeleteRequest,
    ) -> TraceDbClientResult<DeleteResponse> {
        self.post_typed("/v1/records/delete", request).await
    }

    pub async fn delete_typed_with_options(
        &self,
        request: &RecordDeleteRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<DeleteResponse> {
        self.post_typed_with_options("/v1/records/delete", request, options)
            .await
    }

    pub async fn get_record_typed(
        &self,
        request: &RecordGetRequest,
    ) -> TraceDbClientResult<GetRecordResponse> {
        self.post_typed("/v1/records/get", request).await
    }

    pub async fn scan_typed(
        &self,
        request: &RecordScanRequest,
    ) -> TraceDbClientResult<RecordScanOutput> {
        self.post_typed("/v1/records/scan", request).await
    }

    pub async fn query_typed(&self, query: &HybridQuery) -> TraceDbClientResult<QueryResponse> {
        self.post_typed("/v1/query", query).await
    }

    pub async fn traceql_typed(
        &self,
        query: impl Into<String>,
    ) -> TraceDbClientResult<QueryResponse> {
        let request = TraceQlQueryRequest::new(query);
        self.post_typed("/v1/traceql", &request).await
    }

    pub async fn graphql_typed(
        &self,
        query: impl Into<String>,
    ) -> TraceDbClientResult<GraphQlResponse> {
        let request = GraphQlQueryRequest::new(query);
        self.post_typed("/v1/graphql", &request).await
    }

    pub async fn bounded_graphql_typed(
        &self,
        query: impl Into<String>,
    ) -> TraceDbClientResult<QueryResponse> {
        let request = GraphQlQueryRequest::new(query);
        self.post_typed("/v1/graphql/bounded", &request).await
    }

    pub async fn graphql_schema_typed(&self) -> TraceDbClientResult<GraphQlSchemaResponse> {
        self.get_typed("/v1/graphql/schema").await
    }

    pub async fn explain_typed(&self, query: &HybridQuery) -> TraceDbClientResult<HybridExplain> {
        self.post_typed("/v1/explain", query).await
    }

    pub async fn compact_typed(&self) -> TraceDbClientResult<CompactResponse> {
        self.post_typed("/v1/admin/compact", &json!({})).await
    }

    pub async fn compact_typed_with_options(
        &self,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<CompactResponse> {
        self.post_typed_with_options("/v1/admin/compact", &json!({}), options)
            .await
    }

    pub async fn snapshot_typed(
        &self,
        request: &SnapshotRequest,
    ) -> TraceDbClientResult<SnapshotResponse> {
        self.post_typed("/v1/admin/snapshot", request).await
    }

    pub async fn snapshot_typed_with_options(
        &self,
        request: &SnapshotRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<SnapshotResponse> {
        self.post_typed_with_options("/v1/admin/snapshot", request, options)
            .await
    }

    pub async fn restore_typed(
        &self,
        request: &RestoreRequest,
    ) -> TraceDbClientResult<RestoreResponse> {
        self.post_typed("/v1/admin/restore", request).await
    }

    pub async fn restore_typed_with_options(
        &self,
        request: &RestoreRequest,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<RestoreResponse> {
        self.post_typed_with_options("/v1/admin/restore", request, options)
            .await
    }

    async fn get_typed<T: for<'de> Deserialize<'de>>(&self, path: &str) -> TraceDbClientResult<T> {
        decode_typed("GET", path, self.request_json("GET", path, None).await?)
    }

    async fn post_typed<B, R>(&self, path: &str, body: &B) -> TraceDbClientResult<R>
    where
        B: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let value = serde_json::to_value(body)?;
        decode_typed(
            "POST",
            path,
            self.request_json("POST", path, Some(&value)).await?,
        )
    }

    async fn post_typed_with_options<B, R>(
        &self,
        path: &str,
        body: &B,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<R>
    where
        B: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let value = serde_json::to_value(body)?;
        decode_typed(
            "POST",
            path,
            self.request_json_with_options("POST", path, Some(&value), options)
                .await?,
        )
    }

    async fn request_json_once(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<Value> {
        let target = HttpTarget::parse(&self.inner.config.url)?;
        let request_path = target.path(path);
        let body_bytes = self.inner.request_body_bytes(body)?;
        let timeout = self.inner.config.request_timeout();
        let method_value = reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| {
            TraceDbClientError::InvalidRequest {
                method: method.to_string(),
                path: request_path.clone(),
                message: format!("invalid HTTP method: {error}"),
            }
        })?;
        let url = format!("http://{}{}", target.authority, request_path);
        let mut request = self
            .http_client
            .request(method_value, url)
            .timeout(timeout)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(
                reqwest::header::CONTENT_LENGTH,
                body_bytes.len().to_string(),
            );
        if !self.inner.config.token.is_empty() {
            request = request.bearer_auth(&self.inner.config.token);
        }
        if let Some(key) = validated_idempotency_key(method, &request_path, options)? {
            request = request.header("Idempotency-Key", key);
        }
        for (name, value) in self.inner.actor_header_pairs(options)? {
            request = request.header(name, value);
        }
        if !body_bytes.is_empty() {
            request = request.header(reqwest::header::CONTENT_TYPE, "application/json");
        }
        let response = request
            .body(body_bytes)
            .send()
            .await
            .map_err(|error| map_reqwest_error(method, &request_path, timeout, error))?;
        let status = response.status().as_u16();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| map_reqwest_error(method, &request_path, timeout, error))?;
        if !(200..300).contains(&status) {
            return Err(TraceDbClientError::HttpStatus {
                method: method.to_string(),
                path: request_path,
                status,
                body: String::from_utf8_lossy(&bytes).to_string(),
            });
        }
        if bytes.iter().all(u8::is_ascii_whitespace) || bytes.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&bytes).map_err(|error| TraceDbClientError::InvalidResponse {
            method: method.to_string(),
            path: request_path,
            message: format!("invalid JSON body: {error}"),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadyResponse {
    #[serde(default)]
    pub ok: Option<bool>,
    pub ready: bool,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub latest_epoch: Option<u64>,
    #[serde(default)]
    pub durable_epoch: Option<u64>,
    #[serde(default)]
    pub recovery_state: Option<String>,
    #[serde(default)]
    pub engine_url: Option<String>,
    #[serde(default)]
    pub engine_health_checked: Option<bool>,
    #[serde(default)]
    pub engine_status_code: Option<u16>,
    #[serde(default)]
    pub catalog_databases: Option<u64>,
    #[serde(default)]
    pub metered_requests: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub engine_url: Option<String>,
    #[serde(default)]
    pub catalog_databases: Option<u64>,
    #[serde(default)]
    pub metered_requests: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DatabaseSummary {
    pub database_id: String,
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DatabasesResponse {
    pub databases: Vec<DatabaseSummary>,
    #[serde(default)]
    pub gateway: Option<bool>,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BranchSummary {
    pub branch_id: String,
    #[serde(default)]
    pub database_id: Option<String>,
    #[serde(default)]
    pub parent_branch_id: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub latest_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BranchesResponse {
    pub branches: Vec<BranchSummary>,
    #[serde(default)]
    pub gateway: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricsResponse {
    #[serde(default)]
    pub gateway: Option<bool>,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub latest_epoch: Option<u64>,
    #[serde(default)]
    pub durable_epoch: Option<u64>,
    #[serde(default)]
    pub segment_count: Option<usize>,
    #[serde(default)]
    pub index_count: Option<usize>,
    #[serde(default)]
    pub module_count: Option<usize>,
    #[serde(default)]
    pub schema_count: Option<usize>,
    #[serde(default)]
    pub recovery_state: Option<String>,
    #[serde(default)]
    pub requests: Option<u64>,
    #[serde(default)]
    pub rate_limit_enabled: Option<bool>,
    #[serde(default)]
    pub rate_limit_requests: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceQlQueryRequest {
    pub query: String,
}

impl TraceQlQueryRequest {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
        }
    }

    pub fn command<T: Serialize>(
        command: impl AsRef<str>,
        payload: &T,
    ) -> TraceDbClientResult<Self> {
        Ok(Self {
            query: format!("{} {}", command.as_ref(), serde_json::to_string(payload)?),
        })
    }

    pub fn schema_apply(schema: &TableSchema) -> TraceDbClientResult<Self> {
        Self::command("SCHEMA APPLY", schema)
    }

    pub fn put(record: &RecordInput) -> TraceDbClientResult<Self> {
        Self::command("RECORD PUT", record)
    }

    pub fn batch(request: &RecordPutBatchRequest) -> TraceDbClientResult<Self> {
        Self::command("RECORD BATCH", request)
    }

    pub fn patch(request: &RecordPatchRequest) -> TraceDbClientResult<Self> {
        Self::command("RECORD PATCH", request)
    }

    pub fn delete(request: &RecordDeleteRequest) -> TraceDbClientResult<Self> {
        Self::command("RECORD DELETE", request)
    }

    pub fn get(request: &RecordGetRequest) -> TraceDbClientResult<Self> {
        Self::command("RECORD GET", request)
    }

    pub fn scan(request: &RecordScanRequest) -> TraceDbClientResult<Self> {
        Self::command("RECORD SCAN", request)
    }

    pub fn query(query: &HybridQuery) -> TraceDbClientResult<Self> {
        Self::command("QUERY", query)
    }

    pub fn explain(query: &HybridQuery) -> TraceDbClientResult<Self> {
        Self::command("EXPLAIN", query)
    }

    pub fn jobs_list() -> Self {
        Self {
            query: "JOBS LIST".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphQlQueryRequest {
    pub query: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub variables: Value,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "operationName"
    )]
    pub operation_name: Option<String>,
}

impl GraphQlQueryRequest {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            variables: Value::Null,
            operation_name: None,
        }
    }

    pub fn with_variables(mut self, variables: Value) -> Self {
        self.variables = variables;
        self
    }

    pub fn with_operation_name(mut self, operation_name: impl Into<String>) -> Self {
        self.operation_name = Some(operation_name.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphQlResponse {
    #[serde(default)]
    pub data: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<GraphQlError>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphQlError {
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GraphQlSchemaResponse {
    pub adapter: String,
    pub schema: String,
    pub tables: Vec<String>,
    #[serde(alias = "execution_caveat")]
    pub execution: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompactResponse {
    pub compacted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRequest {
    pub target: String,
}

impl SnapshotRequest {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub snapshot: bool,
    pub target: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RestoreRequest {
    pub source: String,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify_record: Option<RecordGetRequest>,
}

impl RestoreRequest {
    pub fn new(source: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            target: target.into(),
            verify_record: None,
        }
    }

    pub fn verify_record(mut self, request: RecordGetRequest) -> Self {
        self.verify_record = Some(request);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RestoreResponse {
    pub restored: bool,
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub verification: Option<RestoreVerification>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RestoreVerification {
    pub status: String,
    pub record_visible: bool,
    #[serde(default)]
    pub request: Option<RecordGetRequest>,
    #[serde(default)]
    pub record: Option<RecordOutput>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdminJob {
    pub queue: String,
    pub state: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct JobsResponse {
    pub jobs: Vec<AdminJob>,
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

fn retry_backoff_delay(attempt: u8) -> Duration {
    let shift = u32::from(attempt).min(16);
    let base_ms = 100_u64.saturating_mul(1_u64 << shift).min(5_000);
    let jitter_quarter = base_ms / 4;
    let jitter_range = jitter_quarter.saturating_mul(2).saturating_add(1);
    let jitter_offset = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        % jitter_range;
    let delay_ms = base_ms
        .saturating_sub(jitter_quarter)
        .saturating_add(jitter_offset)
        .min(5_000)
        .max(1);
    Duration::from_millis(delay_ms)
}

fn required_env(variable: &str, value: Option<String>) -> TraceDbClientResult<String> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(TraceDbClientError::InvalidConfig {
            variable: variable.to_string(),
            message: format!("{variable} is required"),
        }),
    }
}

fn optional_env(variable: &str, value: Option<String>) -> TraceDbClientResult<Option<String>> {
    match value {
        Some(value) if value.trim().is_empty() => Err(TraceDbClientError::InvalidConfig {
            variable: variable.to_string(),
            message: format!("{variable} must not be empty when set"),
        }),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

fn optional_positive_u64_env(
    variable: &str,
    value: Option<String>,
) -> TraceDbClientResult<Option<u64>> {
    let Some(value) = optional_env(variable, value)? else {
        return Ok(None);
    };
    let parsed = value
        .parse::<u64>()
        .map_err(|_| TraceDbClientError::InvalidConfig {
            variable: variable.to_string(),
            message: format!("{variable} must be a positive integer"),
        })?;
    if parsed == 0 {
        return Err(TraceDbClientError::InvalidConfig {
            variable: variable.to_string(),
            message: format!("{variable} must be greater than 0"),
        });
    }
    Ok(Some(parsed))
}

fn optional_u8_env(variable: &str, value: Option<String>) -> TraceDbClientResult<Option<u8>> {
    let Some(value) = optional_env(variable, value)? else {
        return Ok(None);
    };
    value
        .parse::<u8>()
        .map(Some)
        .map_err(|_| TraceDbClientError::InvalidConfig {
            variable: variable.to_string(),
            message: format!("{variable} must be an integer from 0 to 255"),
        })
}

fn idempotency_key_header(
    method: &str,
    path: &str,
    options: &TraceDbRequestOptions,
) -> TraceDbClientResult<String> {
    let Some(key) = validated_idempotency_key(method, path, options)? else {
        return Ok(String::new());
    };
    Ok(format!("Idempotency-Key: {key}\r\n"))
}

fn validated_idempotency_key<'a>(
    method: &str,
    path: &str,
    options: &'a TraceDbRequestOptions,
) -> TraceDbClientResult<Option<&'a str>> {
    let Some(key) = options.idempotency_key.as_deref() else {
        return Ok(None);
    };
    if key.is_empty() || key.contains('\r') || key.contains('\n') {
        return Err(TraceDbClientError::InvalidRequest {
            method: method.to_string(),
            path: path.to_string(),
            message: "idempotency key must be non-empty and must not contain CR or LF".to_string(),
        });
    }
    Ok(Some(key))
}

fn header_line(name: &str, value: &str) -> TraceDbClientResult<String> {
    validate_header_value(name, value)?;
    Ok(format!("{name}: {value}\r\n"))
}

fn validate_header_value(name: &str, value: &str) -> TraceDbClientResult<()> {
    if value.contains('\r') || value.contains('\n') {
        return Err(TraceDbClientError::InvalidRequest {
            method: "CONFIG".to_string(),
            path: name.to_string(),
            message: "header values must not contain CR or LF".to_string(),
        });
    }
    Ok(())
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

fn map_reqwest_error(
    method: &str,
    path: &str,
    timeout: Duration,
    error: reqwest::Error,
) -> TraceDbClientError {
    if error.is_timeout() {
        TraceDbClientError::Timeout {
            method: method.to_string(),
            path: path.to_string(),
            timeout_ms: timeout_ms(timeout),
        }
    } else {
        TraceDbClientError::Io(std::io::Error::other(error.to_string()))
    }
}

fn is_retry_safe_request(method: &str, path: &str) -> bool {
    matches!(
        (method, strip_query(path)),
        ("GET", "/v1/health")
            | ("GET", "/v1/ready")
            | ("POST", "/v1/records/get")
            | ("POST", "/v1/records/scan")
            | ("POST", "/v1/query")
            | ("POST", "/v1/traceql")
            | ("POST", "/v1/graphql")
            | ("POST", "/v1/graphql/bounded")
            | ("GET", "/v1/graphql/schema")
            | ("POST", "/v1/explain")
    )
}

fn is_idempotent_retry_request(method: &str, path: &str) -> bool {
    matches!(
        (method, strip_query(path)),
        ("POST", "/v1/schema/apply")
            | ("POST", "/v1/insert")
            | ("POST", "/v1/records/put")
            | ("POST", "/v1/records/put-batch")
            | ("POST", "/v1/records/patch")
            | ("POST", "/v1/records/delete")
            | ("POST", "/v1/admin/compact")
            | ("POST", "/v1/admin/snapshot")
            | ("POST", "/v1/admin/restore")
            | ("POST", "/v1/graphql")
            | ("POST", "/v1/traceql")
    )
}

fn strip_query(path: &str) -> &str {
    path.split_once('?').map(|(path, _)| path).unwrap_or(path)
}

fn is_retryable_error(error: &TraceDbClientError) -> bool {
    matches!(
        error,
        TraceDbClientError::Io(_) | TraceDbClientError::Timeout { .. }
    ) || matches!(error, TraceDbClientError::HttpStatus { status, .. } if *status >= 500)
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableRecordInput {
    pub id: String,
    pub fields: Map<String, Value>,
}

impl TableRecordInput {
    pub fn new(id: impl Into<String>, fields: Map<String, Value>) -> Self {
        Self {
            id: id.into(),
            fields,
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryBuilder {
    client_config: Option<TraceDbClientConfig>,
    table: String,
    tenant_id: Option<String>,
    text_field: Option<String>,
    text_query: Option<String>,
    vector_field: Option<String>,
    vector: Option<Vec<f32>>,
    scalar_eq: Map<String, Value>,
    freshness: FeatureFreshnessMode,
    limit: usize,
    cursor: Option<String>,
    explain: bool,
}

pub type TableHandle = QueryBuilder;

impl QueryBuilder {
    pub fn tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    pub fn where_eq(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.scalar_eq.insert(field.into(), value.into());
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

    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    pub fn with_explain(mut self) -> Self {
        self.explain = true;
        self
    }

    pub fn query(&self) -> Self {
        self.clone()
    }

    pub fn without_explain(mut self) -> Self {
        self.explain = false;
        self
    }

    pub fn insert(
        &self,
        id: impl Into<String>,
        fields: Map<String, Value>,
    ) -> TraceDbClientResult<EpochResponse> {
        let options = TraceDbRequestOptions::default();
        self.insert_with_options(id, fields, &options)
    }

    pub fn insert_with_options(
        &self,
        id: impl Into<String>,
        fields: Map<String, Value>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<EpochResponse> {
        let path = "/v1/records/put";
        let tenant_id = self.required_tenant_id("POST", path)?;
        let record = self.record_input(TableRecordInput::new(id, fields), &tenant_id);
        self.client("POST", path)?
            .put_typed_with_options(&record, options)
    }

    pub fn insert_batch(
        &self,
        records: Vec<TableRecordInput>,
    ) -> TraceDbClientResult<PutBatchResponse> {
        let options = TraceDbRequestOptions::default();
        self.insert_batch_with_options(records, &options)
    }

    pub fn insert_batch_with_options(
        &self,
        records: Vec<TableRecordInput>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<PutBatchResponse> {
        let path = "/v1/records/put-batch";
        let tenant_id = self.required_tenant_id("POST", path)?;
        let records = records
            .into_iter()
            .map(|record| self.record_input(record, &tenant_id))
            .collect();
        let request = RecordPutBatchRequest::new(records);
        self.client("POST", path)?
            .put_batch_typed_with_options(&request, options)
    }

    pub fn insert_rows(
        &self,
        rows: Vec<Map<String, Value>>,
    ) -> TraceDbClientResult<PutBatchResponse> {
        let options = TraceDbRequestOptions::default();
        self.insert_rows_with_id_field_and_options(rows, "id", &options)
    }

    pub fn insert_rows_with_options(
        &self,
        rows: Vec<Map<String, Value>>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<PutBatchResponse> {
        self.insert_rows_with_id_field_and_options(rows, "id", options)
    }

    pub fn insert_rows_with_id_field(
        &self,
        rows: Vec<Map<String, Value>>,
        id_field: impl Into<String>,
    ) -> TraceDbClientResult<PutBatchResponse> {
        let options = TraceDbRequestOptions::default();
        self.insert_rows_with_id_field_and_options(rows, id_field, &options)
    }

    pub fn insert_rows_with_id_field_and_options(
        &self,
        rows: Vec<Map<String, Value>>,
        id_field: impl Into<String>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<PutBatchResponse> {
        let path = "/v1/records/put-batch";
        let id_field = id_field.into();
        if id_field.is_empty() {
            return Err(TraceDbClientError::InvalidRequest {
                method: "POST".to_string(),
                path: path.to_string(),
                message: "id_field cannot be empty".to_string(),
            });
        }
        let tenant_id = self.required_tenant_id("POST", path)?;
        let records = rows
            .into_iter()
            .enumerate()
            .map(|(index, fields)| self.row_record_input(index, fields, &id_field, &tenant_id))
            .collect::<TraceDbClientResult<Vec<_>>>()?;
        let request = RecordPutBatchRequest::new(records);
        self.client("POST", path)?
            .put_batch_typed_with_options(&request, options)
    }

    pub fn patch_record(
        &self,
        id: impl Into<String>,
        fields: Map<String, Value>,
    ) -> TraceDbClientResult<EpochResponse> {
        let options = TraceDbRequestOptions::default();
        self.patch_record_with_options(id, fields, &options)
    }

    pub fn patch_record_with_options(
        &self,
        id: impl Into<String>,
        fields: Map<String, Value>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<EpochResponse> {
        let path = "/v1/records/patch";
        let request = RecordPatchRequest::new(
            self.table.clone(),
            self.required_tenant_id("POST", path)?,
            id,
            fields,
        );
        self.client("POST", path)?
            .patch_typed_with_options(&request, options)
    }

    pub fn get_record(&self, id: impl Into<String>) -> TraceDbClientResult<GetRecordResponse> {
        let path = "/v1/records/get";
        let request = RecordGetRequest::new(
            self.table.clone(),
            self.required_tenant_id("POST", path)?,
            id,
        );
        self.client("POST", path)?.get_record_typed(&request)
    }

    pub fn scan_typed(&self) -> TraceDbClientResult<RecordScanOutput> {
        let path = "/v1/records/scan";
        let request =
            RecordScanRequest::new(self.table.clone(), self.required_tenant_id("POST", path)?)
                .limit(self.limit);
        let request = if let Some(cursor) = &self.cursor {
            request.cursor(cursor.clone())
        } else {
            request
        };
        self.client("POST", path)?.scan_typed(&request)
    }

    pub fn delete_record(&self, id: impl Into<String>) -> TraceDbClientResult<DeleteResponse> {
        let options = TraceDbRequestOptions::default();
        self.delete_record_with_options(id, &options)
    }

    pub fn delete_record_with_options(
        &self,
        id: impl Into<String>,
        options: &TraceDbRequestOptions,
    ) -> TraceDbClientResult<DeleteResponse> {
        let path = "/v1/records/delete";
        let request = RecordDeleteRequest::new(
            self.table.clone(),
            self.required_tenant_id("POST", path)?,
            id,
        );
        self.client("POST", path)?
            .delete_typed_with_options(&request, options)
    }

    pub fn all(self) -> TraceDbClientResult<QueryResponse> {
        let path = "/v1/query";
        let client = self.client("POST", path)?;
        let query = self.into_hybrid_query(path)?;
        client.query_typed(&query)
    }

    pub fn explain_plan(self) -> TraceDbClientResult<HybridExplain> {
        let path = "/v1/explain";
        let client = self.client("POST", path)?;
        let query = self.into_hybrid_query(path)?;
        client.explain_typed(&query)
    }

    pub fn build(self) -> TraceQueryRequest {
        let freshness = match self.freshness {
            FeatureFreshnessMode::Strict => "Strict",
            FeatureFreshnessMode::AllowDirty => "AllowDirty",
            FeatureFreshnessMode::Lazy
            | FeatureFreshnessMode::OnRead
            | FeatureFreshnessMode::AllowStale => "Lazy",
        };
        TraceQueryRequest {
            table: self.table,
            tenant_id: self.tenant_id.unwrap_or_default(),
            text_field: self.text_field,
            text: self.text_query,
            vector_field: self.vector_field,
            vector: self.vector,
            scalar_eq: self.scalar_eq,
            top_k: self.limit,
            cursor: self.cursor,
            freshness: freshness.to_string(),
            explain: self.explain,
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
            cursor: self.cursor,
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

    fn into_hybrid_query(self, path: &str) -> TraceDbClientResult<HybridQuery> {
        let tenant_id = self.required_tenant_id("POST", path)?;
        let freshness = self.hybrid_freshness();
        Ok(HybridQuery {
            table: self.table,
            tenant_id,
            cursor: self.cursor,
            text_field: self.text_field,
            text: self.text_query,
            vector_field: self.vector_field,
            vector: self.vector,
            scalar_eq: self.scalar_eq,
            graph_seed: None,
            temporal_as_of: None,
            top_k: self.limit,
            freshness,
            explain: self.explain,
        })
    }

    fn hybrid_freshness(&self) -> FreshnessMode {
        match self.freshness {
            FeatureFreshnessMode::Strict => FreshnessMode::Strict,
            FeatureFreshnessMode::AllowDirty => FreshnessMode::AllowDirty,
            FeatureFreshnessMode::Lazy
            | FeatureFreshnessMode::OnRead
            | FeatureFreshnessMode::AllowStale => FreshnessMode::Lazy,
        }
    }

    fn client(&self, method: &str, path: &str) -> TraceDbClientResult<TraceDbClient> {
        self.client_config
            .clone()
            .map(TraceDbClient::new)
            .ok_or_else(|| TraceDbClientError::InvalidRequest {
                method: method.to_string(),
                path: path.to_string(),
                message: "table handle is not bound to a TraceDbClient".to_string(),
            })
    }

    fn required_tenant_id(&self, method: &str, path: &str) -> TraceDbClientResult<String> {
        match self.tenant_id.as_ref().filter(|tenant| !tenant.is_empty()) {
            Some(tenant_id) => Ok(tenant_id.clone()),
            None => Err(TraceDbClientError::InvalidRequest {
                method: method.to_string(),
                path: path.to_string(),
                message: "table handle execution requires tenant(...)".to_string(),
            }),
        }
    }

    fn record_input(&self, record: TableRecordInput, tenant_id: &str) -> RecordInput {
        let mut fields = record.fields;
        fields
            .entry("id".to_string())
            .or_insert_with(|| Value::String(record.id.clone()));
        fields
            .entry("tenant".to_string())
            .or_insert_with(|| Value::String(tenant_id.to_string()));
        RecordInput {
            table: self.table.clone(),
            id: record.id,
            tenant_id: tenant_id.to_string(),
            fields,
        }
    }

    fn row_record_input(
        &self,
        index: usize,
        fields: Map<String, Value>,
        id_field: &str,
        tenant_id: &str,
    ) -> TraceDbClientResult<RecordInput> {
        let id = fields
            .get(id_field)
            .ok_or_else(|| TraceDbClientError::InvalidRequest {
                method: "POST".to_string(),
                path: "/v1/records/put-batch".to_string(),
                message: format!("row {index} missing id field '{id_field}'"),
            })?;
        let id = match id {
            Value::String(id) => id.clone(),
            value => value.to_string(),
        };
        Ok(self.record_input(TableRecordInput::new(id, fields), tenant_id))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TraceQueryRequest {
    pub table: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub text_field: Option<String>,
    pub text: Option<String>,
    pub vector_field: Option<String>,
    pub vector: Option<Vec<f32>>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub scalar_eq: Map<String, Value>,
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
    cursor: Option<String>,
}

impl RecordScanBuilder {
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    pub fn build(self) -> TraceHttpRequest {
        let mut body = json!({
            "table": self.table,
            "tenant_id": self.tenant_id,
            "limit": self.limit,
        });
        if let Some(cursor) = self.cursor {
            body["cursor"] = json!(cursor);
        }
        TraceHttpRequest {
            path: "/v1/records/scan".to_string(),
            body,
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
