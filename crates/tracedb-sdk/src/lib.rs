#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tracedb_features::FeatureFreshnessMode;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceDbClientConfig {
    pub url: String,
    pub token: String,
}

impl TraceDbClientConfig {
    pub fn managed(url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: token.into(),
        }
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
