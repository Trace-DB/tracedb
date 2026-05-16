#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use tracedb_modules::{
    AccessPathDescriptor, ExplainHookDescriptor, SegmentCodecDescriptor, TraceDbModule,
    TypeDescriptor, WalDecoderDescriptor,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    pub source_type: String,
    pub source_id: Option<String>,
    pub source_uri: Option<String>,
    pub parent_records: Vec<String>,
    pub created_by: String,
}

impl Provenance {
    pub fn source_uri(source_uri: impl Into<String>, created_by: impl Into<String>) -> Self {
        Self {
            source_type: "uri".to_string(),
            source_id: None,
            source_uri: Some(source_uri.into()),
            parent_records: Vec::new(),
            created_by: created_by.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetrievalAudit {
    pub query_id: String,
    pub read_epoch: u64,
    pub access_paths: Vec<String>,
    pub candidate_ids: Vec<String>,
    pub returned_ids: Vec<String>,
    pub suppressed_ids: Vec<String>,
}

impl RetrievalAudit {
    pub fn new(query_id: impl Into<String>, read_epoch: u64) -> Self {
        Self {
            query_id: query_id.into(),
            read_epoch,
            access_paths: Vec::new(),
            candidate_ids: Vec::new(),
            returned_ids: Vec::new(),
            suppressed_ids: Vec::new(),
        }
    }

    pub fn with_returned(mut self, returned_ids: Vec<String>) -> Self {
        self.returned_ids = returned_ids;
        self
    }
}

pub struct ProvenanceModule;

impl TraceDbModule for ProvenanceModule {
    fn module_id(&self) -> &str {
        "tracedb-provenance"
    }

    fn types(&self) -> Vec<TypeDescriptor> {
        vec![TypeDescriptor {
            type_id: "PROVENANCE".to_string(),
        }]
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        vec![AccessPathDescriptor {
            access_path_id: "ProvenancePath".to_string(),
            policy_aware: true,
        }]
    }

    fn segment_codecs(&self) -> Vec<SegmentCodecDescriptor> {
        vec![SegmentCodecDescriptor {
            codec_id: "provenance-block-v1".to_string(),
        }]
    }

    fn wal_decoders(&self) -> Vec<WalDecoderDescriptor> {
        vec![WalDecoderDescriptor {
            decoder_id: "provenance-wal-v1".to_string(),
        }]
    }

    fn explain_hooks(&self) -> Vec<ExplainHookDescriptor> {
        vec![ExplainHookDescriptor {
            hook_id: "provenance-explain-v1".to_string(),
        }]
    }
}
