#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use tracedb_core::{FeatureStatus, Result};
use tracedb_modules::{
    AccessPathDescriptor, ExplainHookDescriptor, SegmentCodecDescriptor, TraceDbModule,
    TypeDescriptor, WalDecoderDescriptor,
};

pub struct VectorModule;

impl TraceDbModule for VectorModule {
    fn module_id(&self) -> &str {
        "tracedb-vector"
    }

    fn types(&self) -> Vec<TypeDescriptor> {
        vec![TypeDescriptor {
            type_id: "VECTOR<F32,N,COSINE>".to_string(),
        }]
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        vec![AccessPathDescriptor {
            access_path_id: "VectorPath".to_string(),
            policy_aware: true,
        }]
    }

    fn explain_hooks(&self) -> Vec<ExplainHookDescriptor> {
        vec![
            ExplainHookDescriptor {
                hook_id: "vector".to_string(),
            },
            ExplainHookDescriptor {
                hook_id: "freshness".to_string(),
            },
        ]
    }

    fn segment_codecs(&self) -> Vec<SegmentCodecDescriptor> {
        vec![SegmentCodecDescriptor {
            codec_id: "vector-pages-v1".to_string(),
        }]
    }

    fn wal_decoders(&self) -> Vec<WalDecoderDescriptor> {
        vec![WalDecoderDescriptor {
            decoder_id: "vector-wal-v1".to_string(),
        }]
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorEntry {
    pub record_id: String,
    pub values: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorPage {
    pub vector_column: String,
    pub vectors: Vec<VectorEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorWalEvent {
    pub table: String,
    pub record_id: String,
    pub vector_column: String,
    pub dimensions: usize,
    pub status: FeatureStatus,
}

pub fn encode_vector_page(page: &VectorPage) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(page)?)
}

pub fn decode_vector_page(bytes: &[u8]) -> Result<VectorPage> {
    Ok(serde_json::from_slice(bytes)?)
}

pub fn encode_vector_wal_event(event: &VectorWalEvent) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(event)?)
}

pub fn decode_vector_wal_event(bytes: &[u8]) -> Result<VectorWalEvent> {
    Ok(serde_json::from_slice(bytes)?)
}

pub fn cosine_similarity(query: &[f32], vector: &[f32]) -> Option<f32> {
    if query.len() != vector.len() || query.is_empty() {
        return None;
    }
    let dot = query
        .iter()
        .zip(vector.iter())
        .map(|(left, right)| left * right)
        .sum::<f32>();
    let left_norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
    let right_norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        return None;
    }
    Some(dot / (left_norm * right_norm))
}
