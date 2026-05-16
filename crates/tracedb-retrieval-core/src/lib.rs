#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracedb_modules::{
    AccessPathDescriptor, ExplainHookDescriptor, SegmentCodecDescriptor, TraceDbModule,
    TypeDescriptor, WalDecoderDescriptor,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetrievalMode {
    Observe,
    Suppress,
    Recall,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetrievalEvent {
    pub record_id: String,
    pub mode: RetrievalMode,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RetrievalOverlay {
    events: Vec<RetrievalEvent>,
    suppression: BTreeMap<String, f32>,
}

impl RetrievalOverlay {
    pub fn record(
        &mut self,
        record_id: impl Into<String>,
        mode: RetrievalMode,
        reason: impl Into<String>,
    ) {
        let record_id = record_id.into();
        if mode == RetrievalMode::Suppress {
            self.suppression.insert(record_id.clone(), 1.0);
        }
        self.events.push(RetrievalEvent {
            record_id,
            mode,
            reason: reason.into(),
        });
    }

    pub fn suppression_penalty(&self, record_id: &str) -> f32 {
        self.suppression.get(record_id).copied().unwrap_or(0.0)
    }
}

pub struct RetrievalCoreModule;

impl TraceDbModule for RetrievalCoreModule {
    fn module_id(&self) -> &str {
        "tracedb-retrieval-core"
    }

    fn types(&self) -> Vec<TypeDescriptor> {
        vec![TypeDescriptor {
            type_id: "SUPPRESSION_STATE".to_string(),
        }]
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        vec![AccessPathDescriptor {
            access_path_id: "RetrievalOverlayPath".to_string(),
            policy_aware: true,
        }]
    }

    fn segment_codecs(&self) -> Vec<SegmentCodecDescriptor> {
        vec![SegmentCodecDescriptor {
            codec_id: "retrieval-overlay-v1".to_string(),
        }]
    }

    fn wal_decoders(&self) -> Vec<WalDecoderDescriptor> {
        vec![WalDecoderDescriptor {
            decoder_id: "retrieval-wal-v1".to_string(),
        }]
    }

    fn explain_hooks(&self) -> Vec<ExplainHookDescriptor> {
        vec![ExplainHookDescriptor {
            hook_id: "retrieval-explain-v1".to_string(),
        }]
    }
}
