#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracedb_modules::{
    AccessPathDescriptor, ExplainHookDescriptor, SegmentCodecDescriptor, TraceDbModule,
    TypeDescriptor, WalDecoderDescriptor,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TemporalRange {
    pub start_epoch: u64,
    pub end_epoch: Option<u64>,
}

impl TemporalRange {
    pub fn closed(start_epoch: u64, end_epoch: u64) -> Self {
        Self {
            start_epoch,
            end_epoch: Some(end_epoch),
        }
    }

    pub fn contains(&self, epoch: u64) -> bool {
        self.start_epoch <= epoch && self.end_epoch.map(|end| epoch <= end).unwrap_or(true)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TemporalIndex {
    ranges: BTreeMap<String, Vec<TemporalRange>>,
}

pub struct TemporalModule;

impl TraceDbModule for TemporalModule {
    fn module_id(&self) -> &str {
        "tracedb-temporal"
    }

    fn types(&self) -> Vec<TypeDescriptor> {
        vec![TypeDescriptor {
            type_id: "TEMPORAL_RANGE".to_string(),
        }]
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        vec![AccessPathDescriptor {
            access_path_id: "TemporalPath".to_string(),
            policy_aware: true,
        }]
    }

    fn segment_codecs(&self) -> Vec<SegmentCodecDescriptor> {
        vec![SegmentCodecDescriptor {
            codec_id: "temporal-range-v1".to_string(),
        }]
    }

    fn wal_decoders(&self) -> Vec<WalDecoderDescriptor> {
        vec![WalDecoderDescriptor {
            decoder_id: "temporal-wal-v1".to_string(),
        }]
    }

    fn explain_hooks(&self) -> Vec<ExplainHookDescriptor> {
        vec![ExplainHookDescriptor {
            hook_id: "temporal-explain-v1".to_string(),
        }]
    }
}

impl TemporalIndex {
    pub fn insert(&mut self, record_id: impl Into<String>, range: TemporalRange) {
        self.ranges.entry(record_id.into()).or_default().push(range);
    }

    pub fn as_of(&self, record_id: &str, epoch: u64) -> bool {
        self.ranges
            .get(record_id)
            .map(|ranges| ranges.iter().any(|range| range.contains(epoch)))
            .unwrap_or(false)
    }
}
