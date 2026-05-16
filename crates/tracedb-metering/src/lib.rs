#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum MeterKind {
    Request,
    ComputeMs,
    VectorDistanceUnit,
    IndexBuildUnit,
    EmbeddingJobUnit,
    StorageByte,
    BranchDeltaByte,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UsageMeter {
    totals: BTreeMap<MeterKind, u64>,
}

impl UsageMeter {
    pub fn record(&mut self, kind: MeterKind, units: u64) {
        *self.totals.entry(kind).or_default() += units;
    }

    pub fn total(&self, kind: MeterKind) -> u64 {
        self.totals.get(&kind).copied().unwrap_or(0)
    }
}
