#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use tracedb_core::Epoch;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KernelInvariantReport {
    pub latest_epoch: Epoch,
    pub wal_authoritative: bool,
    pub manifest_authoritative: bool,
    pub final_visibility_guard_enabled: bool,
}

impl KernelInvariantReport {
    pub fn healthy(latest_epoch: Epoch) -> Self {
        Self {
            latest_epoch,
            wal_authoritative: true,
            manifest_authoritative: true,
            final_visibility_guard_enabled: true,
        }
    }
}
