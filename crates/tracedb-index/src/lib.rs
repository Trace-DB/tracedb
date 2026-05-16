#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndexLifecycleState {
    Pending,
    Building,
    Ready,
    Stale,
    Deprecated,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexGeneration {
    pub index_id: String,
    pub generation: u64,
    pub kind: String,
    pub state: IndexLifecycleState,
    pub policy_aware: bool,
}

impl IndexGeneration {
    pub fn ready(index_id: impl Into<String>, kind: impl Into<String>, generation: u64) -> Self {
        Self {
            index_id: index_id.into(),
            generation,
            kind: kind.into(),
            state: IndexLifecycleState::Ready,
            policy_aware: true,
        }
    }
}
