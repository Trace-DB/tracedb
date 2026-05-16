#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActivationState {
    pub record_id: String,
    pub activation_level: f32,
    pub reinforcement_count: u64,
}

impl ActivationState {
    pub fn new(record_id: impl Into<String>) -> Self {
        Self {
            record_id: record_id.into(),
            activation_level: 0.0,
            reinforcement_count: 0,
        }
    }

    pub fn reinforce(&mut self, amount: f32) {
        self.activation_level += amount.max(0.0);
        self.reinforcement_count += 1;
    }
}
