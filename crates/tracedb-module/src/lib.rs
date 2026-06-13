#![forbid(unsafe_code)]

pub use tracedb_modules::*;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ModuleCapabilityLevel {
    Function,
    Type,
    Index,
    Planner,
    Storage,
    RuntimeJob,
    Bridge,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ModuleTrustLevel {
    CoreSigned,
    FirstPartySigned,
    ThirdPartySandboxed,
    LocalDevUnsafe,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModuleDescriptor {
    pub module_id: String,
    pub version: String,
    pub capability_level: ModuleCapabilityLevel,
    pub trust_level: ModuleTrustLevel,
    pub provided_types: Vec<String>,
    pub provided_jobs: Vec<String>,
    pub storage_codecs: Vec<String>,
    pub planner_rules: Vec<String>,
}

impl ModuleDescriptor {
    pub fn first_party(
        module_id: impl Into<String>,
        capability_level: ModuleCapabilityLevel,
    ) -> Self {
        Self {
            module_id: module_id.into(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            capability_level,
            trust_level: ModuleTrustLevel::FirstPartySigned,
            provided_types: Vec::new(),
            provided_jobs: Vec::new(),
            storage_codecs: Vec::new(),
            planner_rules: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModuleConformanceReport {
    pub module_id: String,
    pub accepted: bool,
    pub reasons: Vec<String>,
}

pub fn certify_module_descriptor(descriptor: &ModuleDescriptor) -> ModuleConformanceReport {
    let mut reasons = Vec::new();
    if descriptor.module_id.trim().is_empty() {
        reasons.push("module_id_empty".to_string());
    }
    if descriptor.trust_level == ModuleTrustLevel::LocalDevUnsafe {
        reasons.push("local_dev_unsafe_requires_explicit_opt_in".to_string());
    }
    ModuleConformanceReport {
        module_id: descriptor.module_id.clone(),
        accepted: reasons.is_empty(),
        reasons,
    }
}
