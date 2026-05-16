#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use tracedb_core::{Result, TraceDbError};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TypeDescriptor {
    pub type_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperatorDescriptor {
    pub operator_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccessPathDescriptor {
    pub access_path_id: String,
    pub policy_aware: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SegmentCodecDescriptor {
    pub codec_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalDecoderDescriptor {
    pub decoder_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExplainHookDescriptor {
    pub hook_id: String,
}

pub trait TraceDbModule {
    fn module_id(&self) -> &str;

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn types(&self) -> Vec<TypeDescriptor> {
        Vec::new()
    }

    fn operators(&self) -> Vec<OperatorDescriptor> {
        Vec::new()
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        Vec::new()
    }

    fn segment_codecs(&self) -> Vec<SegmentCodecDescriptor> {
        Vec::new()
    }

    fn wal_decoders(&self) -> Vec<WalDecoderDescriptor> {
        Vec::new()
    }

    fn explain_hooks(&self) -> Vec<ExplainHookDescriptor> {
        Vec::new()
    }

    fn has_hidden_durable_state(&self) -> bool {
        false
    }
}

#[derive(Default)]
pub struct ModuleRegistry {
    modules: Vec<RegisteredModule>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegisteredModule {
    pub module_id: String,
    pub version: String,
    pub types: Vec<TypeDescriptor>,
    pub operators: Vec<OperatorDescriptor>,
    pub access_paths: Vec<AccessPathDescriptor>,
    pub segment_codecs: Vec<SegmentCodecDescriptor>,
    pub wal_decoders: Vec<WalDecoderDescriptor>,
    pub explain_hooks: Vec<ExplainHookDescriptor>,
}

impl ModuleRegistry {
    pub fn register(&mut self, module: Box<dyn TraceDbModule>) -> Result<()> {
        let access_paths = module.access_paths();
        let explain_hooks = module.explain_hooks();
        let segment_codecs = module.segment_codecs();
        let wal_decoders = module.wal_decoders();
        if module.has_hidden_durable_state() {
            return Err(TraceDbError::ModuleRejected {
                module: module.module_id().to_string(),
                reason: "hidden durable state is not allowed".to_string(),
            });
        }
        if !access_paths.is_empty() && explain_hooks.is_empty() {
            return Err(TraceDbError::ModuleRejected {
                module: module.module_id().to_string(),
                reason: "ranked/queryable modules must provide explain hooks".to_string(),
            });
        }
        if !segment_codecs.is_empty() && wal_decoders.is_empty() {
            return Err(TraceDbError::ModuleRejected {
                module: module.module_id().to_string(),
                reason: "durable storage modules must provide WAL decoders".to_string(),
            });
        }
        let module_id = module.module_id().to_string();
        if !self
            .modules
            .iter()
            .any(|registered| registered.module_id == module_id)
        {
            self.modules.push(RegisteredModule {
                module_id,
                version: module.version().to_string(),
                types: module.types(),
                operators: module.operators(),
                access_paths,
                segment_codecs,
                wal_decoders,
                explain_hooks,
            });
        }
        Ok(())
    }

    pub fn modules(&self) -> &[RegisteredModule] {
        &self.modules
    }

    pub fn module_ids(&self) -> Vec<String> {
        self.modules
            .iter()
            .map(|module| module.module_id.clone())
            .collect()
    }
}

pub fn builtin_module_ids() -> Vec<String> {
    [
        "tracedb-text",
        "tracedb-vector",
        "tracedb-graph",
        "tracedb-temporal",
        "tracedb-policy",
        "tracedb-provenance",
        "tracedb-features",
        "tracedb-retrieval-core",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}
