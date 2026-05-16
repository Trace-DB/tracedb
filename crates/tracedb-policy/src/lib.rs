#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use tracedb_modules::{
    AccessPathDescriptor, ExplainHookDescriptor, SegmentCodecDescriptor, TraceDbModule,
    TypeDescriptor, WalDecoderDescriptor,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum VisibilityMode {
    Tenant,
    Public,
    Acl,
    Hidden,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AclEntry {
    pub user_id: String,
    pub allow: bool,
}

impl AclEntry {
    pub fn allow_user(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            allow: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub retain_until_epoch: Option<u64>,
    pub legal_hold: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub tenant_id: String,
    pub workspace_id: Option<String>,
    pub owner_id: Option<String>,
    pub visibility: VisibilityMode,
    pub acl: Vec<AclEntry>,
    pub sensitivity: String,
    pub retention: RetentionPolicy,
    pub suppress_from_ai: bool,
    pub allow_embedding: bool,
    pub allow_training: bool,
}

impl Policy {
    pub fn tenant(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            workspace_id: None,
            owner_id: None,
            visibility: VisibilityMode::Tenant,
            acl: Vec::new(),
            sensitivity: "normal".to_string(),
            retention: RetentionPolicy {
                retain_until_epoch: None,
                legal_hold: false,
            },
            suppress_from_ai: false,
            allow_embedding: true,
            allow_training: false,
        }
    }

    pub fn with_visibility(mut self, visibility: VisibilityMode) -> Self {
        self.visibility = visibility;
        self
    }

    pub fn with_acl(mut self, acl: Vec<AclEntry>) -> Self {
        self.acl = acl;
        self
    }

    pub fn suppress_from_ai(mut self, suppress: bool) -> Self {
        self.suppress_from_ai = suppress;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorContext {
    pub tenant_id: String,
    pub user_id: String,
    pub audit: bool,
}

impl ActorContext {
    pub fn tenant_user(tenant_id: impl Into<String>, user_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            audit: false,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct VisibilityDecision {
    pub allowed: bool,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct VisibilityOracle;

impl VisibilityOracle {
    pub fn visible(
        &self,
        _record_id: &str,
        _version_id: u64,
        policy: &Policy,
        actor: &ActorContext,
    ) -> VisibilityDecision {
        let mut reasons = Vec::new();
        if policy.tenant_id != actor.tenant_id {
            reasons.push("tenant_mismatch".to_string());
        }
        match policy.visibility {
            VisibilityMode::Public | VisibilityMode::Tenant => {}
            VisibilityMode::Hidden => reasons.push("hidden".to_string()),
            VisibilityMode::Acl => {
                let allowed = policy
                    .acl
                    .iter()
                    .any(|entry| entry.allow && entry.user_id == actor.user_id);
                if !allowed {
                    reasons.push("acl_denied".to_string());
                }
            }
        }
        if policy.suppress_from_ai {
            reasons.push("suppress_from_ai".to_string());
        }
        VisibilityDecision {
            allowed: reasons.is_empty(),
            reasons,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyPlan {
    pub required_tenant: String,
    pub retrieval_allowed: bool,
    pub embedding_allowed: bool,
    pub delete_export_scope: String,
}

pub struct PolicyModule;

impl TraceDbModule for PolicyModule {
    fn module_id(&self) -> &str {
        "tracedb-policy"
    }

    fn types(&self) -> Vec<TypeDescriptor> {
        vec![TypeDescriptor {
            type_id: "POLICY".to_string(),
        }]
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        vec![AccessPathDescriptor {
            access_path_id: "PolicyMaskPath".to_string(),
            policy_aware: true,
        }]
    }

    fn segment_codecs(&self) -> Vec<SegmentCodecDescriptor> {
        vec![SegmentCodecDescriptor {
            codec_id: "policy-bitmap-v1".to_string(),
        }]
    }

    fn wal_decoders(&self) -> Vec<WalDecoderDescriptor> {
        vec![WalDecoderDescriptor {
            decoder_id: "policy-wal-v1".to_string(),
        }]
    }

    fn explain_hooks(&self) -> Vec<ExplainHookDescriptor> {
        vec![ExplainHookDescriptor {
            hook_id: "policy-explain-v1".to_string(),
        }]
    }
}
