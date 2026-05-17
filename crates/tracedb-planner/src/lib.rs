#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracedb_core::Epoch;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Predicates {
    pub table: String,
    pub tenant_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    pub visible_records: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CostEstimate {
    pub startup_cost: f32,
    pub per_candidate_cost: f32,
    pub expected_candidates: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryFragment {
    pub text: Option<String>,
    pub vector_dimensions: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PlannerStrategy {
    RelationalFirst,
    TextFirst,
    VectorFirst,
    GraphFirst,
    TemporalFirst,
    PolicyPartitionFirst,
    FeatureStateFirst,
    HybridRecall,
    ExactHotPlusAnnWarm,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceQuery {
    pub target_table: String,
    pub tenant_id: String,
    pub text_terms: Vec<String>,
    pub vector_dimensions: Option<usize>,
    pub graph_seeds: Vec<String>,
    pub temporal_as_of: Option<u64>,
    pub limit: usize,
    pub explain: bool,
}

impl TraceQuery {
    pub fn hybrid(
        target_table: impl Into<String>,
        tenant_id: impl Into<String>,
        text: Option<&str>,
        vector_dimensions: Option<usize>,
        limit: usize,
    ) -> Self {
        Self {
            target_table: target_table.into(),
            tenant_id: tenant_id.into(),
            text_terms: text
                .map(|value| value.split_whitespace().map(str::to_string).collect())
                .unwrap_or_default(),
            vector_dimensions,
            graph_seeds: Vec::new(),
            temporal_as_of: None,
            limit,
            explain: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhysicalPlan {
    pub strategy: PlannerStrategy,
    pub opened_paths: Vec<String>,
    pub skipped_paths: Vec<SkippedAccessPath>,
    pub exact_fallback: bool,
    pub confidence_target: Option<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SkippedAccessPath {
    pub access_path_id: String,
    pub reason: String,
}

pub fn plan_trace_query(query: &TraceQuery, visible_records: usize) -> PhysicalPlan {
    let exact_fallback = query.vector_dimensions.is_some() && visible_records <= 32;
    let strategy = if exact_fallback {
        PlannerStrategy::PolicyPartitionFirst
    } else if query.vector_dimensions.is_some() && !query.text_terms.is_empty() {
        PlannerStrategy::HybridRecall
    } else if query.vector_dimensions.is_some() {
        PlannerStrategy::VectorFirst
    } else if !query.text_terms.is_empty() {
        PlannerStrategy::TextFirst
    } else {
        PlannerStrategy::RelationalFirst
    };
    let mut opened_paths = vec![
        "PolicyPath".to_string(),
        "RelationalPath".to_string(),
        "HotOverlayPath".to_string(),
    ];
    if !query.text_terms.is_empty() {
        opened_paths.push("LexicalPath".to_string());
    }
    if query.vector_dimensions.is_some() {
        opened_paths.push("VectorPath".to_string());
    }
    if !query.graph_seeds.is_empty() {
        opened_paths.push("GraphPath".to_string());
    }
    if query.temporal_as_of.is_some() {
        opened_paths.push("TemporalPath".to_string());
    }
    let skipped_paths = ["GraphPath", "TemporalPath", "ModulePath"]
        .into_iter()
        .filter(|path| !opened_paths.iter().any(|opened| opened == path))
        .map(|path| SkippedAccessPath {
            access_path_id: path.to_string(),
            reason: "query did not request this evidence stream".to_string(),
        })
        .collect();
    PhysicalPlan {
        strategy,
        opened_paths,
        skipped_paths,
        exact_fallback,
        confidence_target: Some(95),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CandidateBudget {
    pub max_candidates: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkBudget {
    pub max_work_units: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlannerFeedback {
    pub accepted_candidates: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccessPathDescriptor {
    pub access_path_id: String,
    pub module_id: Option<String>,
    pub policy_aware: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccessPathExplain {
    pub access_path_id: String,
    pub opened: bool,
    pub visibility_checked_before_open: bool,
    pub candidates: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FeatureFreshness {
    Ready,
    Dirty,
    Pending,
    Failed,
    Missing,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub record_id: String,
    pub version_id: u64,
    pub score_components: ScoreComponents,
    pub score_upper_bound: Option<f32>,
    pub source: String,
    pub freshness: FeatureFreshness,
    pub visibility_checked: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CandidateBatch {
    pub candidates: Vec<Candidate>,
}

pub trait AccessPath {
    fn descriptor(&self) -> AccessPathDescriptor;
    fn estimate(&self, predicates: &Predicates, ctx: &Stats) -> CostEstimate;
    fn open(
        &self,
        query: QueryFragment,
        visibility: &[String],
        budget: CandidateBudget,
    ) -> CandidateBatch;
    fn next_batch(&mut self, budget: WorkBudget) -> CandidateBatch;
    fn refine(&mut self, feedback: PlannerFeedback);
    fn explain(&self) -> AccessPathExplain;
}

pub fn minimum_access_path_descriptors() -> Vec<AccessPathDescriptor> {
    vec![
        AccessPathDescriptor {
            access_path_id: "PolicyPath".to_string(),
            module_id: None,
            policy_aware: true,
        },
        AccessPathDescriptor {
            access_path_id: "RelationalPath".to_string(),
            module_id: None,
            policy_aware: true,
        },
        AccessPathDescriptor {
            access_path_id: "HotOverlayPath".to_string(),
            module_id: None,
            policy_aware: true,
        },
        AccessPathDescriptor {
            access_path_id: "LexicalPath".to_string(),
            module_id: Some("tracedb-text".to_string()),
            policy_aware: true,
        },
        AccessPathDescriptor {
            access_path_id: "VectorPath".to_string(),
            module_id: Some("tracedb-vector".to_string()),
            policy_aware: true,
        },
    ]
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScoreComponents {
    pub vector: Option<f32>,
    pub lexical: Option<f32>,
    pub relational: Option<f32>,
    pub freshness_penalty: Option<f32>,
    pub final_score: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryRow {
    pub record_id: String,
    pub version_id: u64,
    pub tenant_id: String,
    pub fields: Map<String, Value>,
    pub score: ScoreComponents,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExplainOutput {
    pub read_epoch: Epoch,
    pub schema_epoch: Epoch,
    pub policy_epoch: Epoch,
    pub tenant_mask_visible_records: usize,
    pub opened_candidate_streams: Vec<String>,
    pub access_paths: Vec<AccessPathExplain>,
    pub planner_candidates: Vec<Candidate>,
    pub candidate_budget: usize,
    pub text_candidates: usize,
    pub vector_candidates: usize,
    pub hot_overlay_searched: bool,
    pub freshness_mode: String,
    pub dirty_feature_count: usize,
    pub pending_feature_count: usize,
    pub failed_feature_count: usize,
    pub missing_feature_count: usize,
    pub fusion_method: String,
    pub deduped_candidate_count: usize,
    pub materialized_count: usize,
    pub final_visibility_guard_count: usize,
    pub final_visibility_guard_removed: usize,
    pub returned_count: usize,
    pub segments_scanned: usize,
    pub module_versions: Vec<String>,
    pub selected_strategy: Option<String>,
    pub skipped_access_paths: Vec<String>,
    pub exact_fallback_triggered: bool,
    pub early_stop_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryOutput {
    pub results: Vec<QueryRow>,
    pub explain: ExplainOutput,
}
