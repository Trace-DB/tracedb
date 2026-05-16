#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use tracedb_modules::{
    AccessPathDescriptor, ExplainHookDescriptor, SegmentCodecDescriptor, TraceDbModule,
    TypeDescriptor, WalDecoderDescriptor,
};
use tracedb_policy::{ActorContext, Policy, VisibilityOracle};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub edge_id: String,
    pub from: String,
    pub to: String,
    pub edge_type: String,
    pub weight: f32,
    pub policy: Policy,
}

impl Edge {
    pub fn new(
        edge_id: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        edge_type: impl Into<String>,
        weight: f32,
        policy: Policy,
    ) -> Self {
        Self {
            edge_id: edge_id.into(),
            from: from.into(),
            to: to.into(),
            edge_type: edge_type.into(),
            weight,
            policy,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GraphStore {
    edges: Vec<Edge>,
}

pub struct GraphModule;

impl TraceDbModule for GraphModule {
    fn module_id(&self) -> &str {
        "tracedb-graph"
    }

    fn types(&self) -> Vec<TypeDescriptor> {
        vec![TypeDescriptor {
            type_id: "EDGE".to_string(),
        }]
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        vec![AccessPathDescriptor {
            access_path_id: "GraphPath".to_string(),
            policy_aware: true,
        }]
    }

    fn segment_codecs(&self) -> Vec<SegmentCodecDescriptor> {
        vec![SegmentCodecDescriptor {
            codec_id: "graph-adjacency-v1".to_string(),
        }]
    }

    fn wal_decoders(&self) -> Vec<WalDecoderDescriptor> {
        vec![WalDecoderDescriptor {
            decoder_id: "graph-wal-v1".to_string(),
        }]
    }

    fn explain_hooks(&self) -> Vec<ExplainHookDescriptor> {
        vec![ExplainHookDescriptor {
            hook_id: "graph-explain-v1".to_string(),
        }]
    }
}

impl GraphStore {
    pub fn add_edge(&mut self, edge: Edge) {
        self.edges.push(edge);
    }

    pub fn visible_neighbors(
        &self,
        from: &str,
        actor: &ActorContext,
        oracle: &VisibilityOracle,
    ) -> Vec<String> {
        let mut out = self
            .edges
            .iter()
            .filter(|edge| edge.from == from)
            .filter(|edge| {
                oracle
                    .visible(&edge.edge_id, 1, &edge.policy, actor)
                    .allowed
            })
            .map(|edge| edge.to.clone())
            .collect::<Vec<_>>();
        out.sort();
        out
    }
}
