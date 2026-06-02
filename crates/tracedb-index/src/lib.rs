#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use tracedb_core::{
    checksum_bytes, decode_artifact_envelope, decrypt_artifact_if_needed, encode_artifact_envelope,
    ArtifactEnvelopeHeader, EncryptionContext, Result, TraceDbError, ARTIFACT_ENVELOPE_MAGIC,
};

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexRecord {
    pub table: String,
    pub record_id: String,
    pub tenant_id: String,
    pub version_id: u64,
    pub fields: BTreeMap<String, Value>,
    pub text: BTreeMap<String, String>,
    pub vectors: BTreeMap<String, Vec<f32>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextIndexArtifact {
    pub index_id: String,
    pub segment_id: String,
    pub generation: u64,
    pub manifest_generation: u64,
    pub source_segment_checksum: [u8; 32],
    pub doc_count: usize,
    pub avg_len: f32,
    pub documents: Vec<TextIndexDocument>,
    pub postings: BTreeMap<String, Vec<TextIndexPosting>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextIndexDocument {
    pub record_id: String,
    pub fields: BTreeMap<String, String>,
    pub lengths: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TextIndexPosting {
    pub record_id: String,
    pub term_frequency: usize,
    pub doc_len: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextScore {
    pub record_id: String,
    pub score: f32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum VectorIndexAlgorithm {
    #[default]
    LegacyGreedy,
    HnswCosineV1,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HnswFieldGraph {
    pub entry_point: Option<String>,
    pub max_level: usize,
    pub levels: BTreeMap<usize, BTreeMap<String, Vec<String>>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorIndexArtifact {
    pub index_id: String,
    pub segment_id: String,
    pub generation: u64,
    pub manifest_generation: u64,
    pub source_segment_checksum: [u8; 32],
    /// Maximum neighbors per node in upper HNSW layers.
    pub m: usize,
    /// Build-time beam width used while linking HNSW nodes.
    pub ef_construction: usize,
    /// Default max nodes to visit during HNSW search.
    pub ef_search: usize,
    #[serde(default)]
    pub algorithm: VectorIndexAlgorithm,
    pub entries: Vec<VectorIndexEntry>,
    /// Legacy level-0 greedy graph kept for artifact compatibility and debug access.
    #[serde(default)]
    pub neighbors: BTreeMap<String, Vec<String>>,
    /// Per-vector-field deterministic HNSW graph. Edges point at stable entry keys.
    #[serde(default)]
    pub hnsw: BTreeMap<String, HnswFieldGraph>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorIndexEntry {
    pub record_id: String,
    pub version_id: u64,
    pub field: String,
    pub vector: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorScore {
    pub record_id: String,
    pub version_id: u64,
    pub score: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorSearchReport {
    pub scores: Vec<VectorScore>,
    pub field_entry_count: usize,
    pub visited_count: usize,
    pub exact_fallback_used: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BitmapIndexArtifact {
    pub index_id: String,
    pub segment_id: String,
    pub generation: u64,
    pub manifest_generation: u64,
    pub source_segment_checksum: [u8; 32],
    pub records: Vec<BitmapRecord>,
    pub tenant_records: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BitmapRecord {
    pub record_id: String,
    pub tenant_id: String,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IndexPayload {
    Primary(BitmapIndexArtifact),
    Policy(BitmapIndexArtifact),
    Text(TextIndexArtifact),
    Vector(VectorIndexArtifact),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexArtifact {
    pub index_id: String,
    pub segment_id: String,
    pub segment_generation: u64,
    pub manifest_generation: u64,
    pub kind: String,
    pub state_history: Vec<String>,
    pub policy_aware: bool,
    pub source_segment_checksum: [u8; 32],
    pub record_count: usize,
    pub payload: IndexPayload,
}

impl IndexArtifact {
    pub fn payload_checksum(&self) -> Result<[u8; 32]> {
        Ok(checksum_bytes(&serialize_index_artifact_payload(self)?))
    }

    pub fn as_text(&self) -> Option<&TextIndexArtifact> {
        match &self.payload {
            IndexPayload::Text(text) => Some(text),
            _ => None,
        }
    }

    pub fn as_vector(&self) -> Option<&VectorIndexArtifact> {
        match &self.payload {
            IndexPayload::Vector(vector) => Some(vector),
            _ => None,
        }
    }

    pub fn as_bitmap(&self) -> Option<&BitmapIndexArtifact> {
        match &self.payload {
            IndexPayload::Primary(bitmap) | IndexPayload::Policy(bitmap) => Some(bitmap),
            _ => None,
        }
    }
}

pub fn build_segment_index_artifacts(
    segment_id: &str,
    generation: u64,
    manifest_generation: u64,
    source_segment_checksum: [u8; 32],
    records: &[IndexRecord],
) -> Result<Vec<IndexArtifact>> {
    let mut artifacts = vec![
        build_bitmap_artifact(
            "primary",
            segment_id,
            generation,
            manifest_generation,
            source_segment_checksum,
            records,
        ),
        build_bitmap_artifact(
            "policy",
            segment_id,
            generation,
            manifest_generation,
            source_segment_checksum,
            records,
        ),
    ];
    if records.iter().any(|record| !record.text.is_empty()) {
        artifacts.push(IndexArtifact {
            index_id: format!("{segment_id}:text:{generation}"),
            segment_id: segment_id.to_string(),
            segment_generation: generation,
            manifest_generation,
            kind: "text".to_string(),
            state_history: ready_state_history(),
            policy_aware: true,
            source_segment_checksum,
            record_count: records.len(),
            payload: IndexPayload::Text(build_text_index(
                segment_id,
                generation,
                manifest_generation,
                source_segment_checksum,
                records,
            )?),
        });
    }
    if records.iter().any(|record| !record.vectors.is_empty()) {
        artifacts.push(IndexArtifact {
            index_id: format!("{segment_id}:vector:{generation}"),
            segment_id: segment_id.to_string(),
            segment_generation: generation,
            manifest_generation,
            kind: "vector".to_string(),
            state_history: ready_state_history(),
            policy_aware: true,
            source_segment_checksum,
            record_count: records.len(),
            payload: IndexPayload::Vector(build_vector_index(
                segment_id,
                generation,
                manifest_generation,
                source_segment_checksum,
                records,
            )?),
        });
    }
    Ok(artifacts)
}

pub fn build_text_index(
    segment_id: &str,
    generation: u64,
    manifest_generation: u64,
    source_segment_checksum: [u8; 32],
    records: &[IndexRecord],
) -> Result<TextIndexArtifact> {
    let mut documents = Vec::new();
    let mut postings = BTreeMap::<String, Vec<TextIndexPosting>>::new();
    let mut total_len = 0usize;
    for record in records {
        let mut fields = BTreeMap::new();
        let mut lengths = BTreeMap::new();
        for (field, body) in &record.text {
            let tokens = tracedb_text::tokenize(body);
            total_len += tokens.len();
            lengths.insert(field.clone(), tokens.len());
            fields.insert(field.clone(), body.clone());
            let mut tf = BTreeMap::<String, usize>::new();
            for token in tokens {
                *tf.entry(token).or_default() += 1;
            }
            for (term, term_frequency) in tf {
                postings
                    .entry(posting_key(field, &term))
                    .or_default()
                    .push(TextIndexPosting {
                        record_id: record.record_id.clone(),
                        term_frequency,
                        doc_len: *lengths.get(field).unwrap_or(&0),
                    });
            }
        }
        documents.push(TextIndexDocument {
            record_id: record.record_id.clone(),
            fields,
            lengths,
        });
    }
    let doc_count = documents.len();
    Ok(TextIndexArtifact {
        index_id: format!("{segment_id}:text:{generation}"),
        segment_id: segment_id.to_string(),
        generation,
        manifest_generation,
        source_segment_checksum,
        doc_count,
        avg_len: if doc_count == 0 {
            0.0
        } else {
            total_len as f32 / doc_count as f32
        },
        documents,
        postings,
    })
}

impl TextIndexArtifact {
    pub fn score_text(&self, query: &str, text_field: Option<&str>) -> Vec<TextScore> {
        let query_terms = tracedb_text::tokenize(query);
        if query_terms.is_empty() || self.doc_count == 0 {
            return Vec::new();
        }
        let fields = text_field
            .map(|field| vec![field.to_string()])
            .unwrap_or_else(|| {
                self.documents
                    .iter()
                    .flat_map(|doc| doc.fields.keys().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            });
        let mut tf_by_doc = BTreeMap::<String, BTreeMap<String, usize>>::new();
        let mut doc_len_by_doc = BTreeMap::<String, usize>::new();
        let mut df = BTreeMap::<String, usize>::new();
        for field in fields {
            for term in query_terms.iter().collect::<BTreeSet<_>>() {
                let key = posting_key(&field, term);
                let Some(postings) = self.postings.get(&key) else {
                    continue;
                };
                df.insert(term.to_string(), postings.len());
                for posting in postings {
                    tf_by_doc
                        .entry(posting.record_id.clone())
                        .or_default()
                        .insert(term.to_string(), posting.term_frequency);
                    doc_len_by_doc.insert(posting.record_id.clone(), posting.doc_len);
                }
            }
        }
        let mut scores = tf_by_doc
            .into_iter()
            .filter_map(|(record_id, tf)| {
                let doc_len = *doc_len_by_doc.get(&record_id).unwrap_or(&0);
                let score = bm25_from_term_frequency(
                    &query_terms,
                    &tf,
                    doc_len,
                    self.doc_count,
                    self.avg_len,
                    &df,
                );
                (score > 0.0).then_some(TextScore { record_id, score })
            })
            .collect::<Vec<_>>();
        scores.sort_by(|left, right| score_order(left.score, right.score));
        scores
    }
}

const DEFAULT_HNSW_M: usize = 16;
const DEFAULT_HNSW_EF_CONSTRUCTION: usize = 64;
const DEFAULT_HNSW_EF_SEARCH: usize = 64;
const MAX_DETERMINISTIC_HNSW_LEVEL: usize = 16;

pub fn build_vector_index(
    segment_id: &str,
    generation: u64,
    manifest_generation: u64,
    source_segment_checksum: [u8; 32],
    records: &[IndexRecord],
) -> Result<VectorIndexArtifact> {
    let mut entries = Vec::new();
    for record in records {
        for (field, vector) in &record.vectors {
            entries.push(VectorIndexEntry {
                record_id: record.record_id.clone(),
                version_id: record.version_id,
                field: field.clone(),
                vector: vector.clone(),
            });
        }
    }
    entries.sort_by(|left, right| {
        left.field
            .cmp(&right.field)
            .then_with(|| left.record_id.cmp(&right.record_id))
            .then_with(|| left.version_id.cmp(&right.version_id))
    });
    let hnsw = build_hnsw_graphs(&entries, DEFAULT_HNSW_M, DEFAULT_HNSW_EF_CONSTRUCTION);
    let neighbors = hnsw_level_zero_debug_neighbors(&hnsw, &entries);
    Ok(VectorIndexArtifact {
        index_id: format!("{segment_id}:vector:{generation}"),
        segment_id: segment_id.to_string(),
        generation,
        manifest_generation,
        source_segment_checksum,
        m: DEFAULT_HNSW_M,
        ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
        ef_search: DEFAULT_HNSW_EF_SEARCH,
        algorithm: VectorIndexAlgorithm::HnswCosineV1,
        entries,
        neighbors,
        hnsw,
    })
}

impl VectorIndexArtifact {
    /// Access the level-0 nearest-neighbor list for a specific entry.
    pub fn greedy_nn_neighbors(&self, field: &str, record_id: &str) -> Option<&Vec<String>> {
        self.neighbors.get(&vector_node_key(field, record_id))
    }

    pub fn search_vector(&self, field: &str, query: &[f32], limit: usize) -> Vec<VectorScore> {
        self.search_vector_with_report(field, query, limit).scores
    }

    pub fn search_vector_with_report(
        &self,
        field: &str,
        query: &[f32],
        limit: usize,
    ) -> VectorSearchReport {
        self.search_vector_internal(field, query, limit, None)
    }

    pub fn search_vector_with_report_filtered(
        &self,
        field: &str,
        query: &[f32],
        limit: usize,
        allowed: &BTreeSet<(String, u64)>,
    ) -> VectorSearchReport {
        self.search_vector_internal(field, query, limit, Some(allowed))
    }

    fn search_vector_internal(
        &self,
        field: &str,
        query: &[f32],
        limit: usize,
        allowed: Option<&BTreeSet<(String, u64)>>,
    ) -> VectorSearchReport {
        if limit == 0 {
            return VectorSearchReport {
                scores: Vec::new(),
                field_entry_count: 0,
                visited_count: 0,
                exact_fallback_used: false,
            };
        }

        let entries = field_entries_by_key(&self.entries, field);
        if entries.is_empty() {
            return VectorSearchReport {
                scores: Vec::new(),
                field_entry_count: 0,
                visited_count: 0,
                exact_fallback_used: false,
            };
        }

        match self.algorithm {
            VectorIndexAlgorithm::HnswCosineV1 => {
                self.search_hnsw(field, query, limit, allowed, entries)
            }
            VectorIndexAlgorithm::LegacyGreedy => {
                self.search_legacy_greedy(field, query, limit, allowed, entries)
            }
        }
    }

    fn search_hnsw(
        &self,
        field: &str,
        query: &[f32],
        limit: usize,
        allowed: Option<&BTreeSet<(String, u64)>>,
        entries: BTreeMap<String, &VectorIndexEntry>,
    ) -> VectorSearchReport {
        let field_entry_count = entries.len();
        let Some(graph) = self.hnsw.get(field) else {
            return exact_vector_search(&entries, query, limit, allowed, true, 0);
        };
        let Some(mut entry_point) = graph.entry_point.clone() else {
            return exact_vector_search(&entries, query, limit, allowed, true, 0);
        };
        if !entries.contains_key(&entry_point) {
            return exact_vector_search(&entries, query, limit, allowed, true, 0);
        }

        let mut visited = BTreeSet::new();
        for level in (1..=graph.max_level).rev() {
            let candidates = hnsw_search_layer(
                graph,
                &entries,
                query,
                vec![entry_point.clone()],
                level,
                1,
                &mut visited,
            );
            if let Some(best) = candidates.first() {
                entry_point = best.clone();
            }
        }

        let allowed_count = allowed
            .map(|allowed| allowed.len())
            .unwrap_or(field_entry_count);
        let ef_search = self
            .ef_search
            .max(limit.saturating_mul(4))
            .max(
                allowed_count
                    .min(field_entry_count)
                    .min(DEFAULT_HNSW_EF_SEARCH),
            )
            .min(field_entry_count)
            .max(1);
        let mut entry_points = vec![entry_point];
        if let Some(first_key) = entries.keys().next() {
            if !entry_points.iter().any(|key| key == first_key) {
                entry_points.push(first_key.clone());
            }
        }
        let candidates = hnsw_search_layer(
            graph,
            &entries,
            query,
            entry_points,
            0,
            ef_search,
            &mut visited,
        );
        let mut scores = vector_scores_for_keys(&candidates, &entries, query, allowed);
        scores.sort_by(vector_score_order);
        scores.truncate(limit);

        let mut exact_fallback_used = false;
        if scores.len() < limit && allowed_count > scores.len() && visited.len() < field_entry_count
        {
            exact_fallback_used = true;
            let mut exact_scores = exact_scores_for_entries(&entries, query, allowed);
            exact_scores.sort_by(vector_score_order);
            exact_scores.truncate(limit);
            scores = exact_scores;
        }

        VectorSearchReport {
            scores,
            field_entry_count,
            visited_count: visited.len(),
            exact_fallback_used,
        }
    }

    fn search_legacy_greedy(
        &self,
        field: &str,
        query: &[f32],
        limit: usize,
        allowed: Option<&BTreeSet<(String, u64)>>,
        entries: BTreeMap<String, &VectorIndexEntry>,
    ) -> VectorSearchReport {
        let field_entry_count = entries.len();
        let entry_point = entries.keys().next().expect("non-empty entries").clone();
        let target_visits = self.ef_search.max(limit).min(field_entry_count).max(1);
        let mut scores_by_key = BTreeMap::new();
        let mut visited = BTreeSet::new();
        let mut frontier = BTreeSet::from([entry_point]);

        while visited.len() < target_visits && !frontier.is_empty() {
            let Some(node_key) = best_entry_key(&frontier, &entries, query) else {
                break;
            };
            frontier.remove(&node_key);
            if !visited.insert(node_key.clone()) {
                continue;
            }
            if let Some(score) = vector_score_for_key(&node_key, &entries, query) {
                scores_by_key.insert(node_key.clone(), score);
            }

            let Some(entry) = entries.get(&node_key) else {
                continue;
            };
            for neighbor in self
                .neighbors
                .get(&vector_node_key(field, &entry.record_id))
                .into_iter()
                .flat_map(|neighbors| neighbors.iter())
                .filter_map(|record_id| legacy_entry_key_for_record_id(&entries, record_id))
            {
                if !visited.contains(&neighbor) {
                    frontier.insert(neighbor);
                }
            }
        }

        let mut scores = visited
            .iter()
            .filter_map(|key| scores_by_key.get(key).cloned())
            .filter(|score| score_allowed(score, allowed))
            .collect::<Vec<_>>();
        let mut exact_fallback_used = false;
        if visited.len() < field_entry_count || scores.len() < limit {
            exact_fallback_used = true;
            scores = exact_scores_for_entries(&entries, query, allowed);
        }
        scores.sort_by(vector_score_order);
        scores.truncate(limit);
        VectorSearchReport {
            scores,
            field_entry_count,
            visited_count: visited.len(),
            exact_fallback_used,
        }
    }
}

fn build_hnsw_graphs(
    entries: &[VectorIndexEntry],
    m: usize,
    ef_construction: usize,
) -> BTreeMap<String, HnswFieldGraph> {
    let mut grouped = BTreeMap::<String, Vec<&VectorIndexEntry>>::new();
    for entry in entries {
        grouped.entry(entry.field.clone()).or_default().push(entry);
    }

    grouped
        .into_iter()
        .map(|(field, mut field_entries)| {
            field_entries.sort_by(|left, right| {
                left.record_id
                    .cmp(&right.record_id)
                    .then_with(|| left.version_id.cmp(&right.version_id))
            });
            let entries_by_key = field_entries
                .iter()
                .map(|entry| (vector_entry_key(entry), *entry))
                .collect::<BTreeMap<_, _>>();
            let mut graph = HnswFieldGraph::default();

            for entry in field_entries {
                let key = vector_entry_key(entry);
                let node_level = deterministic_hnsw_level(entry, m);
                if graph.entry_point.is_none() {
                    graph.entry_point = Some(key.clone());
                    graph.max_level = node_level;
                    for level in 0..=node_level {
                        graph
                            .levels
                            .entry(level)
                            .or_default()
                            .entry(key.clone())
                            .or_default();
                    }
                    continue;
                }

                let previous_max_level = graph.max_level;
                let mut entry_point = graph.entry_point.clone().expect("entry point exists");
                let mut ignored_visited = BTreeSet::new();
                if previous_max_level > node_level {
                    for level in ((node_level + 1)..=previous_max_level).rev() {
                        let candidates = hnsw_search_layer(
                            &graph,
                            &entries_by_key,
                            &entry.vector,
                            vec![entry_point.clone()],
                            level,
                            1,
                            &mut ignored_visited,
                        );
                        if let Some(best) = candidates.first() {
                            entry_point = best.clone();
                        }
                    }
                }

                let max_link_level = previous_max_level.min(node_level);
                for level in (0..=max_link_level).rev() {
                    let candidates = hnsw_search_layer(
                        &graph,
                        &entries_by_key,
                        &entry.vector,
                        vec![entry_point.clone()],
                        level,
                        ef_construction.max(1),
                        &mut ignored_visited,
                    );
                    let selected = select_hnsw_neighbors(
                        &key,
                        &candidates,
                        &entries_by_key,
                        m_for_level(m, level),
                    );
                    graph
                        .levels
                        .entry(level)
                        .or_default()
                        .entry(key.clone())
                        .or_default();
                    for neighbor in selected {
                        link_hnsw_neighbors(
                            &mut graph,
                            level,
                            &key,
                            &neighbor,
                            &entries_by_key,
                            m_for_level(m, level),
                        );
                    }
                    if let Some(best) = hnsw_best_key(
                        graph
                            .levels
                            .get(&level)
                            .and_then(|level_links| level_links.get(&key))
                            .into_iter()
                            .flat_map(|neighbors| neighbors.iter()),
                        &entries_by_key,
                        &entry.vector,
                    ) {
                        entry_point = best;
                    }
                }

                if node_level > previous_max_level {
                    for level in (previous_max_level + 1)..=node_level {
                        graph
                            .levels
                            .entry(level)
                            .or_default()
                            .entry(key.clone())
                            .or_default();
                    }
                    graph.entry_point = Some(key.clone());
                    graph.max_level = node_level;
                }
            }

            (field, graph)
        })
        .collect()
}

fn hnsw_search_layer(
    graph: &HnswFieldGraph,
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
    entry_points: Vec<String>,
    level: usize,
    ef: usize,
    visited_accumulator: &mut BTreeSet<String>,
) -> Vec<String> {
    let mut visited = BTreeSet::new();
    let mut frontier = entry_points
        .into_iter()
        .filter(|key| entries.contains_key(key))
        .collect::<BTreeSet<_>>();
    let max_visits = ef.max(1).min(entries.len()).max(1);

    while visited.len() < max_visits && !frontier.is_empty() {
        let Some(key) = best_entry_key(&frontier, entries, query) else {
            break;
        };
        frontier.remove(&key);
        if !visited.insert(key.clone()) {
            continue;
        }
        visited_accumulator.insert(key.clone());
        for neighbor in graph
            .levels
            .get(&level)
            .and_then(|level_links| level_links.get(&key))
            .into_iter()
            .flat_map(|neighbors| neighbors.iter())
            .filter(|neighbor| entries.contains_key(*neighbor))
        {
            if !visited.contains(neighbor) {
                frontier.insert(neighbor.clone());
            }
        }
    }

    let mut out = visited.into_iter().collect::<Vec<_>>();
    out.sort_by(|left, right| vector_key_order(left, right, entries, query));
    out.truncate(ef.max(1));
    out
}

fn select_hnsw_neighbors(
    key: &str,
    candidates: &[String],
    entries: &BTreeMap<String, &VectorIndexEntry>,
    m: usize,
) -> Vec<String> {
    let Some(entry) = entries.get(key) else {
        return Vec::new();
    };
    let mut out = candidates
        .iter()
        .filter(|candidate| candidate.as_str() != key)
        .filter(|candidate| entries.contains_key(candidate.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    out.sort_by(|left, right| vector_key_order(left, right, entries, &entry.vector));
    out.truncate(m);
    out
}

fn link_hnsw_neighbors(
    graph: &mut HnswFieldGraph,
    level: usize,
    left: &str,
    right: &str,
    entries: &BTreeMap<String, &VectorIndexEntry>,
    m: usize,
) {
    add_hnsw_neighbor(graph, level, left, right);
    add_hnsw_neighbor(graph, level, right, left);
    prune_hnsw_neighbors(graph, level, left, entries, m);
    prune_hnsw_neighbors(graph, level, right, entries, m);
}

fn add_hnsw_neighbor(graph: &mut HnswFieldGraph, level: usize, key: &str, neighbor: &str) {
    let neighbors = graph
        .levels
        .entry(level)
        .or_default()
        .entry(key.to_string())
        .or_default();
    if !neighbors.iter().any(|existing| existing == neighbor) {
        neighbors.push(neighbor.to_string());
    }
}

fn prune_hnsw_neighbors(
    graph: &mut HnswFieldGraph,
    level: usize,
    key: &str,
    entries: &BTreeMap<String, &VectorIndexEntry>,
    m: usize,
) {
    let Some(entry) = entries.get(key) else {
        return;
    };
    let Some(neighbors) = graph
        .levels
        .get_mut(&level)
        .and_then(|level_links| level_links.get_mut(key))
    else {
        return;
    };
    neighbors.sort_by(|left, right| vector_key_order(left, right, entries, &entry.vector));
    neighbors.dedup();
    neighbors.truncate(m);
}

fn hnsw_best_key<'a>(
    keys: impl Iterator<Item = &'a String>,
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
) -> Option<String> {
    keys.filter(|key| entries.contains_key(key.as_str()))
        .min_by(|left, right| vector_key_order(left, right, entries, query))
        .cloned()
}

fn field_entries_by_key<'a>(
    entries: &'a [VectorIndexEntry],
    field: &str,
) -> BTreeMap<String, &'a VectorIndexEntry> {
    entries
        .iter()
        .filter(|entry| entry.field == field)
        .map(|entry| (vector_entry_key(entry), entry))
        .collect()
}

fn vector_scores_for_keys(
    keys: &[String],
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
    allowed: Option<&BTreeSet<(String, u64)>>,
) -> Vec<VectorScore> {
    keys.iter()
        .filter_map(|key| vector_score_for_key(key, entries, query))
        .filter(|score| score_allowed(score, allowed))
        .collect()
}

fn exact_vector_search(
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
    limit: usize,
    allowed: Option<&BTreeSet<(String, u64)>>,
    exact_fallback_used: bool,
    visited_count: usize,
) -> VectorSearchReport {
    let mut scores = exact_scores_for_entries(entries, query, allowed);
    scores.sort_by(vector_score_order);
    scores.truncate(limit);
    VectorSearchReport {
        scores,
        field_entry_count: entries.len(),
        visited_count,
        exact_fallback_used,
    }
}

fn exact_scores_for_entries(
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
    allowed: Option<&BTreeSet<(String, u64)>>,
) -> Vec<VectorScore> {
    entries
        .values()
        .filter_map(|entry| vector_score_for_entry(entry, query))
        .filter(|score| score_allowed(score, allowed))
        .collect()
}

fn score_allowed(score: &VectorScore, allowed: Option<&BTreeSet<(String, u64)>>) -> bool {
    allowed.is_none_or(|allowed| allowed.contains(&(score.record_id.clone(), score.version_id)))
}

fn best_entry_key(
    frontier: &BTreeSet<String>,
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
) -> Option<String> {
    frontier
        .iter()
        .filter(|key| entries.contains_key(key.as_str()))
        .min_by(|left, right| vector_key_order(left, right, entries, query))
        .cloned()
}

fn vector_score_for_key(
    key: &str,
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
) -> Option<VectorScore> {
    vector_score_for_entry(entries.get(key)?, query)
}

fn vector_key_order(
    left: &str,
    right: &str,
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
) -> std::cmp::Ordering {
    let left_score = entries
        .get(left)
        .and_then(|entry| tracedb_vector::cosine_similarity(query, &entry.vector))
        .unwrap_or(f32::NEG_INFINITY);
    let right_score = entries
        .get(right)
        .and_then(|entry| tracedb_vector::cosine_similarity(query, &entry.vector))
        .unwrap_or(f32::NEG_INFINITY);
    score_order(left_score, right_score).then_with(|| {
        let left_entry = entries.get(left).copied();
        let right_entry = entries.get(right).copied();
        match (left_entry, right_entry) {
            (Some(left), Some(right)) => left
                .record_id
                .cmp(&right.record_id)
                .then_with(|| left.version_id.cmp(&right.version_id)),
            _ => left.cmp(right),
        }
    })
}

fn vector_score_for_entry(entry: &VectorIndexEntry, query: &[f32]) -> Option<VectorScore> {
    tracedb_vector::cosine_similarity(query, &entry.vector).map(|score| VectorScore {
        record_id: entry.record_id.clone(),
        version_id: entry.version_id,
        score,
    })
}

fn vector_entry_key(entry: &VectorIndexEntry) -> String {
    format!(
        "{}\u{0}{}\u{0}{}",
        entry.field, entry.record_id, entry.version_id
    )
}

fn legacy_entry_key_for_record_id(
    entries: &BTreeMap<String, &VectorIndexEntry>,
    record_id: &str,
) -> Option<String> {
    entries
        .iter()
        .find_map(|(key, entry)| (entry.record_id == record_id).then(|| key.clone()))
}

fn m_for_level(m: usize, level: usize) -> usize {
    if level == 0 {
        m.saturating_mul(2).max(1)
    } else {
        m.max(1)
    }
}

fn deterministic_hnsw_level(entry: &VectorIndexEntry, m: usize) -> usize {
    let mut hash = stable_vector_hash(&format!(
        "{}\u{0}{}\u{0}{}",
        entry.field, entry.record_id, entry.version_id
    ));
    let divisor = m.max(2) as u64;
    let mut level = 0usize;
    while level < MAX_DETERMINISTIC_HNSW_LEVEL && hash % divisor == 0 {
        level += 1;
        hash = hash.rotate_right(7) ^ 0x9E37_79B9_7F4A_7C15;
    }
    level
}

fn stable_vector_hash(value: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

fn hnsw_level_zero_debug_neighbors(
    hnsw: &BTreeMap<String, HnswFieldGraph>,
    entries: &[VectorIndexEntry],
) -> BTreeMap<String, Vec<String>> {
    let entries_by_key = entries
        .iter()
        .map(|entry| (vector_entry_key(entry), entry))
        .collect::<BTreeMap<_, _>>();
    hnsw.iter()
        .flat_map(|(field, graph)| {
            graph
                .levels
                .get(&0)
                .into_iter()
                .flat_map(|level| level.iter())
                .filter_map(|(key, neighbors)| {
                    let entry = entries_by_key.get(key)?;
                    let record_ids = neighbors
                        .iter()
                        .filter_map(|neighbor| {
                            entries_by_key
                                .get(neighbor)
                                .map(|entry| entry.record_id.clone())
                        })
                        .collect::<Vec<_>>();
                    Some((vector_node_key(field, &entry.record_id), record_ids))
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

pub fn build_policy_bitmap_index(
    segment_id: &str,
    generation: u64,
    manifest_generation: u64,
    source_segment_checksum: [u8; 32],
    records: &[IndexRecord],
) -> Result<BitmapIndexArtifact> {
    Ok(build_bitmap_index(
        segment_id,
        generation,
        manifest_generation,
        source_segment_checksum,
        records,
    ))
}

impl BitmapIndexArtifact {
    pub fn visible_record_ids(
        &self,
        tenant_id: &str,
        scalar_eq: &serde_json::Map<String, Value>,
    ) -> BTreeSet<String> {
        self.records
            .iter()
            .filter(|record| record.tenant_id == tenant_id)
            .filter(|record| {
                scalar_eq
                    .iter()
                    .all(|(key, value)| record.fields.get(key) == Some(value))
            })
            .map(|record| record.record_id.clone())
            .collect()
    }
}

pub fn write_index_artifact(
    path: impl AsRef<Path>,
    artifact: &IndexArtifact,
    encryption: Option<&EncryptionContext>,
) -> Result<[u8; 32]> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serialize_index_artifact_payload(artifact)?;
    let header = ArtifactEnvelopeHeader::new(
        format!("index-{}", artifact.kind),
        "bincode",
        artifact.segment_id.clone(),
        artifact.segment_generation,
        artifact.manifest_generation,
        artifact.segment_generation,
        artifact.segment_generation,
        artifact.source_segment_checksum,
        &payload,
    );
    let body = encode_artifact_envelope(header, &payload)?;
    let checksum = checksum_bytes(&body);
    let body = match encryption {
        Some(encryption) => encryption.encrypt_artifact("index", &body)?,
        None => body,
    };
    let tmp_path = path.with_extension("tidx.tmp");
    let mut file = File::create(&tmp_path)?;
    file.write_all(&body)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(checksum)
}

pub fn read_index_artifact(
    path: impl AsRef<Path>,
    encryption: Option<&EncryptionContext>,
) -> Result<IndexArtifact> {
    let mut body = Vec::new();
    File::open(path.as_ref())?.read_to_end(&mut body)?;
    let body = decrypt_artifact_if_needed(encryption, "index", &body)?;
    if !body.starts_with(ARTIFACT_ENVELOPE_MAGIC) {
        return Err(TraceDbError::ArtifactCorruption(
            "index artifact envelope magic mismatch".to_string(),
        ));
    }
    let envelope = decode_artifact_envelope(&body)?;
    if !envelope.header.kind.starts_with("index-") {
        return Err(TraceDbError::ArtifactCorruption(format!(
            "index artifact kind mismatch: {}",
            envelope.header.kind
        )));
    }
    let json_bytes: Vec<u8> = bincode::deserialize(&envelope.payload)
        .map_err(|error| TraceDbError::ArtifactCorruption(error.to_string()))?;
    let artifact: IndexArtifact = serde_json::from_slice(&json_bytes)?;
    if envelope.header.kind != format!("index-{}", artifact.kind) {
        return Err(TraceDbError::ArtifactCorruption(format!(
            "index artifact payload kind mismatch: envelope {}, payload {}",
            envelope.header.kind, artifact.kind
        )));
    }
    Ok(artifact)
}

fn build_bitmap_artifact(
    kind: &str,
    segment_id: &str,
    generation: u64,
    manifest_generation: u64,
    source_segment_checksum: [u8; 32],
    records: &[IndexRecord],
) -> IndexArtifact {
    IndexArtifact {
        index_id: format!("{segment_id}:{kind}:{generation}"),
        segment_id: segment_id.to_string(),
        segment_generation: generation,
        manifest_generation,
        kind: kind.to_string(),
        state_history: ready_state_history(),
        policy_aware: true,
        source_segment_checksum,
        record_count: records.len(),
        payload: if kind == "primary" {
            IndexPayload::Primary(build_bitmap_index(
                segment_id,
                generation,
                manifest_generation,
                source_segment_checksum,
                records,
            ))
        } else {
            IndexPayload::Policy(build_bitmap_index(
                segment_id,
                generation,
                manifest_generation,
                source_segment_checksum,
                records,
            ))
        },
    }
}

fn build_bitmap_index(
    segment_id: &str,
    generation: u64,
    manifest_generation: u64,
    source_segment_checksum: [u8; 32],
    records: &[IndexRecord],
) -> BitmapIndexArtifact {
    let records = records
        .iter()
        .map(|record| BitmapRecord {
            record_id: record.record_id.clone(),
            tenant_id: record.tenant_id.clone(),
            fields: record.fields.clone(),
        })
        .collect::<Vec<_>>();
    let mut tenant_records = BTreeMap::<String, Vec<String>>::new();
    for record in &records {
        tenant_records
            .entry(record.tenant_id.clone())
            .or_default()
            .push(record.record_id.clone());
    }
    for ids in tenant_records.values_mut() {
        ids.sort();
    }
    BitmapIndexArtifact {
        index_id: format!("{segment_id}:policy:{generation}"),
        segment_id: segment_id.to_string(),
        generation,
        manifest_generation,
        source_segment_checksum,
        records,
        tenant_records,
    }
}

fn serialize_index_artifact_payload(artifact: &IndexArtifact) -> Result<Vec<u8>> {
    let json_bytes = serde_json::to_vec(artifact)?;
    bincode::serialize(&json_bytes)
        .map_err(|error| TraceDbError::ArtifactCorruption(error.to_string()))
}

fn ready_state_history() -> Vec<String> {
    vec![
        "PENDING".to_string(),
        "BUILDING".to_string(),
        "READY".to_string(),
    ]
}

fn posting_key(field: &str, term: &str) -> String {
    format!("{field}\u{0}{term}")
}

fn vector_node_key(field: &str, record_id: &str) -> String {
    format!("{field}\u{0}{record_id}")
}

fn bm25_from_term_frequency(
    query_terms: &[String],
    tf: &BTreeMap<String, usize>,
    doc_len: usize,
    doc_count: usize,
    avg_len: f32,
    df: &BTreeMap<String, usize>,
) -> f32 {
    let k1 = 1.5;
    let b = 0.75;
    let doc_len = doc_len as f32;
    let mut score = 0.0;
    for term in query_terms {
        let freq = *tf.get(term).unwrap_or(&0) as f32;
        if freq == 0.0 {
            continue;
        }
        let doc_freq = *df.get(term).unwrap_or(&0) as f32;
        let idf = (((doc_count as f32 - doc_freq + 0.5) / (doc_freq + 0.5)) + 1.0).ln();
        let denom = freq + k1 * (1.0 - b + b * doc_len / avg_len.max(1.0));
        score += idf * (freq * (k1 + 1.0)) / denom;
    }
    score
}

fn score_order(left: f32, right: f32) -> std::cmp::Ordering {
    right
        .partial_cmp(&left)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| std::cmp::Ordering::Equal)
}

fn vector_score_order(left: &VectorScore, right: &VectorScore) -> std::cmp::Ordering {
    score_order(left.score, right.score)
        .then_with(|| left.record_id.cmp(&right.record_id))
        .then_with(|| left.version_id.cmp(&right.version_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map};
    use std::collections::BTreeMap;

    const CHECKSUM_VAL: [u8; 32] = [
        0x12u8, 0x34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ];

    fn record(id: &str, body: &str, vector: Vec<f32>) -> IndexRecord {
        IndexRecord {
            table: "docs".to_string(),
            record_id: id.to_string(),
            tenant_id: "tenant-a".to_string(),
            version_id: 1,
            fields: BTreeMap::from([
                ("id".to_string(), json!(id)),
                ("tenant".to_string(), json!("tenant-a")),
                ("body".to_string(), json!(body)),
            ]),
            text: BTreeMap::from([("body".to_string(), body.to_string())]),
            vectors: BTreeMap::from([("embedding".to_string(), vector)]),
        }
    }

    #[test]
    fn text_vector_and_bitmap_index_artifacts_roundtrip_through_binary_envelope() {
        let temp = tempfile::tempdir().expect("tempdir");
        let records = vec![
            record("a", "rare kernel token", vec![1.0, 0.0]),
            record("b", "ordinary text", vec![0.0, 1.0]),
        ];
        let artifacts = build_segment_index_artifacts("seg-1", 1, 77, CHECKSUM_VAL, &records)
            .expect("artifacts");
        assert!(artifacts.iter().any(|artifact| artifact.kind == "text"));
        assert!(artifacts.iter().any(|artifact| artifact.kind == "vector"));
        assert!(artifacts.iter().any(|artifact| artifact.kind == "policy"));
        assert!(artifacts.iter().any(|artifact| artifact.kind == "primary"));

        for artifact in &artifacts {
            let path = temp.path().join(format!("{}.tidx", artifact.index_id));
            write_index_artifact(&path, artifact, None).expect("write index");
            let raw = std::fs::read(&path).expect("raw index");
            assert!(raw.starts_with(tracedb_core::ARTIFACT_ENVELOPE_MAGIC));
            let read = read_index_artifact(&path, None).expect("read index");
            assert_eq!(read.index_id, artifact.index_id);
            assert_eq!(read.source_segment_checksum, CHECKSUM_VAL);
        }
    }

    #[test]
    fn text_index_scores_from_postings_and_vector_index_exposes_hnsw_graph() {
        let records = vec![
            record("rare", "rare kernel token", vec![1.0, 0.0]),
            record("common", "common ordinary text", vec![0.0, 1.0]),
        ];
        let text = build_text_index("seg-1", 1, 77, CHECKSUM_VAL, &records).expect("text index");
        let scores = text.score_text("rare token", Some("body"));
        assert_eq!(
            scores.first().map(|score| score.record_id.as_str()),
            Some("rare")
        );
        assert!(scores[0].score > 0.0);

        let vector =
            build_vector_index("seg-1", 1, 77, CHECKSUM_VAL, &records).expect("vector index");
        let neighbors = vector
            .greedy_nn_neighbors("embedding", "rare")
            .expect("neighbors");
        assert!(!neighbors.is_empty());
        let nearest = vector.search_vector("embedding", &[1.0, 0.0], 2);
        assert_eq!(
            nearest.first().map(|score| score.record_id.as_str()),
            Some("rare")
        );

        let mut scalar_eq = Map::new();
        scalar_eq.insert("tenant".to_string(), json!("tenant-a"));
        let bitmap = build_policy_bitmap_index("seg-1", 1, 77, CHECKSUM_VAL, &records)
            .expect("policy bitmap");
        assert_eq!(
            bitmap
                .visible_record_ids("tenant-a", &scalar_eq)
                .into_iter()
                .collect::<Vec<_>>(),
            vec!["common".to_string(), "rare".to_string()]
        );
    }

    #[test]
    fn vector_search_uses_stable_tie_breaks_and_exact_fallback() {
        let artifact = VectorIndexArtifact {
            index_id: "seg-1:vector:1".to_string(),
            segment_id: "seg-1".to_string(),
            generation: 1,
            manifest_generation: 77,
            source_segment_checksum: CHECKSUM_VAL,
            m: 16,
            ef_construction: 64,
            ef_search: 1,
            algorithm: VectorIndexAlgorithm::LegacyGreedy,
            entries: vec![
                VectorIndexEntry {
                    record_id: "b".to_string(),
                    version_id: 2,
                    field: "embedding".to_string(),
                    vector: vec![1.0, 0.0],
                },
                VectorIndexEntry {
                    record_id: "a".to_string(),
                    version_id: 1,
                    field: "embedding".to_string(),
                    vector: vec![1.0, 0.0],
                },
                VectorIndexEntry {
                    record_id: "c".to_string(),
                    version_id: 3,
                    field: "embedding".to_string(),
                    vector: vec![0.0, 1.0],
                },
            ],
            neighbors: BTreeMap::new(),
            hnsw: BTreeMap::new(),
        };

        let report = artifact.search_vector_with_report("embedding", &[1.0, 0.0], 2);
        let nearest = report.scores;

        assert_eq!(
            nearest
                .iter()
                .map(|score| score.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert!(report.exact_fallback_used);
        assert_eq!(report.visited_count, 1);
    }

    #[test]
    fn vector_search_can_use_hnsw_without_exact_fallback_when_graph_covers_limit() {
        let mut records = vec![
            record("a", "alpha", vec![1.0, 0.0]),
            record("b", "beta", vec![0.9, 0.1]),
            record("c", "gamma", vec![0.8, 0.2]),
            record("d", "delta", vec![0.0, 1.0]),
            record("e", "epsilon", vec![-1.0, 0.0]),
        ];
        for index in 0..80 {
            records.push(record(&format!("far-{index:02}"), "far", vec![0.0, 1.0]));
        }
        let mut artifact =
            build_vector_index("seg-1", 1, 77, CHECKSUM_VAL, &records).expect("vector index");
        artifact.ef_search = 2;

        let report = artifact.search_vector_with_report("embedding", &[1.0, 0.0], 1);

        assert_eq!(
            report.scores.first().map(|score| score.record_id.as_str()),
            Some("a")
        );
        assert!(!report.exact_fallback_used);
        assert!(report.visited_count < report.field_entry_count);
    }

    #[test]
    fn filtered_hnsw_search_only_returns_allowed_records() {
        let records = vec![
            record("global-nearest", "alpha", vec![1.0, 0.0]),
            record("allowed", "beta", vec![0.0, 1.0]),
            record("other", "gamma", vec![-1.0, 0.0]),
        ];
        let artifact =
            build_vector_index("seg-1", 1, 77, CHECKSUM_VAL, &records).expect("vector index");
        let allowed = BTreeSet::from([("allowed".to_string(), 1)]);

        let report =
            artifact.search_vector_with_report_filtered("embedding", &[1.0, 0.0], 1, &allowed);

        assert_eq!(
            report.scores.first().map(|score| score.record_id.as_str()),
            Some("allowed")
        );
        assert!(report
            .scores
            .iter()
            .all(|score| allowed.contains(&(score.record_id.clone(), score.version_id))));
    }
}
