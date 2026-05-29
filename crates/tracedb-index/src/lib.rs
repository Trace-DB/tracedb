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
    pub source_segment_checksum: u32,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorIndexArtifact {
    pub index_id: String,
    pub segment_id: String,
    pub generation: u64,
    pub manifest_generation: u64,
    pub source_segment_checksum: u32,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub entries: Vec<VectorIndexEntry>,
    pub neighbors: BTreeMap<String, Vec<String>>,
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
    pub source_segment_checksum: u32,
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
    pub source_segment_checksum: u32,
    pub record_count: usize,
    pub payload: IndexPayload,
}

impl IndexArtifact {
    pub fn payload_checksum(&self) -> Result<u32> {
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
    source_segment_checksum: u32,
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
    source_segment_checksum: u32,
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

pub fn build_vector_index(
    segment_id: &str,
    generation: u64,
    manifest_generation: u64,
    source_segment_checksum: u32,
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
    let neighbors = deterministic_hnsw_neighbors(&entries, 16);
    Ok(VectorIndexArtifact {
        index_id: format!("{segment_id}:vector:{generation}"),
        segment_id: segment_id.to_string(),
        generation,
        manifest_generation,
        source_segment_checksum,
        m: 16,
        ef_construction: 64,
        ef_search: 64,
        entries,
        neighbors,
    })
}

impl VectorIndexArtifact {
    pub fn hnsw_neighbors(&self, field: &str, record_id: &str) -> Option<&Vec<String>> {
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
        if limit == 0 {
            return VectorSearchReport {
                scores: Vec::new(),
                field_entry_count: 0,
                visited_count: 0,
                exact_fallback_used: false,
            };
        }

        let entries = self
            .entries
            .iter()
            .filter(|entry| entry.field == field)
            .map(|entry| (vector_node_key(field, &entry.record_id), entry))
            .collect::<BTreeMap<_, _>>();
        if entries.is_empty() {
            return VectorSearchReport {
                scores: Vec::new(),
                field_entry_count: 0,
                visited_count: 0,
                exact_fallback_used: false,
            };
        }

        let field_entry_count = entries.len();
        let entry_point = entries.keys().next().expect("non-empty entries").clone();
        let target_visits = self.ef_search.max(limit).min(field_entry_count).max(1);
        let mut scores_by_key = BTreeMap::new();
        let mut visited = BTreeSet::new();
        let mut frontier = BTreeSet::from([entry_point]);

        while visited.len() < target_visits && !frontier.is_empty() {
            let Some(node_key) = best_frontier_key(&frontier, &entries, query, &mut scores_by_key)
            else {
                break;
            };
            frontier.remove(&node_key);
            if !visited.insert(node_key.clone()) {
                continue;
            }

            for neighbor in self
                .neighbors
                .get(&node_key)
                .into_iter()
                .flat_map(|neighbors| neighbors.iter())
                .map(|record_id| vector_node_key(field, record_id))
                .filter(|neighbor_key| entries.contains_key(neighbor_key))
                .collect::<Vec<_>>()
            {
                if !visited.contains(&neighbor) {
                    frontier.insert(neighbor);
                }
            }
        }

        let mut scores = visited
            .iter()
            .filter_map(|key| scores_by_key.get(key).cloned())
            .collect::<Vec<_>>();
        let mut exact_fallback_used = false;
        if visited.len() < field_entry_count {
            exact_fallback_used = true;
            let seen = scores
                .iter()
                .map(|score| vector_node_key(field, &score.record_id))
                .collect::<BTreeSet<_>>();
            scores.extend(
                entries
                    .iter()
                    .filter(|(key, _)| !seen.contains(*key))
                    .filter_map(|(_, entry)| vector_score_for_entry(entry, query)),
            );
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

fn best_frontier_key(
    frontier: &BTreeSet<String>,
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
    scores_by_key: &mut BTreeMap<String, VectorScore>,
) -> Option<String> {
    frontier
        .iter()
        .filter_map(|key| {
            let score = vector_score_for_key(key, entries, query, scores_by_key)?;
            Some((key.clone(), score))
        })
        .min_by(|(_, left), (_, right)| vector_score_order(left, right))
        .map(|(key, _)| key)
}

fn vector_score_for_key(
    key: &str,
    entries: &BTreeMap<String, &VectorIndexEntry>,
    query: &[f32],
    scores_by_key: &mut BTreeMap<String, VectorScore>,
) -> Option<VectorScore> {
    if let Some(score) = scores_by_key.get(key) {
        return Some(score.clone());
    }
    let score = vector_score_for_entry(entries.get(key)?, query)?;
    scores_by_key.insert(key.to_string(), score.clone());
    Some(score)
}

fn vector_score_for_entry(entry: &VectorIndexEntry, query: &[f32]) -> Option<VectorScore> {
    tracedb_vector::cosine_similarity(query, &entry.vector).map(|score| VectorScore {
        record_id: entry.record_id.clone(),
        version_id: entry.version_id,
        score,
    })
}

pub fn build_policy_bitmap_index(
    segment_id: &str,
    generation: u64,
    manifest_generation: u64,
    source_segment_checksum: u32,
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
) -> Result<u32> {
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
    source_segment_checksum: u32,
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
    source_segment_checksum: u32,
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

fn deterministic_hnsw_neighbors(
    entries: &[VectorIndexEntry],
    m: usize,
) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    for entry in entries {
        let mut scored = entries
            .iter()
            .filter(|other| other.field == entry.field && other.record_id != entry.record_id)
            .filter_map(|other| {
                tracedb_vector::cosine_similarity(&entry.vector, &other.vector)
                    .map(|score| (other.record_id.clone(), score))
            })
            .collect::<Vec<_>>();
        scored
            .sort_by(|left, right| score_order(left.1, right.1).then_with(|| left.0.cmp(&right.0)));
        scored.truncate(m);
        out.insert(
            vector_node_key(&entry.field, &entry.record_id),
            scored.into_iter().map(|(record_id, _)| record_id).collect(),
        );
    }
    out
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
        let artifacts =
            build_segment_index_artifacts("seg-1", 1, 77, 1234, &records).expect("artifacts");
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
            assert_eq!(read.source_segment_checksum, 1234);
        }
    }

    #[test]
    fn text_index_scores_from_postings_and_vector_index_exposes_hnsw_graph() {
        let records = vec![
            record("rare", "rare kernel token", vec![1.0, 0.0]),
            record("common", "common ordinary text", vec![0.0, 1.0]),
        ];
        let text = build_text_index("seg-1", 1, 77, 1234, &records).expect("text index");
        let scores = text.score_text("rare token", Some("body"));
        assert_eq!(
            scores.first().map(|score| score.record_id.as_str()),
            Some("rare")
        );
        assert!(scores[0].score > 0.0);

        let vector = build_vector_index("seg-1", 1, 77, 1234, &records).expect("vector index");
        let neighbors = vector
            .hnsw_neighbors("embedding", "rare")
            .expect("neighbors");
        assert!(!neighbors.is_empty());
        let nearest = vector.search_vector("embedding", &[1.0, 0.0], 2);
        assert_eq!(
            nearest.first().map(|score| score.record_id.as_str()),
            Some("rare")
        );

        let mut scalar_eq = Map::new();
        scalar_eq.insert("tenant".to_string(), json!("tenant-a"));
        let bitmap =
            build_policy_bitmap_index("seg-1", 1, 77, 1234, &records).expect("policy bitmap");
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
            source_segment_checksum: 1234,
            m: 16,
            ef_construction: 64,
            ef_search: 1,
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
        let records = vec![
            record("a", "alpha", vec![1.0, 0.0]),
            record("b", "beta", vec![0.9, 0.1]),
        ];
        let mut artifact =
            build_vector_index("seg-1", 1, 77, 1234, &records).expect("vector index");
        artifact.ef_search = 2;

        let report = artifact.search_vector_with_report("embedding", &[1.0, 0.0], 1);

        assert_eq!(
            report.scores.first().map(|score| score.record_id.as_str()),
            Some("a")
        );
        assert!(!report.exact_fallback_used);
        assert_eq!(report.visited_count, report.field_entry_count);
    }
}
