#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use tracedb_core::{checksum_bytes, Result, SegmentManifest, SegmentState, TraceDbError};

pub const SEGMENT_OBJECT_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SegmentObject {
    pub format_version: u32,
    pub segment_id: String,
    pub generation: u64,
    pub state: SegmentState,
    pub state_history: Vec<SegmentState>,
    pub epoch_min: u64,
    pub epoch_max: u64,
    pub table_set: Vec<String>,
    pub tenant_set: Vec<String>,
    pub module_blocks: Vec<ModuleBlockDescriptor>,
    pub records: Vec<SegmentRecord>,
    pub payload_checksum: u32,
    pub object_checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SegmentRecord {
    pub table: String,
    pub record_id: String,
    pub tenant_id: String,
    pub version_id: u64,
    pub fields: BTreeMap<String, Value>,
    pub text: BTreeMap<String, String>,
    pub vectors: BTreeMap<String, Vec<f32>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModuleBlockDescriptor {
    pub module_id: String,
    pub module_version: String,
    pub block_kind: String,
    pub codec_id: String,
    pub logical_row_count: usize,
    pub checksum: u32,
}

impl ModuleBlockDescriptor {
    pub fn new(
        module_id: impl Into<String>,
        block_kind: impl Into<String>,
        codec_id: impl Into<String>,
        logical_row_count: usize,
    ) -> Self {
        let module_id = module_id.into();
        let block_kind = block_kind.into();
        let codec_id = codec_id.into();
        let checksum = checksum_bytes(
            format!("{module_id}:{block_kind}:{codec_id}:{logical_row_count}").as_bytes(),
        );
        Self {
            module_id,
            module_version: "0.1.0".to_string(),
            block_kind,
            codec_id,
            logical_row_count,
            checksum,
        }
    }
}

impl SegmentObject {
    pub fn minimal(segment_id: impl Into<String>, generation: u64) -> Result<Self> {
        Self::from_records(segment_id, generation, Vec::new())
    }

    pub fn from_records(
        segment_id: impl Into<String>,
        generation: u64,
        records: Vec<SegmentRecord>,
    ) -> Result<Self> {
        let segment_id = segment_id.into();
        let payload_checksum = checksum_bytes(&serde_json::to_vec(&records)?);
        let state_history = publication_state_history();
        let table_set = sorted_unique(records.iter().map(|record| record.table.clone()));
        let tenant_set = sorted_unique(records.iter().map(|record| record.tenant_id.clone()));
        let mut object = Self {
            format_version: SEGMENT_OBJECT_FORMAT_VERSION,
            segment_id,
            generation,
            state: SegmentState::Published,
            state_history,
            epoch_min: generation,
            epoch_max: generation,
            table_set,
            tenant_set,
            module_blocks: vec![
                ModuleBlockDescriptor::new(
                    "tracedb-text",
                    "postings",
                    "text-postings-v1",
                    records
                        .iter()
                        .filter(|record| !record.text.is_empty())
                        .count(),
                ),
                ModuleBlockDescriptor::new(
                    "tracedb-vector",
                    "vector-pages",
                    "vector-pages-v1",
                    records
                        .iter()
                        .filter(|record| !record.vectors.is_empty())
                        .count(),
                ),
                ModuleBlockDescriptor::new(
                    "tracedb-policy",
                    "policy-bitmaps",
                    "policy-bitmap-v1",
                    records.len(),
                ),
            ],
            records,
            payload_checksum,
            object_checksum: 0,
        };
        object.object_checksum = compute_segment_object_checksum(&object)?;
        Ok(object)
    }

    pub fn manifest(&self) -> SegmentManifest {
        SegmentManifest {
            segment_id: self.segment_id.clone(),
            generation: self.generation,
            state: self.state.clone(),
            table_set: self.table_set.clone(),
            tenant_set: self.tenant_set.clone(),
        }
    }
}

pub fn published_segment(segment_id: impl Into<String>, generation: u64) -> SegmentManifest {
    SegmentManifest {
        segment_id: segment_id.into(),
        generation,
        state: SegmentState::Published,
        table_set: Vec::new(),
        tenant_set: Vec::new(),
    }
}

pub fn publish_segment_object(
    path: impl AsRef<Path>,
    segment_id: &str,
    generation: u64,
) -> Result<SegmentObject> {
    let path = path.as_ref();
    if path.exists() {
        let object = read_segment_object(path)?;
        if object.segment_id != segment_id {
            return Err(TraceDbError::ManifestCorruption(format!(
                "segment object id mismatch: expected {segment_id}, got {}",
                object.segment_id
            )));
        }
        if object.generation != generation {
            return Err(TraceDbError::ManifestCorruption(format!(
                "segment object generation mismatch: expected {generation}, got {}",
                object.generation
            )));
        }
        if object.state != SegmentState::Published {
            return Err(TraceDbError::ManifestCorruption(format!(
                "segment object {} is not published",
                object.segment_id
            )));
        }
        return Ok(object);
    }

    let object = SegmentObject::minimal(segment_id, generation)?;
    write_segment_object(path, &object)?;
    read_segment_object(path)
}

pub fn publish_segment_records(
    path: impl AsRef<Path>,
    segment_id: &str,
    generation: u64,
    records: Vec<SegmentRecord>,
) -> Result<SegmentObject> {
    let path = path.as_ref();
    if path.exists() {
        return read_segment_object(path);
    }
    let object = SegmentObject::from_records(segment_id, generation, records)?;
    write_segment_object(path, &object)?;
    read_segment_object(path)
}

pub fn read_segment_object(path: impl AsRef<Path>) -> Result<SegmentObject> {
    let mut file = File::open(path)?;
    let mut body = Vec::new();
    file.read_to_end(&mut body)?;
    let object: SegmentObject = serde_json::from_slice(&body)?;
    verify_segment_object(&object)?;
    Ok(object)
}

pub fn write_segment_object(path: impl AsRef<Path>, object: &SegmentObject) -> Result<()> {
    verify_segment_object(object)?;
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tseg.tmp");
    let body = serde_json::to_vec_pretty(object)?;
    let mut file = File::create(&tmp_path)?;
    file.write_all(&body)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub fn verify_segment_object(object: &SegmentObject) -> Result<()> {
    if object.format_version != SEGMENT_OBJECT_FORMAT_VERSION {
        return Err(TraceDbError::ManifestCorruption(format!(
            "unsupported segment object format {}",
            object.format_version
        )));
    }
    if object.segment_id.trim().is_empty() {
        return Err(TraceDbError::ManifestCorruption(
            "segment object id cannot be empty".to_string(),
        ));
    }
    if object.payload_checksum == 0 {
        return Err(TraceDbError::ManifestCorruption(format!(
            "segment object {} has empty payload checksum",
            object.segment_id
        )));
    }
    let actual = compute_segment_object_checksum(object)?;
    if actual != object.object_checksum {
        return Err(TraceDbError::ManifestCorruption(format!(
            "segment object checksum mismatch: expected {}, got {actual}",
            object.object_checksum
        )));
    }
    Ok(())
}

pub fn compute_segment_object_checksum(object: &SegmentObject) -> Result<u32> {
    let mut normalized = object.clone();
    normalized.object_checksum = 0;
    let bytes = serde_json::to_vec(&normalized)?;
    let round_tripped: SegmentObject = serde_json::from_slice(&bytes)?;
    Ok(checksum_bytes(&serde_json::to_vec(&round_tripped)?))
}

fn publication_state_history() -> Vec<SegmentState> {
    vec![
        SegmentState::Building,
        SegmentState::Built,
        SegmentState::Verifying,
        SegmentState::Verified,
        SegmentState::Publishing,
        SegmentState::Published,
    ]
}

fn sorted_unique(values: impl Iterator<Item = String>) -> Vec<String> {
    let mut values = values.collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn segment_object_checksum_survives_provider_vector_json_roundtrip() {
        let vector = (0..2048)
            .map(|index| ((index as f32 + 0.5) / 2048.0).sin() / 32.0)
            .collect::<Vec<_>>();
        let record = SegmentRecord {
            table: "bench_records".to_string(),
            record_id: "github://example/repo/file.py#L1-L8".to_string(),
            tenant_id: "tenant-a".to_string(),
            version_id: 42,
            fields: BTreeMap::from([
                (
                    "id".to_string(),
                    json!("github://example/repo/file.py#L1-L8"),
                ),
                ("tenant".to_string(), json!("tenant-a")),
                (
                    "body".to_string(),
                    json!("def provider_vector_fixture(): pass"),
                ),
                (
                    "embedding".to_string(),
                    serde_json::to_value(&vector).expect("vector json"),
                ),
            ]),
            text: BTreeMap::from([(
                "body".to_string(),
                "def provider_vector_fixture(): pass".to_string(),
            )]),
            vectors: BTreeMap::from([("embedding".to_string(), vector)]),
        };
        let object = SegmentObject::from_records("provider-vector-segment", 7, vec![record])
            .expect("segment object");
        let bytes = serde_json::to_vec_pretty(&object).expect("serialize");
        let round_tripped: SegmentObject = serde_json::from_slice(&bytes).expect("parse");

        verify_segment_object(&round_tripped).expect("round-tripped checksum remains valid");
    }
}
