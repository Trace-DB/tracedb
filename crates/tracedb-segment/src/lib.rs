#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use tracedb_core::{
    checksum_bytes, decode_artifact_envelope, decrypt_artifact_if_needed, encode_artifact_envelope,
    ArtifactEnvelopeHeader, EncryptionContext, Result, SegmentManifest, SegmentState, TraceDbError,
    ARTIFACT_ENVELOPE_MAGIC,
};

pub const SEGMENT_OBJECT_FORMAT_VERSION: u32 = 2;
pub const SEGMENT_LEGACY_JSON_FORMAT_VERSION: u32 = 1;

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
    pub payload_checksum: [u8; 32],
    pub object_checksum: [u8; 32],
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
    pub checksum: [u8; 32],
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
        let payload_checksum = checksum_bytes(&bincode::serialize(&records).map_err(|error| {
            TraceDbError::ArtifactCorruption(format!("serialize segment records: {error}"))
        })?);
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
            object_checksum: [0u8; 32],
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
    publish_segment_records_with_encryption(path, segment_id, generation, records, None)
}

pub fn publish_segment_records_with_encryption(
    path: impl AsRef<Path>,
    segment_id: &str,
    generation: u64,
    records: Vec<SegmentRecord>,
    encryption: Option<&EncryptionContext>,
) -> Result<SegmentObject> {
    let path = path.as_ref();
    if path.exists() {
        return read_segment_object_with_encryption(path, encryption);
    }
    let object = SegmentObject::from_records(segment_id, generation, records)?;
    write_segment_object_with_encryption(path, &object, encryption)?;
    read_segment_object_with_encryption(path, encryption)
}

pub fn read_segment_object(path: impl AsRef<Path>) -> Result<SegmentObject> {
    read_segment_object_with_encryption(path, None)
}

pub fn read_segment_object_with_encryption(
    path: impl AsRef<Path>,
    encryption: Option<&EncryptionContext>,
) -> Result<SegmentObject> {
    let mut file = File::open(path)?;
    let mut body = Vec::new();
    file.read_to_end(&mut body)?;
    let body = decrypt_artifact_if_needed(encryption, "segment", &body)?;
    let object: SegmentObject = if body.starts_with(ARTIFACT_ENVELOPE_MAGIC) {
        let envelope = decode_artifact_envelope(&body)?;
        if envelope.header.kind != "segment" {
            return Err(TraceDbError::ArtifactCorruption(format!(
                "segment artifact kind mismatch: {}",
                envelope.header.kind
            )));
        }
        let json_bytes: Vec<u8> = bincode::deserialize(&envelope.payload).map_err(|error| {
            TraceDbError::ArtifactCorruption(format!("decode binary segment payload: {error}"))
        })?;
        serde_json::from_slice(&json_bytes)?
    } else {
        serde_json::from_slice(&body)?
    };
    verify_segment_object(&object)?;
    Ok(object)
}

pub fn write_segment_object(path: impl AsRef<Path>, object: &SegmentObject) -> Result<()> {
    write_segment_object_with_encryption(path, object, None)
}

pub fn write_segment_object_with_encryption(
    path: impl AsRef<Path>,
    object: &SegmentObject,
    encryption: Option<&EncryptionContext>,
) -> Result<()> {
    verify_segment_object(object)?;
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tseg.tmp");
    let json_bytes = serde_json::to_vec(object)?;
    let payload = bincode::serialize(&json_bytes).map_err(|error| {
        TraceDbError::ArtifactCorruption(format!("encode binary segment payload: {error}"))
    })?;
    let header = ArtifactEnvelopeHeader::new(
        "segment",
        "bincode",
        object.segment_id.clone(),
        object.generation,
        object.generation,
        object.epoch_min,
        object.epoch_max,
        object.object_checksum,
        &payload,
    );
    let body = encode_artifact_envelope(header, &payload)?;
    let body = match encryption {
        Some(encryption) => encryption.encrypt_artifact("segment", &body)?,
        None => body,
    };
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
    if !matches!(
        object.format_version,
        SEGMENT_OBJECT_FORMAT_VERSION | SEGMENT_LEGACY_JSON_FORMAT_VERSION
    ) {
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
    if object.payload_checksum == [0u8; 32] {
        return Err(TraceDbError::ManifestCorruption(format!(
            "segment object {} has empty payload checksum",
            object.segment_id
        )));
    }
    let actual = compute_segment_object_checksum(object)?;
    let checksum_matches = actual == object.object_checksum
        || (object.format_version == SEGMENT_LEGACY_JSON_FORMAT_VERSION
            && compute_segment_object_checksum_legacy_json(object)? == object.object_checksum);
    if !checksum_matches {
        return Err(TraceDbError::ManifestCorruption(format!(
            "segment object checksum mismatch: expected {:?}, got {actual:?}",
            object.object_checksum
        )));
    }
    Ok(())
}

pub fn compute_segment_object_checksum(object: &SegmentObject) -> Result<[u8; 32]> {
    let mut normalized = object.clone();
    normalized.object_checksum = [0u8; 32];
    let bytes = serde_json::to_vec(&normalized)?;
    let round_tripped: SegmentObject = serde_json::from_slice(&bytes)?;
    Ok(checksum_bytes(&serde_json::to_vec(&round_tripped)?))
}

fn compute_segment_object_checksum_legacy_json(object: &SegmentObject) -> Result<[u8; 32]> {
    let mut normalized = object.clone();
    normalized.object_checksum = [0u8; 32];
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

    #[test]
    fn segment_writer_uses_binary_envelope_and_reader_accepts_legacy_json() {
        let temp = tempfile::tempdir().expect("tempdir");
        let record = SegmentRecord {
            table: "docs".to_string(),
            record_id: "a".to_string(),
            tenant_id: "tenant-a".to_string(),
            version_id: 1,
            fields: BTreeMap::from([
                ("id".to_string(), json!("a")),
                ("tenant".to_string(), json!("tenant-a")),
                ("body".to_string(), json!("legacy and binary")),
            ]),
            text: BTreeMap::from([("body".to_string(), "legacy and binary".to_string())]),
            vectors: BTreeMap::from([("embedding".to_string(), vec![1.0, 0.0])]),
        };
        let object =
            SegmentObject::from_records("seg-binary", 1, vec![record.clone()]).expect("object");
        let binary_path = temp.path().join("seg-binary.tseg");
        write_segment_object(&binary_path, &object).expect("write binary segment");

        let raw = std::fs::read(&binary_path).expect("raw segment");
        assert!(raw.starts_with(tracedb_core::ARTIFACT_ENVELOPE_MAGIC));
        let envelope = tracedb_core::decode_artifact_envelope(&raw).expect("segment envelope");
        assert_eq!(envelope.header.kind, "segment");
        assert_eq!(envelope.header.codec, "bincode");
        assert_eq!(
            envelope.header.payload_checksum,
            checksum_bytes(&envelope.payload)
        );

        let binary_read = read_segment_object(&binary_path).expect("read binary segment");
        assert_eq!(binary_read, object);

        let legacy_path = temp.path().join("legacy-json.tseg");
        let mut legacy =
            SegmentObject::from_records("legacy-json", 1, vec![record]).expect("legacy object");
        legacy.format_version = 1;
        legacy.object_checksum =
            compute_segment_object_checksum_legacy_json(&legacy).expect("legacy checksum");
        std::fs::write(
            &legacy_path,
            serde_json::to_vec_pretty(&legacy).expect("legacy json"),
        )
        .expect("write legacy json");
        let legacy_read = read_segment_object(&legacy_path).expect("read legacy json");
        assert_eq!(legacy_read.segment_id, "legacy-json");
        assert_eq!(legacy_read.format_version, 1);
    }
}
