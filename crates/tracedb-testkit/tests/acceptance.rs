use serde_json::json;
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;
use tempfile::TempDir;
use tracedb_core::{
    checksum_bytes, compute_manifest_checksum, Epoch, FeatureStatus, IndexState, SegmentState,
    TraceDbManifest,
};
use tracedb_log::{CommitRecord, Wal};
use tracedb_modules::{AccessPathDescriptor, ModuleRegistry, TraceDbModule};
use tracedb_query::{
    FreshnessMode, HybridQuery, RecordInput, TableSchema, TraceDb, VectorColumnSchema,
};

fn db() -> (TempDir, TraceDb) {
    let temp = tempfile::tempdir().expect("tempdir");
    let db = TraceDb::open(temp.path()).expect("open db");
    (temp, db)
}

fn schema() -> TableSchema {
    TableSchema {
        name: "docs".to_string(),
        primary_id_column: "id".to_string(),
        tenant_id_column: "tenant".to_string(),
        scalar_columns: vec!["conversation".to_string()],
        text_indexed_columns: vec!["body".to_string()],
        vector_columns: vec![VectorColumnSchema {
            name: "embedding".to_string(),
            dimensions: 3,
            source_columns: vec!["body".to_string()],
        }],
    }
}

fn record(id: &str, tenant: &str, body: &str, vector: [f32; 3]) -> RecordInput {
    RecordInput {
        table: "docs".to_string(),
        id: id.to_string(),
        tenant_id: tenant.to_string(),
        fields: json!({
            "id": id,
            "tenant": tenant,
            "conversation": "c1",
            "body": body,
            "embedding": vector,
        })
        .as_object()
        .unwrap()
        .clone(),
    }
}

fn seeded_db() -> (TempDir, TraceDb) {
    let (temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    db.insert(record(
        "a",
        "tenant-a",
        "rust database kernel",
        [1.0, 0.0, 0.0],
    ))
    .expect("insert a");
    db.insert(record(
        "b",
        "tenant-a",
        "vector lexical fusion",
        [0.9, 0.1, 0.0],
    ))
    .expect("insert b");
    db.insert(record(
        "c",
        "tenant-b",
        "rust secret tenant",
        [1.0, 0.0, 0.0],
    ))
    .expect("insert c");
    (temp, db)
}

fn query() -> HybridQuery {
    HybridQuery {
        table: "docs".to_string(),
        tenant_id: "tenant-a".to_string(),
        text: Some("rust kernel".to_string()),
        vector: Some(vec![1.0, 0.0, 0.0]),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 5,
        freshness: FreshnessMode::Strict,
        explain: true,
    }
}

#[test]
fn open_database_directory() {
    let (temp, _db) = db();
    assert!(temp.path().join("manifest.tdb").exists());
    assert!(temp.path().join("wal/000001.twal").exists());
    assert!(temp.path().join("hot/rows").exists());
}

#[test]
fn create_table_manifest() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    let manifest = db.inspect_manifest().expect("manifest");
    assert_eq!(manifest.schemas[0].name, "docs");
    assert_eq!(manifest.schemas[0].text_indexed_columns, vec!["body"]);
}

#[test]
fn insert_assigns_epoch() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    let epoch = db
        .insert(record(
            "a",
            "tenant-a",
            "rust database kernel",
            [1.0, 0.0, 0.0],
        ))
        .expect("insert");
    assert_eq!(epoch.get(), 2);
    assert_eq!(db.inspect_manifest().unwrap().latest_epoch.get(), 2);
}

#[test]
fn wal_replay_recovers_committed_epoch() {
    let (temp, db) = seeded_db();
    let before = db.inspect_manifest().unwrap().latest_epoch;
    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(recovered.inspect_manifest().unwrap().latest_epoch, before);
    assert_eq!(recovered.query(query()).unwrap().results.len(), 2);
}

#[test]
fn partial_wal_frame_is_not_committed() {
    let (temp, db) = seeded_db();
    drop(db);
    std::fs::OpenOptions::new()
        .append(true)
        .open(temp.path().join("wal/000001.twal"))
        .unwrap()
        .write_all(b"partial-frame")
        .unwrap();
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(recovered.inspect_manifest().unwrap().latest_epoch.get(), 4);
}

#[test]
fn snapshot_read_is_stable() {
    let (_temp, mut db) = seeded_db();
    let snapshot = db.snapshot().expect("snapshot");
    db.insert(record("d", "tenant-a", "new rust row", [1.0, 0.0, 0.0]))
        .expect("insert");
    let rows = snapshot.visible_records("docs", "tenant-a");
    assert_eq!(rows.len(), 2);
}

#[test]
fn hot_overlay_immediate_visibility() {
    let (_temp, db) = seeded_db();
    let result = db.query(query()).expect("query");
    assert!(result
        .results
        .iter()
        .any(|row| row.record_id.as_str() == "a"));
    assert!(result.explain.hot_overlay_searched);
}

#[test]
fn register_text_and_vector_modules() {
    let (_temp, db) = db();
    let modules = db.registered_modules();
    assert!(modules.iter().any(|m| m == "tracedb-text"));
    assert!(modules.iter().any(|m| m == "tracedb-vector"));
}

#[test]
fn builtin_modules_catalog_access_paths_explain_codecs_and_decoders() {
    let (_temp, db) = db();
    let catalog = db.registered_module_catalog();

    let text = catalog
        .iter()
        .find(|module| module.module_id == "tracedb-text")
        .expect("text module");
    assert!(text
        .access_paths
        .iter()
        .any(|path| path.access_path_id == "LexicalPath" && path.policy_aware));
    assert!(!text.explain_hooks.is_empty());
    assert!(text
        .segment_codecs
        .iter()
        .any(|codec| codec.codec_id == "text-postings-v1"));
    assert!(text
        .wal_decoders
        .iter()
        .any(|decoder| decoder.decoder_id == "text-wal-v1"));

    let vector = catalog
        .iter()
        .find(|module| module.module_id == "tracedb-vector")
        .expect("vector module");
    assert!(vector
        .access_paths
        .iter()
        .any(|path| path.access_path_id == "VectorPath" && path.policy_aware));
    assert!(!vector.explain_hooks.is_empty());
    assert!(vector
        .segment_codecs
        .iter()
        .any(|codec| codec.codec_id == "vector-pages-v1"));
    assert!(vector
        .wal_decoders
        .iter()
        .any(|decoder| decoder.decoder_id == "vector-wal-v1"));
}

#[test]
fn text_module_codecs_and_wal_decoders_roundtrip_payloads() {
    let postings = tracedb_text::TextPostingsBlock {
        term: "rust".to_string(),
        postings: vec![tracedb_text::TextPosting {
            record_id: "a".to_string(),
            positions: vec![0, 2],
        }],
    };
    let encoded = tracedb_text::encode_text_postings(&postings).expect("encode postings");
    let decoded = tracedb_text::decode_text_postings(&encoded).expect("decode postings");
    assert_eq!(decoded, postings);

    let event = tracedb_text::TextWalEvent {
        table: "docs".to_string(),
        record_id: "a".to_string(),
        terms: vec!["rust".to_string(), "kernel".to_string()],
    };
    let encoded = tracedb_text::encode_text_wal_event(&event).expect("encode wal");
    let decoded = tracedb_text::decode_text_wal_event(&encoded).expect("decode wal");
    assert_eq!(decoded, event);
}

#[test]
fn vector_module_codecs_and_wal_decoders_roundtrip_payloads() {
    let page = tracedb_vector::VectorPage {
        vector_column: "embedding".to_string(),
        vectors: vec![tracedb_vector::VectorEntry {
            record_id: "a".to_string(),
            values: vec![1.0, 0.0, 0.0],
        }],
    };
    let encoded = tracedb_vector::encode_vector_page(&page).expect("encode page");
    let decoded = tracedb_vector::decode_vector_page(&encoded).expect("decode page");
    assert_eq!(decoded, page);

    let event = tracedb_vector::VectorWalEvent {
        table: "docs".to_string(),
        record_id: "a".to_string(),
        vector_column: "embedding".to_string(),
        dimensions: 3,
        status: FeatureStatus::Ready,
    };
    let encoded = tracedb_vector::encode_vector_wal_event(&event).expect("encode wal");
    let decoded = tracedb_vector::decode_vector_wal_event(&encoded).expect("decode wal");
    assert_eq!(decoded, event);
}

#[test]
fn bm25_rare_symbols_rank_deterministically() {
    let docs = vec![
        (
            "id-exact".to_string(),
            "user_id tenant_id trace_id request_id".to_string(),
        ),
        (
            "route-exact".to_string(),
            "app/api/chat/route.ts POST /api/chat handleChatRoute".to_string(),
        ),
        (
            "path-exact".to_string(),
            "crates/tracedb-query/src/lib.rs query_access_paths".to_string(),
        ),
        (
            "error-exact".to_string(),
            "ERR_VECTOR_DIMENSION_MISMATCH invalid vector dimensions".to_string(),
        ),
        (
            "function-exact".to_string(),
            "score_corpus cosine_similarity vector_only_query_orders_by_exact_similarity"
                .to_string(),
        ),
        (
            "config-exact".to_string(),
            "TRACEDB_VECTOR_DIMENSIONS trace.vector.dimensions".to_string(),
        ),
        (
            "distractor".to_string(),
            "chat vector query dimensions tenant route source corpus".to_string(),
        ),
    ];

    for (query, expected_id) in [
        ("trace_id", "id-exact"),
        ("/api/chat", "route-exact"),
        ("crates/tracedb-query/src/lib.rs", "path-exact"),
        ("ERR_VECTOR_DIMENSION_MISMATCH", "error-exact"),
        ("cosine_similarity", "function-exact"),
        ("trace.vector.dimensions", "config-exact"),
    ] {
        let mut scores = tracedb_text::score_corpus(query, &docs);
        scores.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .expect("BM25 scores should be finite")
                .then_with(|| left.0.cmp(&right.0))
        });
        let top = scores
            .first()
            .unwrap_or_else(|| panic!("expected scores for rare-symbol query {query:?}"));
        assert_eq!(
            top.0, expected_id,
            "rare-symbol query {query:?} should rank its exact symbol document first; scores: {scores:?}"
        );
    }

    assert!(
        tracedb_text::score_corpus(" ./---_ ", &docs).is_empty(),
        "empty lexical query should return no scores"
    );
}

#[test]
fn exact_vector_cosine_orders_candidates() {
    let query = [1.0, 0.0, 0.0];
    let mut scores = vec![
        (
            "far",
            tracedb_vector::cosine_similarity(&query, &[0.2, 0.98, 0.0])
                .expect("far vector should score"),
        ),
        (
            "orthogonal",
            tracedb_vector::cosine_similarity(&query, &[0.0, 1.0, 0.0])
                .expect("orthogonal vector should score"),
        ),
        (
            "near",
            tracedb_vector::cosine_similarity(&query, &[0.99, 0.01, 0.0])
                .expect("near vector should score"),
        ),
    ];
    scores.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .expect("cosine scores should be finite")
            .then_with(|| left.0.cmp(right.0))
    });

    assert_eq!(
        scores.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec!["near", "far", "orthogonal"],
        "exact cosine scoring should order near above far and orthogonal; scores: {scores:?}"
    );
    assert!(
        scores[0].1 > scores[1].1 && scores[1].1 > scores[2].1,
        "cosine scores should be strictly descending for the fixture; scores: {scores:?}"
    );
}

#[test]
fn exact_vector_rejects_invalid_vectors() {
    assert_eq!(
        tracedb_vector::cosine_similarity(&[1.0, 0.0, 0.0], &[1.0, 0.0]),
        None,
        "dimension mismatch should produce no vector score"
    );
    assert_eq!(
        tracedb_vector::cosine_similarity(&[0.0, 0.0, 0.0], &[1.0, 0.0, 0.0]),
        None,
        "zero query vector should produce no vector score"
    );
    assert_eq!(
        tracedb_vector::cosine_similarity(&[1.0, 0.0, 0.0], &[0.0, 0.0, 0.0]),
        None,
        "zero candidate vector should produce no vector score"
    );
}

#[test]
fn vector_only_query_orders_by_exact_similarity() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    db.insert(record(
        "far",
        "tenant-a",
        "far vector inserted before near vector",
        [0.2, 0.98, 0.0],
    ))
    .expect("insert far");
    db.insert(record(
        "near",
        "tenant-a",
        "near vector should rank first",
        [0.99, 0.01, 0.0],
    ))
    .expect("insert near");
    db.insert(record(
        "orthogonal",
        "tenant-a",
        "orthogonal vector should rank last",
        [0.0, 1.0, 0.0],
    ))
    .expect("insert orthogonal");

    let result = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 3,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query");

    assert_eq!(
        result
            .results
            .iter()
            .map(|row| row.record_id.as_str())
            .collect::<Vec<_>>(),
        vec!["near", "far", "orthogonal"],
        "vector-only query should preserve exact cosine ordering; results: {:?}",
        result.results
    );
    assert!(
        result.results.iter().all(|row| row.score.vector.is_some()),
        "vector-only fixture should return vector-scored rows; results: {:?}",
        result.results
    );
}

#[test]
fn text_candidate_stream_explain() {
    let (_temp, db) = seeded_db();
    let result = db.query(query()).expect("query");
    assert!(result
        .explain
        .opened_candidate_streams
        .contains(&"text".to_string()));
    assert!(result.explain.text_candidates > 0);
}

#[test]
fn exact_vector_candidate_stream_explain() {
    let (_temp, db) = seeded_db();
    let result = db.query(query()).expect("query");
    assert!(result
        .explain
        .opened_candidate_streams
        .contains(&"vector".to_string()));
    assert!(result.explain.vector_candidates > 0);
}

#[test]
fn vector_query_dimension_mismatch_is_error_not_fallback() {
    let (_temp, db) = seeded_db();
    let err = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text: None,
            vector: Some(vec![1.0, 0.0]),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .unwrap_err();
    assert!(err.to_string().contains("invalid vector dimensions"));
}

#[test]
fn policy_relational_and_hot_paths_feed_fallback_results_without_text_or_vector() {
    let (_temp, db) = seeded_db();
    let result = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text: None,
            vector: None,
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query");

    assert_eq!(result.results.len(), 2);
    assert!(result
        .results
        .iter()
        .all(|row| row.score.relational.is_some()));
    assert_eq!(result.explain.text_candidates, 0);
    assert_eq!(result.explain.vector_candidates, 0);
    assert_eq!(result.explain.deduped_candidate_count, 2);
    for expected in ["PolicyPath", "RelationalPath", "HotOverlayPath"] {
        assert!(result
            .explain
            .planner_candidates
            .iter()
            .any(|candidate| candidate.source == expected));
    }
}

#[test]
fn relational_fallback_returns_dirty_records_skipped_by_vector_path() {
    let (_temp, mut db) = seeded_db();
    db.insert(RecordInput {
        table: "docs".to_string(),
        id: "a".to_string(),
        tenant_id: "tenant-a".to_string(),
        fields: json!({ "body": "rust changed source text" })
            .as_object()
            .unwrap()
            .clone(),
    })
    .expect("update");

    let result = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query");

    assert_eq!(result.explain.vector_candidates, 1);
    let dirty_row = result
        .results
        .iter()
        .find(|row| row.record_id == "a")
        .expect("dirty row returned by fallback");
    assert!(dirty_row.score.vector.is_none());
    assert!(dirty_row.score.relational.is_some());
}

#[test]
fn query_execution_uses_real_planner_access_paths_and_candidates() {
    let (_temp, db) = seeded_db();
    let result = db.query(query()).expect("query");

    let opened_paths = result
        .explain
        .access_paths
        .iter()
        .filter(|path| path.opened)
        .map(|path| path.access_path_id.as_str())
        .collect::<BTreeSet<_>>();
    for expected in [
        "PolicyPath",
        "RelationalPath",
        "HotOverlayPath",
        "LexicalPath",
        "VectorPath",
    ] {
        assert!(opened_paths.contains(expected), "{expected} was not opened");
    }

    let candidate_sources = result
        .explain
        .planner_candidates
        .iter()
        .map(|candidate| candidate.source.as_str())
        .collect::<BTreeSet<_>>();
    for expected in ["PolicyPath", "HotOverlayPath", "LexicalPath", "VectorPath"] {
        assert!(
            candidate_sources.contains(expected),
            "{expected} produced no planner candidates"
        );
    }
    for candidate in &result.explain.planner_candidates {
        assert!(!candidate.record_id.is_empty());
        assert!(candidate.version_id > 0);
        assert!(!candidate.source.is_empty());
        assert!(candidate.score_upper_bound.is_some());
        assert!(candidate.visibility_checked);
    }
}

#[test]
fn hybrid_rrf_fusion() {
    let (_temp, db) = seeded_db();
    let result = db.query(query()).expect("query");
    assert_eq!(result.explain.fusion_method, "RRF");
    assert!(result.results[0].score.final_score > 0.0);
}

#[test]
fn update_text_marks_vector_dirty() {
    let (_temp, mut db) = seeded_db();
    db.insert(RecordInput {
        table: "docs".to_string(),
        id: "a".to_string(),
        tenant_id: "tenant-a".to_string(),
        fields: json!({
            "body": "rust changed source text",
        })
        .as_object()
        .unwrap()
        .clone(),
    })
    .expect("update");
    let state = db
        .feature_state("docs", "tenant-a", "a", "embedding")
        .expect("feature state");
    assert_eq!(state.status, FeatureStatus::Dirty);
}

#[test]
fn strict_query_skips_dirty_vector() {
    let (_temp, mut db) = seeded_db();
    db.insert(RecordInput {
        table: "docs".to_string(),
        id: "a".to_string(),
        tenant_id: "tenant-a".to_string(),
        fields: json!({ "body": "rust changed source text" })
            .as_object()
            .unwrap()
            .clone(),
    })
    .expect("update");
    let result = db.query(query()).expect("query");
    let row = result
        .results
        .iter()
        .find(|r| r.record_id.as_str() == "a")
        .unwrap();
    assert!(row.score.vector.is_none());
    assert!(result.explain.dirty_feature_count > 0);
}

#[test]
fn lazy_query_uses_text_fallback() {
    let (_temp, mut db) = seeded_db();
    db.insert(RecordInput {
        table: "docs".to_string(),
        id: "a".to_string(),
        tenant_id: "tenant-a".to_string(),
        fields: json!({ "body": "rust changed source" })
            .as_object()
            .unwrap()
            .clone(),
    })
    .expect("update");
    let mut q = query();
    q.freshness = FreshnessMode::Lazy;
    let result = db.query(q).expect("query");
    let row = result
        .results
        .iter()
        .find(|r| r.record_id.as_str() == "a")
        .unwrap();
    assert!(row.score.lexical.is_some());
    assert!(row.score.vector.is_none());
}

#[test]
fn tenant_mask_applies_before_text_search() {
    let (_temp, db) = seeded_db();
    let result = db.query(query()).expect("query");
    assert_eq!(result.explain.tenant_mask_visible_records, 2);
    assert!(result.results.iter().all(|r| r.tenant_id == "tenant-a"));
}

#[test]
fn tenant_mask_applies_before_vector_search() {
    let (_temp, db) = seeded_db();
    let result = db.query(query()).expect("query");
    assert_eq!(result.explain.vector_candidates, 2);
    assert!(result.results.iter().all(|r| r.record_id.as_str() != "c"));
}

#[test]
fn final_visibility_guard_removes_only() {
    let (_temp, db) = seeded_db();
    let result = db.query(query()).expect("query");
    assert_eq!(
        result.explain.final_visibility_guard_count,
        result.explain.deduped_candidate_count
    );
    assert!(result.explain.final_visibility_guard_count >= result.explain.returned_count);
    assert_eq!(result.explain.final_visibility_guard_removed, 0);
}

#[test]
fn final_visibility_guard_counts_checked_candidates_not_only_returned_rows() {
    let (_temp, db) = seeded_db();
    let mut q = query();
    q.top_k = 1;
    let result = db.query(q).expect("query");
    assert_eq!(result.explain.returned_count, 1);
    assert!(result.explain.final_visibility_guard_count > result.explain.returned_count);
    assert_eq!(
        result.explain.final_visibility_guard_count,
        result.explain.deduped_candidate_count
    );
}

#[test]
fn segment_publish_requires_manifest_reference() {
    let (temp, mut db) = seeded_db();
    std::fs::write(temp.path().join("segments/orphan.segment"), b"orphan").unwrap();
    assert!(db.inspect_manifest().unwrap().segments.is_empty());
    db.publish_segment("seg-1").expect("publish");
    assert_eq!(
        db.inspect_manifest().unwrap().segments[0].segment_id,
        "seg-1"
    );
}

#[test]
fn published_segment_object_has_format_state_and_checksum() {
    let (temp, mut db) = seeded_db();
    let parent_generation = db.inspect_manifest().unwrap().manifest_generation;
    db.publish_segment_with_parent_generation("seg-1", parent_generation)
        .expect("publish");

    let object = tracedb_segment::read_segment_object(temp.path().join("segments/seg-1.tseg"))
        .expect("segment object");
    assert_eq!(
        object.format_version,
        tracedb_segment::SEGMENT_OBJECT_FORMAT_VERSION
    );
    assert_eq!(object.segment_id, "seg-1");
    assert_eq!(object.state, SegmentState::Published);
    assert_ne!(object.payload_checksum, 0);
    assert_ne!(object.object_checksum, 0);

    let manifest = db.inspect_manifest().unwrap();
    assert_eq!(manifest.segments[0].segment_id, "seg-1");
    assert_eq!(manifest.segments[0].state, SegmentState::Published);
    assert!(manifest.indexes.iter().any(|index| {
        index.segment_id == "seg-1"
            && index.kind == "text"
            && index.state == IndexState::Ready
            && index.policy_aware
            && index.parent_manifest_generation == parent_generation
            && temp.path().join(&index.object_path).exists()
    }));
    assert!(manifest.indexes.iter().any(|index| {
        index.segment_id == "seg-1"
            && index.kind == "vector"
            && index.state == IndexState::Ready
            && index.checksum != 0
    }));

    let segment_count = manifest.segments.len();
    let index_count = manifest.indexes.len();
    let generation = manifest.manifest_generation;
    db.publish_segment("seg-1").expect("idempotent republish");
    let after = db.inspect_manifest().unwrap();
    assert_eq!(after.segments.len(), segment_count);
    assert_eq!(after.indexes.len(), index_count);
    assert_eq!(after.manifest_generation, generation);
}

#[test]
fn segment_publish_rejects_parent_manifest_generation_mismatch() {
    let (_temp, mut db) = seeded_db();
    let parent_generation = db.inspect_manifest().unwrap().manifest_generation;
    let err = db
        .publish_segment_with_parent_generation("seg-1", parent_generation + 1)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("parent manifest generation mismatch"));
    assert!(db.inspect_manifest().unwrap().segments.is_empty());
}

#[test]
fn segment_publish_rejects_stale_handle_using_durable_manifest_generation() {
    let (temp, mut first) = seeded_db();
    let mut stale = TraceDb::open(temp.path()).expect("stale handle");
    let parent_generation = first.inspect_manifest().unwrap().manifest_generation;

    first
        .publish_segment_with_parent_generation("seg-1", parent_generation)
        .expect("first publish");
    let err = stale
        .publish_segment_with_parent_generation("seg-2", parent_generation)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("parent manifest generation mismatch"));

    let reopened = TraceDb::open(temp.path()).expect("reopen");
    let manifest = reopened.inspect_manifest().unwrap();
    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(manifest.segments[0].segment_id, "seg-1");
}

struct HiddenStateModule;

impl TraceDbModule for HiddenStateModule {
    fn module_id(&self) -> &str {
        "hidden-state"
    }

    fn has_hidden_durable_state(&self) -> bool {
        true
    }
}

#[test]
fn reject_module_hidden_durable_state() {
    let mut registry = ModuleRegistry::default();
    let err = registry.register(Box::new(HiddenStateModule)).unwrap_err();
    assert!(err.to_string().contains("hidden durable state"));
}

#[test]
fn backup_restore_preserves_query_results() {
    let (temp, db) = seeded_db();
    let before = db.query(query()).expect("query").results;
    let backup_temp = tempfile::tempdir().unwrap();
    let backup_dir = backup_temp.path().join("backup");
    db.backup(&backup_dir).expect("backup");
    let restore_dir = temp.path().join("restore");
    TraceDb::restore(&backup_dir, &restore_dir).expect("restore");
    let restored = TraceDb::open(&restore_dir).expect("open restore");
    assert_eq!(restored.query(query()).unwrap().results, before);
}

#[test]
fn restore_requires_distinct_target() {
    let (_temp, db) = seeded_db();
    let backup_temp = tempfile::tempdir().unwrap();
    let backup_dir = backup_temp.path().join("backup");
    db.backup(&backup_dir).expect("backup");
    let err = TraceDb::restore(&backup_dir, &backup_dir).unwrap_err();
    assert!(err
        .to_string()
        .contains("source and target directories must differ"));
}

#[test]
fn sealed_segment_text_and_vector_search_participates_in_query() {
    let (_temp, mut db) = seeded_db();
    db.publish_segment("sealed-a").expect("publish segment");
    let result = db.query(query()).expect("query");
    assert!(result.explain.segments_scanned > 0);
    assert!(result.explain.text_candidates >= 2);
    assert!(result.explain.vector_candidates > 2);
}

#[test]
fn failed_insert_does_not_alter_wal_or_recovery() {
    let (temp, mut db) = seeded_db();
    let before_epoch = db.inspect_manifest().unwrap().latest_epoch;
    let before_wal_entries = db.inspect_wal().unwrap().len();
    let err = db
        .insert(RecordInput {
            table: "docs".to_string(),
            id: "bad".to_string(),
            tenant_id: "tenant-a".to_string(),
            fields: json!({
                "body": "bad vector",
                "embedding": [1.0, 0.0],
            })
            .as_object()
            .unwrap()
            .clone(),
        })
        .unwrap_err();
    assert!(err.to_string().contains("invalid vector dimensions"));
    assert_eq!(db.inspect_wal().unwrap().len(), before_wal_entries);
    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(
        recovered.inspect_manifest().unwrap().latest_epoch,
        before_epoch
    );
    assert_eq!(recovered.query(query()).unwrap().results.len(), 2);
}

#[test]
fn incompatible_vector_dimension_schema_change_is_rejected_after_committed_rows() {
    let (temp, mut db) = seeded_db();
    let before_epoch = db.inspect_manifest().unwrap().latest_epoch;
    let before_wal_entries = db.inspect_wal().unwrap().len();
    let mut incompatible = schema();
    incompatible.vector_columns[0].dimensions = 4;

    let err = db.apply_schema(incompatible).unwrap_err();
    assert!(err.to_string().contains("incompatible schema"));
    assert_eq!(db.inspect_wal().unwrap().len(), before_wal_entries);
    drop(db);

    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(
        recovered.inspect_manifest().unwrap().latest_epoch,
        before_epoch
    );
    assert_eq!(
        recovered.inspect_manifest().unwrap().schemas[0].vector_columns[0].dimensions,
        3
    );
}

#[test]
fn identity_schema_change_is_rejected_after_committed_rows() {
    let (temp, mut db) = seeded_db();
    let before_epoch = db.inspect_manifest().unwrap().latest_epoch;
    let before_wal_entries = db.inspect_wal().unwrap().len();

    let mut incompatible_primary = schema();
    incompatible_primary.primary_id_column = "uuid".to_string();
    let err = db.apply_schema(incompatible_primary).unwrap_err();
    assert!(err.to_string().contains("primary id column cannot change"));

    let mut incompatible_tenant = schema();
    incompatible_tenant.tenant_id_column = "account".to_string();
    let err = db.apply_schema(incompatible_tenant).unwrap_err();
    assert!(err.to_string().contains("tenant id column cannot change"));

    assert_eq!(db.inspect_wal().unwrap().len(), before_wal_entries);
    drop(db);

    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(
        recovered.inspect_manifest().unwrap().latest_epoch,
        before_epoch
    );
    assert_eq!(
        recovered.inspect_manifest().unwrap().schemas[0].primary_id_column,
        "id"
    );
    assert_eq!(
        recovered.inspect_manifest().unwrap().schemas[0].tenant_id_column,
        "tenant"
    );
    assert_eq!(recovered.query(query()).unwrap().results.len(), 2);
}

#[test]
fn wal_replay_recovers_schema_when_manifest_is_stale() {
    let (temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    drop(db);

    let manifest_path = temp.path().join("manifest.tdb");
    let mut manifest: TraceDbManifest =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
    manifest.schemas = Vec::new();
    manifest.latest_epoch = Epoch::new(0);
    manifest.durable_epoch = Epoch::new(0);
    manifest.checksums.manifest_checksum = 0;
    manifest.checksums.manifest_checksum = compute_manifest_checksum(&manifest).unwrap();
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let mut recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(
        recovered.inspect_manifest().unwrap().schemas[0].name,
        "docs"
    );
    recovered
        .insert(record("a", "tenant-a", "schema recovered", [1.0, 0.0, 0.0]))
        .expect("insert after schema replay");
}

#[test]
fn wal_prev_checksum_mismatch_is_corruption() {
    let (temp, db) = seeded_db();
    drop(db);
    let payload = serde_json::to_vec(&CommitRecord::empty(999, Epoch::new(999))).unwrap();
    let payload_checksum = checksum_bytes(&payload);
    let mut frame = Vec::new();
    frame.extend_from_slice(&0x5444_574cu32.to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&999u64.to_le_bytes());
    frame.extend_from_slice(&123u32.to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload_checksum.to_le_bytes());
    frame.extend_from_slice(&payload);
    std::fs::OpenOptions::new()
        .append(true)
        .open(temp.path().join("wal/000001.twal"))
        .unwrap()
        .write_all(&frame)
        .unwrap();
    let err = TraceDb::open(temp.path()).unwrap_err();
    assert!(err.to_string().contains("prev checksum mismatch"));
}

#[test]
fn wal_scan_rejects_oversized_payload_length_before_allocation() {
    let (temp, db) = seeded_db();
    let wal = Wal::open(temp.path()).unwrap();
    let entries = wal.scan().unwrap();
    let last = entries.last().unwrap();
    drop(db);

    let mut frame = Vec::new();
    frame.extend_from_slice(&0x5444_574cu32.to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&last.lsn.next().get().to_le_bytes());
    frame.extend_from_slice(&last.checksum.to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&u32::MAX.to_le_bytes());
    frame.extend_from_slice(&0u32.to_le_bytes());
    std::fs::OpenOptions::new()
        .append(true)
        .open(temp.path().join("wal/000001.twal"))
        .unwrap()
        .write_all(&frame)
        .unwrap();

    let err = TraceDb::open(temp.path()).unwrap_err();
    assert!(err.to_string().contains("payload length"));
}

#[test]
fn wal_scan_reports_and_ignores_trailing_short_payload() {
    let (temp, db) = seeded_db();
    let wal_path = temp.path().join("wal/000001.twal");
    let wal = Wal::open(temp.path()).unwrap();
    let entries = wal.scan().unwrap();
    let last = entries.last().unwrap();
    drop(db);

    let payload = serde_json::to_vec(&CommitRecord::empty(999, Epoch::new(999))).unwrap();
    let payload_checksum = checksum_bytes(&payload);
    let mut frame = Vec::new();
    frame.extend_from_slice(&0x5444_574cu32.to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&last.lsn.next().get().to_le_bytes());
    frame.extend_from_slice(&last.checksum.to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload_checksum.to_le_bytes());
    frame.extend_from_slice(&payload[..payload.len() / 2]);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(&frame)
        .unwrap();

    let scan = Wal::open(temp.path())
        .unwrap()
        .scan_with_metadata()
        .unwrap();
    assert_eq!(scan.entries.len(), entries.len());
    let torn_tail = scan.torn_tail.expect("torn tail");
    assert_eq!(torn_tail.lsn, Some(last.lsn.next()));
    assert_eq!(torn_tail.reason, "short_payload");

    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(recovered.inspect_manifest().unwrap().latest_epoch.get(), 4);
    let recovered_torn_tail = recovered
        .last_recovery_torn_tail()
        .expect("recovered torn tail");
    assert_eq!(recovered_torn_tail.lsn, Some(last.lsn.next()));
    assert_eq!(recovered_torn_tail.reason, "short_payload");
}

#[test]
fn wal_scan_ignores_checksum_valid_frame_without_commit_footer() {
    let (temp, db) = seeded_db();
    let wal_path = temp.path().join("wal/000001.twal");
    let wal = Wal::open(temp.path()).unwrap();
    let entries = wal.scan().unwrap();
    let last = entries.last().unwrap();
    drop(db);

    let mut commit = CommitRecord::empty(999, Epoch::new(999));
    commit.previous_commit_hash = last.checksum;
    let payload = serde_json::to_vec(&commit).unwrap();
    let payload_checksum = checksum_bytes(&payload);
    let mut frame = Vec::new();
    frame.extend_from_slice(&0x5444_574cu32.to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&last.lsn.next().get().to_le_bytes());
    frame.extend_from_slice(&last.checksum.to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload_checksum.to_le_bytes());
    frame.extend_from_slice(&payload);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(&frame)
        .unwrap();

    let scan = Wal::open(temp.path())
        .unwrap()
        .scan_with_metadata()
        .unwrap();
    assert_eq!(scan.entries.len(), entries.len());
    let torn_tail = scan.torn_tail.expect("torn tail");
    assert_eq!(torn_tail.lsn, Some(last.lsn.next()));
    assert_eq!(torn_tail.reason, "missing_commit_footer");

    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(recovered.inspect_manifest().unwrap().latest_epoch.get(), 4);
}

#[test]
fn feature_invalidation_is_logged_with_dirty_epoch() {
    let (_temp, mut db) = seeded_db();
    let before = db
        .feature_state("docs", "tenant-a", "a", "embedding")
        .unwrap();
    db.insert(RecordInput {
        table: "docs".to_string(),
        id: "a".to_string(),
        tenant_id: "tenant-a".to_string(),
        fields: json!({ "body": "rust changed source text" })
            .as_object()
            .unwrap()
            .clone(),
    })
    .expect("update");
    let after = db
        .feature_state("docs", "tenant-a", "a", "embedding")
        .unwrap();
    assert_eq!(after.status, FeatureStatus::Dirty);
    assert_ne!(after.source_hash, before.source_hash);
    let wal = db.inspect_wal().unwrap();
    let invalidations = &wal.last().unwrap().commit.feature_invalidations;
    assert_eq!(invalidations.len(), 1);
    assert_eq!(invalidations[0].feature, "embedding");
    assert_eq!(invalidations[0].status, FeatureStatus::Dirty);
}

#[test]
fn schema_and_insert_wal_records_module_participation() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    db.insert(record(
        "a",
        "tenant-a",
        "rust database kernel",
        [1.0, 0.0, 0.0],
    ))
    .expect("insert");

    let wal = db.inspect_wal().unwrap();
    let schema_events = &wal[0].commit.module_events;
    assert!(schema_events
        .iter()
        .any(|event| event.module_id == "tracedb-text"));
    assert!(schema_events
        .iter()
        .any(|event| event.module_id == "tracedb-vector"));

    let insert_events = &wal[1].commit.module_events;
    assert!(insert_events
        .iter()
        .any(|event| event.module_id == "tracedb-text"));
    assert!(insert_events
        .iter()
        .any(|event| event.module_id == "tracedb-vector"));
}

#[test]
fn tenant_ids_scope_record_identity() {
    let (_temp, mut db) = seeded_db();
    db.insert(record(
        "a",
        "tenant-b",
        "tenant b independent row",
        [0.0, 1.0, 0.0],
    ))
    .expect("same id in different tenant");

    let tenant_a = db.query(query()).unwrap();
    assert!(tenant_a
        .results
        .iter()
        .any(|row| row.record_id == "a" && row.tenant_id == "tenant-a"));

    let tenant_b = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-b".to_string(),
            text: Some("independent".to_string()),
            vector: Some(vec![0.0, 1.0, 0.0]),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .unwrap();
    assert_eq!(tenant_b.results[0].tenant_id, "tenant-b");
    assert_eq!(
        tenant_b.results[0].fields["body"].as_str(),
        Some("tenant b independent row")
    );
}

#[test]
fn feature_state_is_tenant_scoped_for_same_record_id() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).unwrap();
    db.insert(record("same", "tenant-a", "tenant a body", [1.0, 0.0, 0.0]))
        .unwrap();
    db.insert(record("same", "tenant-b", "tenant b body", [0.0, 1.0, 0.0]))
        .unwrap();
    let tenant_a = db
        .feature_state("docs", "tenant-a", "same", "embedding")
        .unwrap();
    let tenant_b = db
        .feature_state("docs", "tenant-b", "same", "embedding")
        .unwrap();
    assert_ne!(tenant_a.source_hash, tenant_b.source_hash);
}

#[test]
fn manifest_has_verified_checksum() {
    let (temp, db) = seeded_db();
    let checksum = db.inspect_manifest().unwrap().checksums.manifest_checksum;
    assert_ne!(checksum, 0);
    drop(db);
    let mut manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(temp.path().join("manifest.tdb")).unwrap())
            .unwrap();
    manifest["database_id"] = json!("tampered");
    std::fs::write(
        temp.path().join("manifest.tdb"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let err = TraceDb::open(temp.path()).unwrap_err();
    assert!(err.to_string().contains("manifest checksum mismatch"));
}

#[test]
fn manifest_checksum_zero_is_rejected_for_existing_manifest() {
    let (temp, db) = seeded_db();
    drop(db);
    let manifest_path = temp.path().join("manifest.tdb");
    let mut manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
    manifest["database_id"] = json!("tampered");
    manifest["checksums"]["manifest_checksum"] = json!(0);
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let err = TraceDb::open(temp.path()).unwrap_err();
    assert!(err.to_string().contains("missing manifest checksum"));
}

#[test]
fn minimum_access_paths_are_policy_aware() {
    let descriptors = tracedb_planner::minimum_access_path_descriptors();
    for expected in [
        "PolicyPath",
        "RelationalPath",
        "HotOverlayPath",
        "LexicalPath",
        "VectorPath",
    ] {
        assert!(descriptors
            .iter()
            .any(|descriptor| descriptor.access_path_id == expected && descriptor.policy_aware));
    }
}

struct RankedWithoutExplainModule;

impl TraceDbModule for RankedWithoutExplainModule {
    fn module_id(&self) -> &str {
        "ranked-without-explain"
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        vec![AccessPathDescriptor {
            access_path_id: "BadPath".to_string(),
            policy_aware: true,
        }]
    }
}

#[test]
fn reject_ranked_module_without_explain_hook() {
    let mut registry = ModuleRegistry::default();
    let err = registry
        .register(Box::new(RankedWithoutExplainModule))
        .unwrap_err();
    assert!(err.to_string().contains("explain hooks"));
}

#[test]
fn top_k_zero_returns_no_rows() {
    let (_temp, db) = seeded_db();
    let mut q = query();
    q.top_k = 0;
    let result = db.query(q).unwrap();
    assert!(result.results.is_empty());
    assert_eq!(result.explain.returned_count, 0);
}

#[test]
fn server_health_endpoint_responds() {
    let (temp, _db) = db();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"ok\":true"));
}
