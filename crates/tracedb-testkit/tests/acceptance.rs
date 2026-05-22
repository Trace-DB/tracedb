use serde_json::json;
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;
use tempfile::TempDir;
use tracedb_core::{
    checksum_bytes, compute_manifest_checksum, source_hash, Epoch, FeatureInvalidation,
    FeatureStatus, IndexState, SegmentState, TraceDbError, TraceDbManifest,
};
use tracedb_log::{CommitRecord, Wal};
use tracedb_modules::{AccessPathDescriptor, ModuleRegistry, TraceDbModule};
use tracedb_planner::QueryOutput;
use tracedb_query::{
    FreshnessMode, HybridQuery, RecordDeleteRequest, RecordGetRequest, RecordInput,
    RecordPutBatchRequest, RecordPutRequest, RecordScanRequest, TableSchema, TraceDb,
    VectorColumnSchema,
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
        text_field: None,
        text: Some("rust kernel".to_string()),
        vector_field: None,
        vector: Some(vec![1.0, 0.0, 0.0]),
        scalar_eq: Default::default(),
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
fn source_hash_uses_full_width_change_detection_bits() {
    let fields = json!({
        "body": "embedding source text that should not be reduced to a 32-bit checksum"
    })
    .as_object()
    .unwrap()
    .clone();

    let hash = source_hash(&fields, &["body".to_string()]);

    assert_ne!(
        hash >> 32,
        0,
        "source_hash should use more than CRC32-width entropy"
    );
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
fn checkpoint_recovery_does_not_require_pre_checkpoint_wal_entries() {
    let (temp, mut db) = seeded_db();
    db.delete(RecordDeleteRequest::new("docs", "tenant-a", "a").tombstone("user_delete"))
        .expect("delete before checkpoint");
    let checkpoint_epoch = db.checkpoint().expect("checkpoint");
    let manifest = db.inspect_manifest().expect("manifest");
    assert_eq!(manifest.latest_epoch, checkpoint_epoch);
    assert_eq!(manifest.checkpoint_epoch, checkpoint_epoch);
    assert!(temp
        .path()
        .join("checkpoints")
        .join(format!("checkpoint-{}.tchk", checkpoint_epoch.get()))
        .exists());
    assert_eq!(
        db.inspect_wal().expect("wal after checkpoint").len(),
        0,
        "checkpoint should truncate WAL entries covered by the durable checkpoint"
    );

    drop(db);
    let mut recovered = TraceDb::open(temp.path()).expect("recover from checkpoint");
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("get deleted")
        .is_none());
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "b"))
        .expect("get live")
        .is_some());
    let next_epoch = recovered
        .insert(record(
            "d",
            "tenant-a",
            "post checkpoint row",
            [0.8, 0.2, 0.0],
        ))
        .expect("post-checkpoint insert");
    assert_eq!(next_epoch, checkpoint_epoch.next());

    drop(recovered);
    let recovered = TraceDb::open(temp.path()).expect("recover post-checkpoint WAL");
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "d"))
        .expect("get post-checkpoint row")
        .is_some());
}

#[test]
fn checkpoint_recovery_reports_structured_corruption_for_bad_checkpoint_files() {
    let (temp, mut db) = seeded_db();
    let checkpoint_epoch = db.checkpoint().expect("checkpoint");
    let checkpoint_path = temp
        .path()
        .join("checkpoints")
        .join(format!("checkpoint-{}.tchk", checkpoint_epoch.get()));
    drop(db);

    std::fs::write(&checkpoint_path, b"TDBCHK").expect("write torn checkpoint");
    let err = TraceDb::open(temp.path()).expect_err("torn checkpoint should fail closed");
    assert!(
        matches!(err, TraceDbError::ManifestCorruption(ref message) if message.contains("checkpoint")),
        "expected checkpoint manifest corruption, got {err:?}"
    );

    let (missing_temp, mut missing_db) = seeded_db();
    let missing_epoch = missing_db.checkpoint().expect("checkpoint");
    std::fs::remove_file(
        missing_temp
            .path()
            .join("checkpoints")
            .join(format!("checkpoint-{}.tchk", missing_epoch.get())),
    )
    .expect("remove checkpoint");
    drop(missing_db);

    let err =
        TraceDb::open(missing_temp.path()).expect_err("missing checkpoint should fail closed");
    assert!(
        matches!(err, TraceDbError::ManifestCorruption(ref message) if message.contains("checkpoint")),
        "expected checkpoint manifest corruption, got {err:?}"
    );
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
            text_field: None,
            text: None,
            vector_field: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            scalar_eq: Default::default(),
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
fn hybrid_query_uses_vector_order_when_lexical_scores_are_tied() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    for (id, vector) in [
        ("early-orthogonal", [0.0, 1.0, 0.0]),
        ("early-far", [0.2, 0.98, 0.0]),
        ("target-near", [0.99, 0.01, 0.0]),
        ("target-exact", [1.0, 0.0, 0.0]),
        ("target-close", [0.98, 0.02, 0.0]),
    ] {
        db.insert(record(
            id,
            "tenant-a",
            "shared lexical topic repeated",
            vector,
        ))
        .unwrap_or_else(|error| panic!("insert {id}: {error}"));
    }

    let result = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text_field: None,
            text: Some("shared lexical topic".to_string()),
            vector_field: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            scalar_eq: Default::default(),
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
        vec!["target-exact", "target-near", "target-close"],
        "when lexical scores tie, vector similarity should decide hybrid ordering; results: {:?}",
        result.results
    );
}

#[test]
fn hybrid_query_preserves_rare_symbol_lexical_winner() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    db.insert(record(
        "vector-distractor",
        "tenant-a",
        "ordinary semantic vector document",
        [1.0, 0.0, 0.0],
    ))
    .expect("insert vector distractor");
    db.insert(record(
        "rare-symbol",
        "tenant-a",
        "ERR_VECTOR_DIMENSION_MISMATCH invalid vector dimensions",
        [0.0, 1.0, 0.0],
    ))
    .expect("insert rare symbol");

    let result = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text_field: None,
            text: Some("ERR_VECTOR_DIMENSION_MISMATCH".to_string()),
            vector_field: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 1,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query");

    assert_eq!(
        result.results.first().map(|row| row.record_id.as_str()),
        Some("rare-symbol"),
        "rare exact lexical symbol should beat a vector-only distractor; results: {:?}",
        result.results
    );
}

#[test]
fn hybrid_query_does_not_let_fallback_streams_swamp_lexical_hits() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    for idx in 0..25 {
        db.insert(record(
            &format!("early-vector-distractor-{idx:02}"),
            "tenant-a",
            "api contract ordinary helper",
            [1.0, 0.0, 0.0],
        ))
        .unwrap_or_else(|error| panic!("insert distractor {idx}: {error}"));
    }
    db.insert(record(
        "rare-lexical-hit",
        "tenant-a",
        "ultrarare api contract exact implementation",
        [0.0, 1.0, 0.0],
    ))
    .expect("insert rare lexical hit");

    let result = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text_field: None,
            text: Some("ultrarare api contract".to_string()),
            vector_field: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query");

    assert_eq!(
        result.results.first().map(|row| row.record_id.as_str()),
        Some("rare-lexical-hit"),
        "strong lexical hits should rank ahead of policy/relational/hot fallback distractors; results: {:?}",
        result.results
    );
}

#[test]
fn evidence_queries_bound_fallback_access_path_candidates() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    for idx in 0..50 {
        db.insert(record(
            &format!("evidence-doc-{idx:02}"),
            "tenant-a",
            "agent memory vector retrieval policy freshness",
            [1.0, 0.0, 0.0],
        ))
        .unwrap_or_else(|error| panic!("insert evidence doc {idx}: {error}"));
    }

    let result = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text_field: None,
            text: Some("agent memory vector".to_string()),
            vector_field: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query");

    for expected in ["PolicyPath", "RelationalPath", "HotOverlayPath"] {
        let path = result
            .explain
            .access_paths
            .iter()
            .find(|path| path.access_path_id == expected)
            .unwrap_or_else(|| panic!("{expected} path missing"));
        assert!(
            path.candidates <= result.explain.candidate_budget,
            "{expected} should be bounded by candidate_budget for evidence queries"
        );
    }
}

#[test]
fn hybrid_query_explain_reports_phase_and_access_path_timings() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    for idx in 0..25 {
        db.insert(record(
            &format!("timing-doc-{idx:02}"),
            "tenant-a",
            "agent memory vector retrieval policy freshness timing",
            [1.0, 0.0, 0.0],
        ))
        .unwrap_or_else(|error| panic!("insert timing doc {idx}: {error}"));
    }

    let result = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text_field: None,
            text: Some("agent memory vector timing".to_string()),
            vector_field: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query");

    for expected in [
        "tenant_visibility",
        "scalar_filter",
        "access_path_build",
        "access_path_open",
        "fusion",
        "materialization",
    ] {
        assert!(
            result
                .explain
                .phase_timings
                .iter()
                .any(|timing| timing.phase == expected && timing.elapsed_ms >= 0.0),
            "{expected} phase timing missing from explain: {:?}",
            result.explain.phase_timings
        );
    }

    for expected in ["LexicalPath", "VectorPath"] {
        let timing = result
            .explain
            .access_path_timings
            .iter()
            .find(|timing| timing.access_path_id == expected)
            .unwrap_or_else(|| panic!("{expected} timing missing"));
        assert!(
            timing.build_ms >= 0.0 && timing.open_ms >= 0.0,
            "{expected} timing should be non-negative: {timing:?}"
        );
    }
}

#[test]
fn query_explain_false_skips_heavy_diagnostics_without_changing_results() {
    let (_temp, db) = seeded_db();
    let verbose = db.query(query()).expect("verbose query");
    let mut lean_query = query();
    lean_query.explain = false;

    let lean = db.query(lean_query).expect("lean query");

    assert_eq!(lean.results, verbose.results);
    assert!(lean.explain.opened_candidate_streams.is_empty());
    assert!(lean.explain.access_paths.is_empty());
    assert!(lean.explain.planner_candidates.is_empty());
    assert!(lean.explain.module_versions.is_empty());
    assert!(lean.explain.phase_timings.is_empty());
    assert!(lean.explain.access_path_timings.is_empty());
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
            text_field: None,
            text: None,
            vector_field: None,
            vector: Some(vec![1.0, 0.0]),
            scalar_eq: Default::default(),
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
            text_field: None,
            text: None,
            vector_field: None,
            vector: None,
            scalar_eq: Default::default(),
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
            text_field: None,
            text: None,
            vector_field: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            scalar_eq: Default::default(),
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
fn explain_exposes_replay_minimum_candidate_facts() {
    let (_temp, db) = seeded_db();
    let q = query();
    let top_k = q.top_k;
    let result = db.query(q).expect("query");
    let explain = &result.explain;

    assert!(explain.read_epoch.get() > 0);
    assert!(explain.schema_epoch.get() > 0);
    assert!(explain.policy_epoch.get() > 0);
    assert_eq!(explain.fusion_method, "RRF");
    assert!(explain.candidate_budget >= top_k);
    assert!(explain
        .opened_candidate_streams
        .iter()
        .any(|stream| stream == "text"));
    assert!(explain
        .opened_candidate_streams
        .iter()
        .any(|stream| stream == "vector"));
    assert!(explain
        .module_versions
        .iter()
        .any(|version| version.starts_with("tracedb-text@")));
    assert!(explain
        .module_versions
        .iter()
        .any(|version| version.starts_with("tracedb-vector@")));

    assert!(!explain.access_paths.is_empty());
    for path in &explain.access_paths {
        assert!(!path.access_path_id.is_empty());
        assert!(path.opened);
        assert!(path.visibility_checked_before_open);
    }

    assert!(!explain.planner_candidates.is_empty());
    for candidate in &explain.planner_candidates {
        assert!(!candidate.record_id.is_empty());
        assert!(candidate.version_id > 0);
        assert!(!candidate.source.is_empty());
        assert!(candidate.visibility_checked);
        assert!(candidate.score_upper_bound.is_some());
        assert!(candidate.score_components.final_score.is_finite());
    }
    assert!(explain.planner_candidates.iter().any(|candidate| {
        candidate.source == "LexicalPath" && candidate.score_components.lexical.is_some()
    }));
    assert!(explain.planner_candidates.iter().any(|candidate| {
        candidate.source == "VectorPath" && candidate.score_components.vector.is_some()
    }));

    assert_eq!(
        explain.final_visibility_guard_count,
        explain.deduped_candidate_count
    );
    assert!(explain.final_visibility_guard_count >= explain.returned_count);
    assert_eq!(explain.final_visibility_guard_removed, 0);
}

#[test]
fn policy_epoch_tracks_visibility_boundary() {
    let (_temp, mut db) = db();
    let schema_epoch = db.apply_schema(schema()).expect("schema");
    assert_policy_boundary_explain(
        &db,
        db.query(policy_boundary_query())
            .expect("query after schema"),
        schema_epoch,
        "after schema",
        PolicyBoundaryExpectation::EpochOnly,
    );

    let insert_a_epoch = db
        .insert(record(
            "tenant-a-policy",
            "tenant-a",
            "policy boundary shared vector",
            [1.0, 0.0, 0.0],
        ))
        .expect("insert tenant a");
    db.insert(record(
        "tenant-b-policy",
        "tenant-b",
        "policy boundary shared vector",
        [1.0, 0.0, 0.0],
    ))
    .expect("insert tenant b");
    db.insert(record(
        "tenant-a-replaced",
        "tenant-a",
        "draft text before replacement",
        [0.0, 1.0, 0.0],
    ))
    .expect("insert replaced");
    let insert_delete_epoch = db
        .insert(record(
            "tenant-a-deleted",
            "tenant-a",
            "temporary row",
            [0.0, 0.0, 1.0],
        ))
        .expect("insert deleted");

    let after_insert = db
        .query(policy_boundary_query())
        .expect("query after insert");
    assert_policy_boundary_explain(
        &db,
        after_insert,
        insert_delete_epoch,
        "after insert",
        PolicyBoundaryExpectation::RetrievalCandidates,
    );

    let update_epoch = db
        .insert(record(
            "tenant-a-replaced",
            "tenant-a",
            "policy boundary replacement vector",
            [0.98, 0.02, 0.0],
        ))
        .expect("replace tenant a");
    let after_update = db
        .query(policy_boundary_query())
        .expect("query after update");
    assert_policy_boundary_explain(
        &db,
        after_update,
        update_epoch,
        "after update",
        PolicyBoundaryExpectation::RetrievalCandidates,
    );

    let delete_epoch = db
        .delete(RecordDeleteRequest::new(
            "docs",
            "tenant-a",
            "tenant-a-deleted",
        ))
        .expect("delete tenant a");
    let after_delete = db
        .query(policy_boundary_query())
        .expect("query after delete");
    assert_policy_boundary_explain(
        &db,
        after_delete,
        delete_epoch,
        "after delete",
        PolicyBoundaryExpectation::RetrievalCandidates,
    );

    assert!(delete_epoch > insert_a_epoch);
}

fn policy_boundary_query() -> HybridQuery {
    HybridQuery {
        table: "docs".to_string(),
        tenant_id: "tenant-a".to_string(),
        text_field: None,
        text: Some("policy boundary vector".to_string()),
        vector_field: None,
        vector: Some(vec![1.0, 0.0, 0.0]),
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 10,
        freshness: FreshnessMode::Strict,
        explain: true,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PolicyBoundaryExpectation {
    EpochOnly,
    RetrievalCandidates,
}

fn assert_policy_boundary_explain(
    db: &TraceDb,
    result: QueryOutput,
    expected_epoch: Epoch,
    context: &str,
    expectation: PolicyBoundaryExpectation,
) {
    let explain = result.explain;
    let manifest_epoch = db.inspect_manifest().expect("manifest").latest_epoch;
    assert_eq!(manifest_epoch, expected_epoch, "{context}: operation epoch");
    assert_eq!(explain.read_epoch, manifest_epoch, "{context}: read epoch");
    assert_eq!(
        explain.schema_epoch, manifest_epoch,
        "{context}: v0 exposes the manifest/latest epoch as schema_epoch"
    );
    assert_eq!(
        explain.policy_epoch, manifest_epoch,
        "{context}: v0 has no independent policy store; policy_epoch tracks manifest/latest epoch"
    );
    if expectation == PolicyBoundaryExpectation::EpochOnly {
        return;
    }

    for stream in ["text", "vector"] {
        assert!(
            explain
                .opened_candidate_streams
                .iter()
                .any(|opened| opened == stream),
            "{context}: {stream} candidate stream should open after records exist"
        );
    }
    assert!(
        result.results.iter().all(|row| row.tenant_id == "tenant-a"),
        "{context}: tenant-a query must not return tenant-b rows"
    );
    assert!(
        result
            .results
            .iter()
            .all(|row| row.record_id != "tenant-b-policy"),
        "{context}: matching tenant-b text/vector row must stay hidden"
    );

    let candidate_ids = explain
        .planner_candidates
        .iter()
        .map(|candidate| candidate.record_id.as_str())
        .collect::<BTreeSet<_>>();
    assert!(
        !candidate_ids.contains("tenant-b-policy"),
        "{context}: tenant-b-only matching row must not become a planner candidate"
    );
    assert!(
        explain
            .planner_candidates
            .iter()
            .filter(|candidate| candidate.source == "VectorPath")
            .all(|candidate| candidate.record_id != "tenant-b-policy"),
        "{context}: vector candidates must not include tenant-b-only ids"
    );
    let lexical_path_candidates = explain
        .planner_candidates
        .iter()
        .filter(|candidate| candidate.source == "LexicalPath")
        .count();
    let vector_path_candidates = explain
        .planner_candidates
        .iter()
        .filter(|candidate| candidate.source == "VectorPath")
        .count();
    assert!(
        lexical_path_candidates > 0,
        "{context}: LexicalPath should produce candidates after records exist"
    );
    assert!(
        vector_path_candidates > 0,
        "{context}: VectorPath should produce candidates after records exist"
    );
    assert_eq!(
        lexical_path_candidates, explain.text_candidates,
        "{context}: lexical candidate count should match exposed planner candidates"
    );
    assert_eq!(
        vector_path_candidates, explain.vector_candidates,
        "{context}: vector candidate count should match exposed planner candidates"
    );
    assert!(
        explain.vector_candidates <= explain.tenant_mask_visible_records,
        "{context}: vector candidate count should stay within tenant-visible records"
    );
    assert!(explain
        .planner_candidates
        .iter()
        .filter(|candidate| candidate.source == "LexicalPath" || candidate.source == "VectorPath")
        .all(|candidate| candidate.visibility_checked));
    assert!(explain
        .access_paths
        .iter()
        .filter(|path| path.access_path_id == "LexicalPath" || path.access_path_id == "VectorPath")
        .all(|path| path.visibility_checked_before_open));
    assert!(
        explain.final_visibility_guard_count >= explain.returned_count,
        "{context}: final guard must check at least returned rows"
    );
    assert_eq!(
        explain.final_visibility_guard_removed, 0,
        "{context}: normal tenant-mask-safe query should not rely on final guard removals"
    );
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
fn query_explain_counts_pending_and_failed_features() {
    let (temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    db.insert(record(
        "pending",
        "tenant-a",
        "pending lifecycle vector",
        [1.0, 0.0, 0.0],
    ))
    .expect("insert pending");
    db.insert(record(
        "failed",
        "tenant-a",
        "failed lifecycle vector",
        [1.0, 0.0, 0.0],
    ))
    .expect("insert failed");

    db.set_feature_status(
        "docs",
        "tenant-a",
        "pending",
        "embedding",
        FeatureStatus::Pending,
    )
    .expect("pending status");
    db.set_feature_status(
        "docs",
        "tenant-a",
        "failed",
        "embedding",
        FeatureStatus::Failed,
    )
    .expect("failed status");

    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    let result = recovered
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text_field: None,
            text: Some("lifecycle vector".to_string()),
            vector_field: None,
            vector: Some(vec![1.0, 0.0, 0.0]),
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query");

    let pending = result
        .results
        .iter()
        .find(|row| row.record_id == "pending")
        .expect("pending row");
    let failed = result
        .results
        .iter()
        .find(|row| row.record_id == "failed")
        .expect("failed row");

    assert!(pending.score.vector.is_none());
    assert!(failed.score.vector.is_none());
    assert_eq!(result.explain.pending_feature_count, 1);
    assert_eq!(result.explain.failed_feature_count, 1);
    assert_eq!(result.explain.dirty_feature_count, 0);
    assert_eq!(result.explain.missing_feature_count, 0);
}

#[test]
fn set_feature_status_rejects_blank_tenant_without_wal_or_feature_mutation() {
    let (_temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    db.insert(record("same", "tenant-a", "tenant a body", [1.0, 0.0, 0.0]))
        .expect("tenant a");
    db.insert(record("same", "tenant-b", "tenant b body", [0.0, 1.0, 0.0]))
        .expect("tenant b");
    let wal_len = db.inspect_wal().unwrap().len();

    for blank in ["", "   "] {
        let err = db
            .set_feature_status("docs", blank, "same", "embedding", FeatureStatus::Failed)
            .unwrap_err();
        assert!(err.to_string().contains("tenant id cannot be empty"));
        assert_eq!(db.inspect_wal().unwrap().len(), wal_len);
        assert_eq!(
            db.feature_state("docs", "tenant-a", "same", "embedding")
                .unwrap()
                .status,
            FeatureStatus::Ready
        );
        assert_eq!(
            db.feature_state("docs", "tenant-b", "same", "embedding")
                .unwrap()
                .status,
            FeatureStatus::Ready
        );
    }
}

#[test]
fn old_style_feature_invalidation_replay_scopes_to_same_commit_tenant() {
    let (temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    db.insert(record("same", "tenant-a", "tenant a body", [1.0, 0.0, 0.0]))
        .expect("tenant a");
    db.insert(record("same", "tenant-b", "tenant b body", [0.0, 1.0, 0.0]))
        .expect("tenant b");

    let epoch = db.inspect_manifest().unwrap().latest_epoch.next();
    drop(db);
    let wal = Wal::open(temp.path()).unwrap();
    let mut mutation = record("same", "tenant-a", "tenant a changed body", [1.0, 0.0, 0.0]);
    mutation.fields.remove("embedding");
    let commit = CommitRecord {
        mutations: vec![mutation],
        feature_invalidations: vec![FeatureInvalidation {
            table: "docs".to_string(),
            tenant_id: String::new(),
            record_id: "same".to_string(),
            feature: "embedding".to_string(),
            status: FeatureStatus::Dirty,
        }],
        ..CommitRecord::empty(epoch.get(), epoch)
    };
    wal.append_commit(&commit).unwrap();

    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(
        recovered
            .feature_state("docs", "tenant-a", "same", "embedding")
            .unwrap()
            .status,
        FeatureStatus::Dirty
    );
    assert_eq!(
        recovered
            .feature_state("docs", "tenant-b", "same", "embedding")
            .unwrap()
            .status,
        FeatureStatus::Ready
    );
}

#[test]
fn old_style_feature_invalidation_replay_rejects_ambiguous_active_tenants() {
    let (temp, mut db) = db();
    db.apply_schema(schema()).expect("schema");
    db.insert(record("same", "tenant-a", "tenant a body", [1.0, 0.0, 0.0]))
        .expect("tenant a");
    db.insert(record("same", "tenant-b", "tenant b body", [0.0, 1.0, 0.0]))
        .expect("tenant b");

    let epoch = db.inspect_manifest().unwrap().latest_epoch.next();
    drop(db);
    let wal = Wal::open(temp.path()).unwrap();
    let commit = CommitRecord {
        feature_invalidations: vec![FeatureInvalidation {
            table: "docs".to_string(),
            tenant_id: String::new(),
            record_id: "same".to_string(),
            feature: "embedding".to_string(),
            status: FeatureStatus::Dirty,
        }],
        ..CommitRecord::empty(epoch.get(), epoch)
    };
    wal.append_commit(&commit).unwrap();

    let err = TraceDb::open(temp.path()).unwrap_err();
    assert!(err.to_string().contains("ambiguous feature invalidation"));
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
    assert_eq!(manifest.segments[0].table_set, vec!["docs"]);
    assert_eq!(
        manifest.segments[0].tenant_set,
        vec!["tenant-a", "tenant-b"]
    );
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
fn query_skips_segment_files_when_manifest_table_set_cannot_match() {
    let (temp, mut db) = seeded_db();
    db.compact().expect("compact docs segment");
    let manifest = db.inspect_manifest().expect("manifest");
    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(manifest.segments[0].table_set, vec!["docs"]);

    let segment_path = temp
        .path()
        .join("segments")
        .join(format!("{}.tseg", manifest.segments[0].segment_id));
    std::fs::write(&segment_path, b"not a valid segment").expect("corrupt irrelevant segment");

    let mut other_schema = schema();
    other_schema.name = "other_docs".to_string();
    db.apply_schema(other_schema).expect("other schema");
    db.insert(RecordInput {
        table: "other_docs".to_string(),
        id: "other-a".to_string(),
        tenant_id: "tenant-a".to_string(),
        fields: json!({
            "id": "other-a",
            "tenant": "tenant-a",
            "conversation": "c1",
            "body": "fresh unrelated table evidence",
            "embedding": [1.0, 0.0, 0.0],
        })
        .as_object()
        .unwrap()
        .clone(),
    })
    .expect("insert other record");

    let result = db
        .query(HybridQuery {
            table: "other_docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text_field: None,
            text: Some("fresh unrelated evidence".to_string()),
            vector_field: None,
            vector: None,
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query should skip irrelevant corrupt segment");

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].record_id, "other-a");
    assert_eq!(result.explain.segments_scanned, 0);
}

#[test]
fn compact_keeps_hot_rows_authoritative_for_segment_materialization_and_visibility() {
    let (_temp, mut db) = seeded_db();

    db.compact().expect("compact segment");
    db.insert(record(
        "a",
        "tenant-a",
        "fresh hot materialized body",
        [0.0, 1.0, 0.0],
    ))
    .expect("update a after compact");

    let hot_row = db
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("get hot row")
        .expect("hot row visible");
    assert_eq!(
        hot_row.fields.get("body").and_then(|value| value.as_str()),
        Some("fresh hot materialized body")
    );

    let result = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text_field: None,
            text: Some("rust database kernel".to_string()),
            vector_field: None,
            vector: None,
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query stale segment text");

    assert!(result.explain.segments_scanned > 0);
    assert!(result
        .explain
        .planner_candidates
        .iter()
        .any(|candidate| candidate.record_id == "a" && candidate.source == "LexicalPath"));
    assert!(result
        .explain
        .planner_candidates
        .iter()
        .all(|candidate| candidate.record_id != "c"));
    assert!(result.results.iter().all(|row| row.tenant_id == "tenant-a"));
    assert!(result.results.iter().all(|row| row.record_id != "c"));

    let materialized = result
        .results
        .iter()
        .find(|row| row.record_id == "a")
        .expect("stale segment candidate should materialize visible hot row");
    assert_eq!(materialized.version_id, hot_row.version_id);
    assert_eq!(
        materialized
            .fields
            .get("body")
            .and_then(|value| value.as_str()),
        Some("fresh hot materialized body")
    );
    assert_ne!(
        materialized
            .fields
            .get("body")
            .and_then(|value| value.as_str()),
        Some("rust database kernel")
    );
    assert_eq!(result.explain.final_visibility_guard_removed, 0);
}

#[test]
fn delete_hides_record_from_hot_text_vector_feature_and_sealed_candidates() {
    let (temp, mut db) = seeded_db();

    let before_delete = db.query(query()).expect("query before delete");
    assert!(before_delete
        .results
        .iter()
        .any(|row| row.record_id.as_str() == "a"));
    assert!(before_delete.explain.text_candidates > 0);
    assert!(before_delete.explain.vector_candidates > 0);

    db.publish_segment("sealed-before-delete")
        .expect("publish segment");
    db.delete(RecordDeleteRequest::new("docs", "tenant-a", "a").tombstone("user_delete"))
        .expect("delete a");

    assert!(db
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("get deleted")
        .is_none());
    let scan = db
        .scan(RecordScanRequest::new("docs", "tenant-a").limit(10))
        .expect("scan after delete");
    assert!(scan.records.iter().all(|row| row.id.as_str() != "a"));

    let after_delete = db.query(query()).expect("query after delete");
    assert!(after_delete
        .results
        .iter()
        .all(|row| row.record_id.as_str() != "a"));
    assert!(after_delete.explain.segments_scanned > 0);
    assert!(after_delete.explain.text_candidates > 0);
    assert!(after_delete.explain.vector_candidates > 0);
    assert!(after_delete
        .explain
        .planner_candidates
        .iter()
        .any(|candidate| candidate.record_id == "a" && candidate.source == "LexicalPath"));
    assert!(after_delete
        .explain
        .planner_candidates
        .iter()
        .any(|candidate| candidate.record_id == "a" && candidate.source == "VectorPath"));
    assert!(
        after_delete.explain.final_visibility_guard_removed > 0,
        "stale sealed candidates should reach the final guard and be removed: {:?}",
        after_delete.explain
    );
    assert!(db
        .feature_state("docs", "tenant-a", "a", "embedding")
        .is_err());

    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("reopen db");
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("recovered get deleted")
        .is_none());
    let recovered_query = recovered.query(query()).expect("recovered query");
    assert!(recovered_query
        .results
        .iter()
        .all(|row| row.record_id.as_str() != "a"));
    assert!(recovered_query.explain.segments_scanned > 0);
    assert!(recovered_query.explain.final_visibility_guard_removed > 0);
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
fn failed_put_does_not_alter_memory_wal_or_recovery() {
    let (temp, mut db) = seeded_db();
    let before_epoch = db.inspect_manifest().unwrap().latest_epoch;
    let before_wal_entries = db.inspect_wal().unwrap().len();
    let before = db
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("get before")
        .expect("row before");

    let err = db
        .put(RecordPutRequest::new(RecordInput {
            table: "docs".to_string(),
            id: "a".to_string(),
            tenant_id: "tenant-a".to_string(),
            fields: json!({
                "id": "a",
                "tenant": "tenant-a",
                "body": "bad replacement should not become visible",
                "embedding": [1.0, 0.0],
            })
            .as_object()
            .unwrap()
            .clone(),
        }))
        .unwrap_err();
    assert!(err.to_string().contains("invalid vector dimensions"));
    assert_eq!(db.inspect_wal().unwrap().len(), before_wal_entries);
    assert_eq!(db.inspect_manifest().unwrap().latest_epoch, before_epoch);
    assert_eq!(
        db.get(RecordGetRequest::new("docs", "tenant-a", "a"))
            .expect("get after")
            .expect("row after")
            .fields,
        before.fields
    );

    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(
        recovered.inspect_manifest().unwrap().latest_epoch,
        before_epoch
    );
    assert_eq!(
        recovered
            .get(RecordGetRequest::new("docs", "tenant-a", "a"))
            .expect("recovered get")
            .expect("recovered row")
            .fields,
        before.fields
    );
}

#[test]
fn batch_put_invalid_record_does_not_alter_memory_wal_or_recovery() {
    let (temp, mut db) = seeded_db();
    let before_epoch = db.inspect_manifest().unwrap().latest_epoch;
    let before_wal_entries = db.inspect_wal().unwrap().len();
    let before = db
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("get before")
        .expect("row before");

    let err = db
        .put_batch(RecordPutBatchRequest::new(vec![
            record(
                "a",
                "tenant-a",
                "valid replacement should not become visible",
                [0.5, 0.5, 0.0],
            ),
            RecordInput {
                table: "docs".to_string(),
                id: "bad".to_string(),
                tenant_id: "tenant-a".to_string(),
                fields: json!({
                    "id": "bad",
                    "tenant": "tenant-a",
                    "body": "bad vector",
                    "embedding": [1.0, 0.0],
                })
                .as_object()
                .unwrap()
                .clone(),
            },
        ]))
        .unwrap_err();
    assert!(err.to_string().contains("invalid vector dimensions"));
    assert_eq!(db.inspect_wal().unwrap().len(), before_wal_entries);
    assert_eq!(db.inspect_manifest().unwrap().latest_epoch, before_epoch);
    assert_eq!(
        db.get(RecordGetRequest::new("docs", "tenant-a", "a"))
            .expect("get after")
            .expect("row after")
            .fields,
        before.fields
    );

    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(
        recovered.inspect_manifest().unwrap().latest_epoch,
        before_epoch
    );
    assert_eq!(
        recovered
            .get(RecordGetRequest::new("docs", "tenant-a", "a"))
            .expect("recovered get")
            .expect("recovered row")
            .fields,
        before.fields
    );
}

#[test]
fn put_full_replacement_wal_recovery_and_dirty_feature_state_match() {
    let (temp, mut db) = seeded_db();

    let epoch = db
        .put(RecordPutRequest::new(RecordInput {
            table: "docs".to_string(),
            id: "a".to_string(),
            tenant_id: "tenant-a".to_string(),
            fields: json!({
                "id": "a",
                "tenant": "tenant-a",
                "body": "full replacement without embedding",
            })
            .as_object()
            .unwrap()
            .clone(),
        }))
        .expect("put replacement");

    let hot = db
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("hot get")
        .expect("hot row");
    assert_eq!(
        hot.fields["body"],
        json!("full replacement without embedding")
    );
    assert!(
        !hot.fields.contains_key("conversation"),
        "put should replace the record fields rather than patching them"
    );
    assert!(
        !hot.fields.contains_key("embedding"),
        "omitted vector field should not be preserved by put replacement"
    );
    assert_eq!(
        db.feature_state("docs", "tenant-a", "a", "embedding")
            .expect("hot feature state")
            .status,
        FeatureStatus::Dirty
    );
    let wal = db.inspect_wal().expect("wal");
    let last = &wal.last().expect("last commit").commit;
    assert_eq!(last.epoch, epoch);
    assert_eq!(last.replacements.len(), 1);
    assert!(last.mutations.is_empty());
    assert_eq!(last.feature_invalidations.len(), 1);

    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    let recovered_row = recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "a"))
        .expect("recovered get")
        .expect("recovered row");
    assert_eq!(recovered_row.fields, hot.fields);
    assert_eq!(
        recovered
            .feature_state("docs", "tenant-a", "a", "embedding")
            .expect("recovered feature state")
            .status,
        FeatureStatus::Dirty
    );
}

#[test]
fn batch_put_writes_one_wal_commit_and_recovers_all_replacements() {
    let (temp, mut db) = seeded_db();
    let before_wal_entries = db.inspect_wal().unwrap().len();

    let epoch = db
        .put_batch(RecordPutBatchRequest::new(vec![
            RecordInput {
                table: "docs".to_string(),
                id: "a".to_string(),
                tenant_id: "tenant-a".to_string(),
                fields: json!({
                    "id": "a",
                    "tenant": "tenant-a",
                    "body": "batch replacement a",
                })
                .as_object()
                .unwrap()
                .clone(),
            },
            RecordInput {
                table: "docs".to_string(),
                id: "b".to_string(),
                tenant_id: "tenant-a".to_string(),
                fields: json!({
                    "id": "b",
                    "tenant": "tenant-a",
                    "body": "batch replacement b",
                })
                .as_object()
                .unwrap()
                .clone(),
            },
        ]))
        .expect("batch put");

    assert_eq!(db.inspect_manifest().unwrap().latest_epoch, epoch);
    let wal = db.inspect_wal().expect("wal");
    assert_eq!(wal.len(), before_wal_entries + 1);
    let last = &wal.last().expect("last commit").commit;
    assert_eq!(last.epoch, epoch);
    assert_eq!(last.replacements.len(), 2);
    assert!(last.mutations.is_empty());
    assert!(last.deletions.is_empty());
    assert_eq!(last.feature_invalidations.len(), 2);

    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(recovered.inspect_manifest().unwrap().latest_epoch, epoch);
    assert_eq!(
        recovered
            .get(RecordGetRequest::new("docs", "tenant-a", "a"))
            .expect("recovered a")
            .expect("row a")
            .fields["body"],
        json!("batch replacement a")
    );
    assert_eq!(
        recovered
            .get(RecordGetRequest::new("docs", "tenant-a", "b"))
            .expect("recovered b")
            .expect("row b")
            .fields["body"],
        json!("batch replacement b")
    );
    assert_eq!(
        recovered
            .feature_state("docs", "tenant-a", "a", "embedding")
            .expect("feature state a")
            .status,
        FeatureStatus::Dirty
    );
}

#[test]
fn batch_put_write_timing_reports_phase_costs_and_recovers_replacements() {
    let (temp, mut db) = seeded_db();
    let before_wal_entries = db.inspect_wal().unwrap().len();

    let (epoch, timing) = db
        .put_batch_with_write_timing(RecordPutBatchRequest::new(vec![
            record(
                "timed-a",
                "tenant-a",
                "timed batch replacement a",
                [0.2, 0.5, 0.3],
            ),
            record(
                "timed-b",
                "tenant-a",
                "timed batch replacement b",
                [0.3, 0.4, 0.3],
            ),
        ]))
        .expect("timed batch put");

    assert_eq!(db.inspect_manifest().unwrap().latest_epoch, epoch);
    assert_eq!(db.inspect_wal().unwrap().len(), before_wal_entries + 1);
    assert!(timing.total_ms >= 0.0);
    assert!(timing.store_clone_ms >= 0.0);
    assert!(timing.store_apply_ms >= 0.0);
    assert!(timing.store_apply_validate_identity_ms >= 0.0);
    assert!(timing.store_apply_validate_vector_ms >= 0.0);
    assert!(timing.store_apply_key_ms >= 0.0);
    assert!(timing.store_apply_fields_ms >= 0.0);
    assert!(timing.store_apply_finalize_identity_ms >= 0.0);
    assert!(timing.store_apply_features_ms >= 0.0);
    assert!(timing.store_apply_install_ms >= 0.0);
    let store_apply_subphase_ms = timing.store_apply_validate_identity_ms
        + timing.store_apply_validate_vector_ms
        + timing.store_apply_key_ms
        + timing.store_apply_fields_ms
        + timing.store_apply_finalize_identity_ms
        + timing.store_apply_features_ms
        + timing.store_apply_install_ms;
    assert!(store_apply_subphase_ms > 0.0);
    assert!(
        store_apply_subphase_ms <= timing.store_apply_ms + 1.0,
        "store apply subphases ({store_apply_subphase_ms}) should stay within measured store_apply_ms ({}) plus timer overhead tolerance",
        timing.store_apply_ms
    );
    assert!(timing.feature_invalidation_ms >= 0.0);
    assert!(timing.commit_build_ms >= 0.0);
    assert!(timing.wal_total_ms >= 0.0);
    assert!(timing.wal_commit_prepare_ms >= 0.0);
    assert!(timing.wal_serialize_ms >= 0.0);
    assert!(timing.wal_payload_checksum_ms >= 0.0);
    assert!(timing.wal_frame_assembly_ms >= 0.0);
    assert!(timing.wal_payload_bytes > 0);
    assert!(timing.wal_frame_bytes >= timing.wal_payload_bytes);
    assert!(timing.manifest_total_ms >= 0.0);
    assert!(timing.cache_clear_ms >= 0.0);

    drop(db);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(recovered.inspect_manifest().unwrap().latest_epoch, epoch);
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "timed-a"))
        .expect("timed a get")
        .is_some());
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "timed-b"))
        .expect("timed b get")
        .is_some());
}

#[test]
fn stale_handle_put_reconciles_committed_wal_before_epoch_allocation() {
    let (temp, mut first) = seeded_db();
    let mut stale = TraceDb::open(temp.path()).expect("stale handle");

    let first_epoch = first
        .put(RecordPutRequest::new(record(
            "fresh",
            "tenant-a",
            "fresh handle committed first",
            [0.7, 0.2, 0.1],
        )))
        .expect("fresh handle put");
    let stale_epoch = stale
        .put(RecordPutRequest::new(record(
            "stale",
            "tenant-a",
            "stale handle committed second",
            [0.6, 0.3, 0.1],
        )))
        .expect("stale handle put");

    assert_eq!(
        stale_epoch,
        first_epoch.next(),
        "stale writers must not reuse an epoch already committed by another handle"
    );
    assert!(
        stale
            .get(RecordGetRequest::new("docs", "tenant-a", "fresh"))
            .expect("stale get fresh")
            .is_some(),
        "stale writer should refresh its hot store before installing its own write"
    );

    drop(first);
    drop(stale);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(
        recovered.inspect_manifest().unwrap().latest_epoch,
        stale_epoch
    );
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "fresh"))
        .expect("recovered fresh")
        .is_some());
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "stale"))
        .expect("recovered stale")
        .is_some());
}

#[test]
fn stale_handle_batch_put_reconciles_committed_wal_before_epoch_allocation() {
    let (temp, mut first) = seeded_db();
    let mut stale = TraceDb::open(temp.path()).expect("stale handle");

    let first_epoch = first
        .put(RecordPutRequest::new(record(
            "fresh",
            "tenant-a",
            "fresh handle committed first",
            [0.7, 0.2, 0.1],
        )))
        .expect("fresh put");
    let stale_epoch = stale
        .put_batch(RecordPutBatchRequest::new(vec![
            record("stale-a", "tenant-a", "stale batch a", [0.4, 0.4, 0.2]),
            record("stale-b", "tenant-a", "stale batch b", [0.3, 0.5, 0.2]),
        ]))
        .expect("stale batch put");

    assert_eq!(
        stale_epoch,
        first_epoch.next(),
        "stale batch writers must not reuse an epoch already committed by another handle"
    );

    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(
        recovered.inspect_manifest().unwrap().latest_epoch,
        stale_epoch
    );
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "fresh"))
        .expect("fresh get")
        .is_some());
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "stale-a"))
        .expect("stale a get")
        .is_some());
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "stale-b"))
        .expect("stale b get")
        .is_some());
}

#[test]
fn stale_handle_insert_reconciles_committed_wal_before_epoch_allocation() {
    let (temp, mut first) = seeded_db();
    let mut stale = TraceDb::open(temp.path()).expect("stale handle");

    let first_epoch = first
        .insert(record(
            "fresh-insert",
            "tenant-a",
            "fresh insert committed first",
            [0.7, 0.2, 0.1],
        ))
        .expect("fresh insert");
    let stale_epoch = stale
        .insert(record(
            "stale-insert",
            "tenant-a",
            "stale insert committed second",
            [0.6, 0.3, 0.1],
        ))
        .expect("stale insert");

    assert_eq!(stale_epoch, first_epoch.next());
    assert!(stale
        .get(RecordGetRequest::new("docs", "tenant-a", "fresh-insert"))
        .expect("stale get fresh insert")
        .is_some());

    drop(first);
    drop(stale);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "fresh-insert"))
        .expect("recovered fresh insert")
        .is_some());
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "stale-insert"))
        .expect("recovered stale insert")
        .is_some());
}

#[test]
fn stale_handle_delete_reconciles_committed_wal_before_staging() {
    let (temp, mut first) = seeded_db();
    let mut stale = TraceDb::open(temp.path()).expect("stale handle");

    let first_epoch = first
        .insert(record(
            "fresh-delete",
            "tenant-a",
            "fresh delete target",
            [0.7, 0.2, 0.1],
        ))
        .expect("fresh insert");
    let stale_epoch = stale
        .delete(RecordDeleteRequest::new("docs", "tenant-a", "fresh-delete"))
        .expect("stale delete");

    assert_eq!(stale_epoch, first_epoch.next());
    assert!(stale
        .get(RecordGetRequest::new("docs", "tenant-a", "fresh-delete"))
        .expect("stale get deleted")
        .is_none());

    drop(first);
    drop(stale);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "fresh-delete"))
        .expect("recovered deleted")
        .is_none());
}

#[test]
fn stale_handle_record_write_preserves_published_segment_manifest() {
    let (temp, mut first) = seeded_db();
    let mut stale = TraceDb::open(temp.path()).expect("stale handle");
    first
        .publish_segment("seg-before-stale-write")
        .expect("publish segment");
    assert_eq!(first.inspect_manifest().unwrap().segments.len(), 1);

    stale
        .put(RecordPutRequest::new(record(
            "after-segment",
            "tenant-a",
            "record after segment publish",
            [0.6, 0.3, 0.1],
        )))
        .expect("stale put after segment publish");

    assert_eq!(
        stale.inspect_manifest().unwrap().segments.len(),
        1,
        "stale record write must preserve durable segment metadata"
    );
    drop(first);
    drop(stale);
    let recovered = TraceDb::open(temp.path()).expect("recover");
    assert_eq!(recovered.inspect_manifest().unwrap().segments.len(), 1);
    assert!(recovered
        .get(RecordGetRequest::new("docs", "tenant-a", "after-segment"))
        .expect("recovered after segment")
        .is_some());
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
            text_field: None,
            text: Some("independent".to_string()),
            vector_field: None,
            vector: Some(vec![0.0, 1.0, 0.0]),
            scalar_eq: Default::default(),
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
