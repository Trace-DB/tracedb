#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracedb_core::{
    checksum_bytes, Epoch, FeatureInvalidation, Lsn, ModuleCommitEvent, RecordDeletion,
    RecordInput, Result, TableSchema, TraceDbError,
};

const WAL_MAGIC: u32 = 0x5444_574c;
const WAL_FORMAT_VERSION: u32 = 1;
const HEADER_LEN: usize = 32;
const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;
const COMMIT_FOOTER: u32 = 0x5444_434d;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WalAppendTiming {
    pub total_ms: f64,
    pub lock_tail_ms: f64,
    pub frame_build_ms: f64,
    pub commit_prepare_ms: f64,
    pub serialize_ms: f64,
    pub payload_checksum_ms: f64,
    pub frame_assembly_ms: f64,
    pub payload_bytes: u64,
    pub frame_bytes: u64,
    pub write_ms: f64,
    pub sync_data_ms: f64,
    pub tail_update_ms: f64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub database_id: String,
    pub branch_id: String,
    pub transaction_id: u64,
    pub epoch: Epoch,
    pub parent_epoch: Epoch,
    pub previous_commit_hash: u32,
    pub idempotency_key: Option<String>,
    pub schema_epoch: Epoch,
    pub policy_epoch: Epoch,
    pub schema_changes: Vec<TableSchema>,
    #[serde(default)]
    pub replacements: Vec<RecordInput>,
    pub mutations: Vec<RecordInput>,
    #[serde(default)]
    pub deletions: Vec<RecordDeletion>,
    pub feature_invalidations: Vec<FeatureInvalidation>,
    pub module_events: Vec<ModuleCommitEvent>,
    pub commit_marker: String,
}

impl CommitRecord {
    pub fn empty(transaction_id: u64, epoch: Epoch) -> Self {
        Self {
            database_id: "local".to_string(),
            branch_id: "main".to_string(),
            transaction_id,
            epoch,
            parent_epoch: Epoch::new(epoch.get().saturating_sub(1)),
            previous_commit_hash: 0,
            idempotency_key: None,
            schema_epoch: epoch,
            policy_epoch: epoch,
            schema_changes: Vec::new(),
            replacements: Vec::new(),
            mutations: Vec::new(),
            deletions: Vec::new(),
            feature_invalidations: Vec::new(),
            module_events: Vec::new(),
            commit_marker: "COMMITTED".to_string(),
        }
    }

    pub fn for_database(
        mut self,
        database_id: impl Into<String>,
        branch_id: impl Into<String>,
    ) -> Self {
        self.database_id = database_id.into();
        self.branch_id = branch_id.into();
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalEntry {
    pub lsn: Lsn,
    pub checksum: u32,
    pub commit: CommitRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TornWalTail {
    pub offset: u64,
    pub lsn: Option<Lsn>,
    pub reason: String,
    pub expected_len: usize,
    pub actual_len: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalScan {
    pub entries: Vec<WalEntry>,
    pub torn_tail: Option<TornWalTail>,
}

#[derive(Clone, Debug)]
pub struct Wal {
    path: PathBuf,
    tail: Arc<Mutex<WalTail>>,
}

#[derive(Clone, Debug, Default)]
struct WalTail {
    last_lsn: Option<Lsn>,
    last_epoch: Option<Epoch>,
    last_checksum: u32,
    file_len: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct CommitFrame {
    epoch: Epoch,
    payload_checksum: u32,
    payload_bytes: u64,
    frame_bytes: u64,
    frame: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct CommitFrameBuildTiming {
    frame_build_ms: f64,
    commit_prepare_ms: f64,
    serialize_ms: f64,
    payload_checksum_ms: f64,
    frame_assembly_ms: f64,
    payload_bytes: u64,
    frame_bytes: u64,
}

#[derive(Serialize)]
struct CommitRecordForFrame<'a> {
    database_id: &'a str,
    branch_id: &'a str,
    transaction_id: u64,
    epoch: Epoch,
    parent_epoch: Epoch,
    previous_commit_hash: u32,
    idempotency_key: &'a Option<String>,
    schema_epoch: Epoch,
    policy_epoch: Epoch,
    schema_changes: &'a [TableSchema],
    replacements: &'a [RecordInput],
    mutations: &'a [RecordInput],
    deletions: &'a [RecordDeletion],
    feature_invalidations: &'a [FeatureInvalidation],
    module_events: &'a [ModuleCommitEvent],
    commit_marker: &'a str,
}

impl Wal {
    pub fn open(db_dir: impl AsRef<Path>) -> Result<Self> {
        let wal_dir = db_dir.as_ref().join("wal");
        fs::create_dir_all(&wal_dir)?;
        let path = wal_dir.join("000001.twal");
        if !path.exists() {
            File::create(&path)?;
        }
        let scan = scan_file(&path)?;
        let tail = tail_from_scan(&path, &scan)?;
        Ok(Self {
            path,
            tail: Arc::new(Mutex::new(tail)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn last_commit_epoch(&self) -> Result<Option<Epoch>> {
        let mut tail = self
            .tail
            .lock()
            .map_err(|_| TraceDbError::WalCorruption("wal tail cache lock poisoned".to_string()))?;
        let file_len = fs::metadata(&self.path)?.len();
        if file_len != tail.file_len {
            let scan = scan_file(&self.path)?;
            *tail = tail_from_scan(&self.path, &scan)?;
        }
        Ok(tail.last_epoch)
    }

    pub fn append_commit(&self, commit: &CommitRecord) -> Result<Lsn> {
        let _guard = WalWriteLock::acquire(&self.path)?;
        let mut tail = self
            .tail
            .lock()
            .map_err(|_| TraceDbError::WalCorruption("wal tail cache lock poisoned".to_string()))?;
        let file_len = fs::metadata(&self.path)?.len();
        if file_len != tail.file_len {
            let scan = scan_file(&self.path)?;
            *tail = tail_from_scan(&self.path, &scan)?;
        }
        let lsn = tail
            .last_lsn
            .map(|last_lsn| last_lsn.next())
            .unwrap_or_else(|| Lsn::new(1));
        let prev_checksum = tail.last_checksum;
        let frame = build_commit_frame(commit, lsn, prev_checksum)?;

        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&frame.frame)?;
        file.sync_data()?;
        tail.last_lsn = Some(lsn);
        tail.last_epoch = Some(frame.epoch);
        tail.last_checksum = frame.payload_checksum;
        tail.file_len += frame.frame_bytes;
        Ok(lsn)
    }

    pub fn append_commit_with_timing(
        &self,
        commit: &CommitRecord,
    ) -> Result<(Lsn, WalAppendTiming)> {
        let total_started = Instant::now();
        let lock_tail_started = Instant::now();
        let _guard = WalWriteLock::acquire(&self.path)?;
        let mut tail = self
            .tail
            .lock()
            .map_err(|_| TraceDbError::WalCorruption("wal tail cache lock poisoned".to_string()))?;
        let file_len = fs::metadata(&self.path)?.len();
        if file_len != tail.file_len {
            let scan = scan_file(&self.path)?;
            *tail = tail_from_scan(&self.path, &scan)?;
        }
        let lsn = tail
            .last_lsn
            .map(|last_lsn| last_lsn.next())
            .unwrap_or_else(|| Lsn::new(1));
        let prev_checksum = tail.last_checksum;
        let lock_tail_ms = elapsed_ms(lock_tail_started);

        let (frame, frame_timing) = build_commit_frame_with_timing(commit, lsn, prev_checksum)?;

        let write_started = Instant::now();
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&frame.frame)?;
        let write_ms = elapsed_ms(write_started);
        let sync_data_started = Instant::now();
        file.sync_data()?;
        let sync_data_ms = elapsed_ms(sync_data_started);
        let tail_update_started = Instant::now();
        tail.last_lsn = Some(lsn);
        tail.last_epoch = Some(frame.epoch);
        tail.last_checksum = frame.payload_checksum;
        tail.file_len += frame.frame_bytes;
        let tail_update_ms = elapsed_ms(tail_update_started);
        Ok((
            lsn,
            WalAppendTiming {
                total_ms: elapsed_ms(total_started),
                lock_tail_ms,
                frame_build_ms: frame_timing.frame_build_ms,
                commit_prepare_ms: frame_timing.commit_prepare_ms,
                serialize_ms: frame_timing.serialize_ms,
                payload_checksum_ms: frame_timing.payload_checksum_ms,
                frame_assembly_ms: frame_timing.frame_assembly_ms,
                payload_bytes: frame_timing.payload_bytes,
                frame_bytes: frame_timing.frame_bytes,
                write_ms,
                sync_data_ms,
                tail_update_ms,
            },
        ))
    }

    pub fn scan(&self) -> Result<Vec<WalEntry>> {
        Ok(self.scan_with_metadata()?.entries)
    }

    pub fn scan_with_metadata(&self) -> Result<WalScan> {
        scan_file(&self.path)
    }
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn build_commit_frame(
    commit: &CommitRecord,
    lsn: Lsn,
    previous_checksum: u32,
) -> Result<CommitFrame> {
    let payload = serialize_commit_payload_for_frame(commit, previous_checksum, lsn)?;
    let payload_checksum = checksum_bytes(&payload);
    let frame = assemble_commit_frame(lsn, previous_checksum, payload_checksum, &payload);
    let payload_bytes = payload.len() as u64;
    let frame_bytes = frame.len() as u64;
    Ok(CommitFrame {
        epoch: commit.epoch,
        payload_checksum,
        payload_bytes,
        frame_bytes,
        frame,
    })
}

fn build_commit_frame_with_timing(
    commit: &CommitRecord,
    lsn: Lsn,
    previous_checksum: u32,
) -> Result<(CommitFrame, CommitFrameBuildTiming)> {
    let frame_build_started = Instant::now();
    let commit_prepare_started = Instant::now();
    let commit_for_frame = prepare_commit_for_frame(commit, previous_checksum);
    let commit_prepare_ms = elapsed_ms(commit_prepare_started);

    let serialize_started = Instant::now();
    let payload = serialize_prepared_commit_payload(&commit_for_frame, lsn)?;
    let serialize_ms = elapsed_ms(serialize_started);

    let payload_checksum_started = Instant::now();
    let payload_checksum = checksum_bytes(&payload);
    let payload_checksum_ms = elapsed_ms(payload_checksum_started);

    let frame_assembly_started = Instant::now();
    let frame = assemble_commit_frame(lsn, previous_checksum, payload_checksum, &payload);
    let payload_bytes = payload.len() as u64;
    let frame_bytes = frame.len() as u64;
    let frame_assembly_ms = elapsed_ms(frame_assembly_started);
    let frame_build_ms = elapsed_ms(frame_build_started);

    Ok((
        CommitFrame {
            epoch: commit.epoch,
            payload_checksum,
            payload_bytes,
            frame_bytes,
            frame,
        },
        CommitFrameBuildTiming {
            frame_build_ms,
            commit_prepare_ms,
            serialize_ms,
            payload_checksum_ms,
            frame_assembly_ms,
            payload_bytes,
            frame_bytes,
        },
    ))
}

fn prepare_commit_for_frame<'a>(
    commit: &'a CommitRecord,
    previous_checksum: u32,
) -> CommitRecordForFrame<'a> {
    CommitRecordForFrame {
        database_id: &commit.database_id,
        branch_id: &commit.branch_id,
        transaction_id: commit.transaction_id,
        epoch: commit.epoch,
        parent_epoch: commit.parent_epoch,
        previous_commit_hash: previous_checksum,
        idempotency_key: &commit.idempotency_key,
        schema_epoch: commit.schema_epoch,
        policy_epoch: commit.policy_epoch,
        schema_changes: &commit.schema_changes,
        replacements: &commit.replacements,
        mutations: &commit.mutations,
        deletions: &commit.deletions,
        feature_invalidations: &commit.feature_invalidations,
        module_events: &commit.module_events,
        commit_marker: &commit.commit_marker,
    }
}

#[cfg(test)]
fn serialize_commit_payload(commit: &CommitRecord, lsn: Lsn) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(commit)?;
    check_payload_len(&payload, lsn)?;
    Ok(payload)
}

fn serialize_commit_payload_for_frame(
    commit: &CommitRecord,
    previous_checksum: u32,
    lsn: Lsn,
) -> Result<Vec<u8>> {
    let commit = prepare_commit_for_frame(commit, previous_checksum);
    serialize_prepared_commit_payload(&commit, lsn)
}

fn serialize_prepared_commit_payload(
    commit: &CommitRecordForFrame<'_>,
    lsn: Lsn,
) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(commit)?;
    check_payload_len(&payload, lsn)?;
    Ok(payload)
}

fn check_payload_len(payload: &[u8], lsn: Lsn) -> Result<()> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(TraceDbError::WalCorruption(format!(
            "payload length {} exceeds max {MAX_PAYLOAD_LEN} at lsn {}",
            payload.len(),
            lsn.get()
        )));
    }
    Ok(())
}

fn assemble_commit_frame(
    lsn: Lsn,
    previous_checksum: u32,
    payload_checksum: u32,
    payload: &[u8],
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    frame.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    frame.extend_from_slice(&lsn.get().to_le_bytes());
    frame.extend_from_slice(&previous_checksum.to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload_checksum.to_le_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&COMMIT_FOOTER.to_le_bytes());
    frame
}

fn tail_from_scan(path: &Path, scan: &WalScan) -> Result<WalTail> {
    let last = scan.entries.last();
    Ok(WalTail {
        last_lsn: last.map(|entry| entry.lsn),
        last_epoch: last.map(|entry| entry.commit.epoch),
        last_checksum: last.map(|entry| entry.checksum).unwrap_or_default(),
        file_len: fs::metadata(path)?.len(),
    })
}

fn scan_file(path: &Path) -> Result<WalScan> {
    let mut file = File::open(path)?;
    let mut entries: Vec<WalEntry> = Vec::new();
    let mut offset = 0u64;

    loop {
        let mut header = [0u8; HEADER_LEN];
        let read = read_some(&mut file, &mut header)?;
        if read == 0 {
            break;
        }
        if read < HEADER_LEN {
            return Ok(WalScan {
                entries,
                torn_tail: Some(TornWalTail {
                    offset,
                    lsn: None,
                    reason: "short_header".to_string(),
                    expected_len: HEADER_LEN,
                    actual_len: read,
                }),
            });
        }
        offset += HEADER_LEN as u64;

        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let lsn = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let prev_checksum = u32::from_le_bytes(header[16..20].try_into().unwrap());
        let kind = u32::from_le_bytes(header[20..24].try_into().unwrap());
        let payload_len = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
        let payload_checksum = u32::from_le_bytes(header[28..32].try_into().unwrap());

        if magic != WAL_MAGIC {
            return Err(TraceDbError::WalCorruption("invalid magic".to_string()));
        }
        if version != WAL_FORMAT_VERSION {
            return Err(TraceDbError::WalCorruption(format!(
                "unsupported wal version {version}"
            )));
        }
        if kind != 1 {
            return Err(TraceDbError::WalCorruption(format!(
                "unsupported frame kind {kind}"
            )));
        }
        let expected_prev = entries
            .last()
            .map(|entry| entry.checksum)
            .unwrap_or_default();
        if prev_checksum != expected_prev {
            return Err(TraceDbError::WalCorruption(format!(
                "prev checksum mismatch at lsn {lsn}"
            )));
        }
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(TraceDbError::WalCorruption(format!(
                "payload length {payload_len} exceeds max {MAX_PAYLOAD_LEN} at lsn {lsn}"
            )));
        }

        let mut payload = vec![0u8; payload_len];
        let read = read_some(&mut file, &mut payload)?;
        if read < payload_len {
            return Ok(WalScan {
                entries,
                torn_tail: Some(TornWalTail {
                    offset,
                    lsn: Some(Lsn::new(lsn)),
                    reason: "short_payload".to_string(),
                    expected_len: payload_len + std::mem::size_of::<u32>(),
                    actual_len: read,
                }),
            });
        }
        offset += payload_len as u64;
        let actual_checksum = checksum_bytes(&payload);
        if actual_checksum != payload_checksum {
            return Err(TraceDbError::WalCorruption(format!(
                "payload checksum mismatch at lsn {lsn}"
            )));
        }
        let mut footer = [0u8; std::mem::size_of::<u32>()];
        let read = read_some(&mut file, &mut footer)?;
        if read < footer.len() {
            return Ok(WalScan {
                entries,
                torn_tail: Some(TornWalTail {
                    offset,
                    lsn: Some(Lsn::new(lsn)),
                    reason: "missing_commit_footer".to_string(),
                    expected_len: footer.len(),
                    actual_len: read,
                }),
            });
        }
        offset += footer.len() as u64;
        let footer = u32::from_le_bytes(footer);
        if footer != COMMIT_FOOTER {
            return Err(TraceDbError::WalCorruption(format!(
                "commit footer mismatch at lsn {lsn}"
            )));
        }
        let commit: CommitRecord = serde_json::from_slice(&payload)?;
        if commit.commit_marker != "COMMITTED" {
            return Err(TraceDbError::WalCorruption(format!(
                "missing commit marker at lsn {lsn}"
            )));
        }
        if commit.epoch.get() == 0 || commit.parent_epoch >= commit.epoch {
            return Err(TraceDbError::WalCorruption(format!(
                "invalid parent epoch {} for epoch {} at lsn {lsn}",
                commit.parent_epoch.get(),
                commit.epoch.get()
            )));
        }
        if commit.previous_commit_hash != prev_checksum {
            return Err(TraceDbError::WalCorruption(format!(
                "previous commit hash mismatch at lsn {lsn}"
            )));
        }
        entries.push(WalEntry {
            lsn: Lsn::new(lsn),
            checksum: payload_checksum,
            commit,
        });
    }

    Ok(WalScan {
        entries,
        torn_tail: None,
    })
}

struct WalWriteLock {
    path: PathBuf,
}

impl WalWriteLock {
    fn acquire(wal_path: &Path) -> Result<Self> {
        let path = wal_path.with_extension("twal.lock");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(std::process::id().to_string().as_bytes())?;
                    file.sync_all()?;
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if Instant::now() >= deadline {
                        return Err(TraceDbError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("timed out waiting for WAL lock {}", path.display()),
                        )));
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(TraceDbError::Io(error)),
            }
        }
    }
}

impl Drop for WalWriteLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn read_some(file: &mut File, buf: &mut [u8]) -> Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        let read = file.read(&mut buf[total..])?;
        if read == 0 {
            break;
        }
        total += read;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timed_and_untimed_frame_builders_produce_identical_frame_bytes() {
        let commit = CommitRecord::empty(7, Epoch::new(3)).for_database("db", "main");
        let lsn = Lsn::new(11);
        let previous_checksum = 0x1234_5678;

        let untimed = build_commit_frame(&commit, lsn, previous_checksum).expect("untimed frame");
        let (timed, timing) =
            build_commit_frame_with_timing(&commit, lsn, previous_checksum).expect("timed frame");

        assert_eq!(untimed.frame, timed.frame);
        assert_eq!(untimed.payload_checksum, timed.payload_checksum);
        assert_eq!(untimed.epoch, timed.epoch);
        assert_eq!(timing.payload_bytes, untimed.payload_bytes);
        assert_eq!(timing.frame_bytes, untimed.frame_bytes);
    }

    #[test]
    fn timed_and_untimed_append_paths_write_identical_wal_bytes() {
        let untimed_dir = tempfile::tempdir().expect("untimed tempdir");
        let timed_dir = tempfile::tempdir().expect("timed tempdir");
        let untimed = Wal::open(untimed_dir.path()).expect("untimed wal");
        let timed = Wal::open(timed_dir.path()).expect("timed wal");
        let commits = [
            CommitRecord::empty(1, Epoch::new(1)).for_database("db", "main"),
            CommitRecord::empty(2, Epoch::new(2)).for_database("db", "main"),
            CommitRecord::empty(3, Epoch::new(3)).for_database("db", "main"),
        ];

        let mut prior_len = 0u64;
        for commit in &commits {
            let untimed_lsn = untimed.append_commit(commit).expect("untimed append");
            let (timed_lsn, timing) = timed
                .append_commit_with_timing(commit)
                .expect("timed append");
            assert_eq!(untimed_lsn, timed_lsn);
            let current_len = std::fs::metadata(timed.path())
                .expect("timed metadata")
                .len();
            assert_eq!(timing.frame_bytes, current_len - prior_len);
            prior_len = current_len;
        }

        let untimed_bytes = std::fs::read(untimed.path()).expect("untimed bytes");
        let timed_bytes = std::fs::read(timed.path()).expect("timed bytes");
        assert_eq!(untimed_bytes, timed_bytes);

        let untimed_entries = untimed.scan().expect("untimed scan");
        let timed_entries = timed.scan().expect("timed scan");
        assert_eq!(untimed_entries, timed_entries);
        for pair in timed_entries.windows(2) {
            assert_eq!(pair[1].commit.previous_commit_hash, pair[0].checksum);
        }
    }

    #[test]
    fn borrowed_commit_payload_matches_cloned_commit_record_bytes() {
        let mut fields = serde_json::Map::new();
        fields.insert("title".to_string(), serde_json::json!("alpha"));
        fields.insert(
            "embedding".to_string(),
            serde_json::json!([0.1_f32, 0.2_f32, 0.3_f32]),
        );
        let commit = CommitRecord {
            database_id: "db".to_string(),
            branch_id: "main".to_string(),
            transaction_id: 42,
            epoch: Epoch::new(9),
            parent_epoch: Epoch::new(8),
            previous_commit_hash: 0,
            idempotency_key: Some("idem-42".to_string()),
            schema_epoch: Epoch::new(6),
            policy_epoch: Epoch::new(7),
            schema_changes: vec![TableSchema {
                name: "docs".to_string(),
                primary_id_column: "id".to_string(),
                tenant_id_column: "tenant_id".to_string(),
                scalar_columns: vec!["title".to_string()],
                text_indexed_columns: vec!["body".to_string()],
                vector_columns: Vec::new(),
            }],
            replacements: vec![RecordInput {
                table: "docs".to_string(),
                id: "a".to_string(),
                tenant_id: "tenant-a".to_string(),
                fields: fields.clone(),
            }],
            mutations: vec![RecordInput {
                table: "docs".to_string(),
                id: "b".to_string(),
                tenant_id: "tenant-a".to_string(),
                fields,
            }],
            deletions: vec![RecordDeletion::new("docs", "tenant-a", "old", "replace")],
            feature_invalidations: vec![FeatureInvalidation {
                table: "docs".to_string(),
                tenant_id: "tenant-a".to_string(),
                record_id: "a".to_string(),
                feature: "embedding".to_string(),
                status: tracedb_core::FeatureStatus::Dirty,
            }],
            module_events: vec![ModuleCommitEvent {
                module_id: "text".to_string(),
                event: "indexed".to_string(),
            }],
            commit_marker: "COMMITTED".to_string(),
        };
        let previous_checksum = 0xCAFE_BABE;
        let lsn = Lsn::new(4);
        let mut cloned = commit.clone();
        cloned.previous_commit_hash = previous_checksum;
        let cloned_payload = serialize_commit_payload(&cloned, lsn).expect("cloned payload");

        let borrowed_payload = serialize_commit_payload_for_frame(&commit, previous_checksum, lsn)
            .expect("borrowed payload");

        assert_eq!(borrowed_payload, cloned_payload);
    }
}
