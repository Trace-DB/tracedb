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
        let mut commit = commit.clone();
        commit.previous_commit_hash = prev_checksum;
        let payload = serde_json::to_vec(&commit)?;
        if payload.len() > MAX_PAYLOAD_LEN {
            return Err(TraceDbError::WalCorruption(format!(
                "payload length {} exceeds max {MAX_PAYLOAD_LEN} at lsn {}",
                payload.len(),
                lsn.get()
            )));
        }
        let payload_checksum = checksum_bytes(&payload);

        let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
        frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        frame.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        frame.extend_from_slice(&lsn.get().to_le_bytes());
        frame.extend_from_slice(&prev_checksum.to_le_bytes());
        frame.extend_from_slice(&1u32.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload_checksum.to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&COMMIT_FOOTER.to_le_bytes());

        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&frame)?;
        file.sync_data()?;
        tail.last_lsn = Some(lsn);
        tail.last_epoch = Some(commit.epoch);
        tail.last_checksum = payload_checksum;
        tail.file_len += frame.len() as u64;
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

        let frame_build_started = Instant::now();
        let mut commit = commit.clone();
        commit.previous_commit_hash = prev_checksum;
        let payload = serde_json::to_vec(&commit)?;
        if payload.len() > MAX_PAYLOAD_LEN {
            return Err(TraceDbError::WalCorruption(format!(
                "payload length {} exceeds max {MAX_PAYLOAD_LEN} at lsn {}",
                payload.len(),
                lsn.get()
            )));
        }
        let payload_checksum = checksum_bytes(&payload);

        let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
        frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        frame.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        frame.extend_from_slice(&lsn.get().to_le_bytes());
        frame.extend_from_slice(&prev_checksum.to_le_bytes());
        frame.extend_from_slice(&1u32.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload_checksum.to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&COMMIT_FOOTER.to_le_bytes());
        let frame_build_ms = elapsed_ms(frame_build_started);

        let write_started = Instant::now();
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&frame)?;
        let write_ms = elapsed_ms(write_started);
        let sync_data_started = Instant::now();
        file.sync_data()?;
        let sync_data_ms = elapsed_ms(sync_data_started);
        let tail_update_started = Instant::now();
        tail.last_lsn = Some(lsn);
        tail.last_epoch = Some(commit.epoch);
        tail.last_checksum = payload_checksum;
        tail.file_len += frame.len() as u64;
        let tail_update_ms = elapsed_ms(tail_update_started);
        Ok((
            lsn,
            WalAppendTiming {
                total_ms: elapsed_ms(total_started),
                lock_tail_ms,
                frame_build_ms,
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
