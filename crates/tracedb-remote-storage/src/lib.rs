#![forbid(unsafe_code)]
//! Provider-neutral remote storage interfaces for hosted and self-hosted TraceDB.
//!
//! This crate belongs to the public Apache runtime. TraceDB Cloud may orchestrate
//! these interfaces, but it must not own private-only storage semantics.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use thiserror::Error;

pub type StorageFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorageError>> + Send + 'a>>;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found: {key}")]
    ObjectNotFound { key: String },
    #[error("manifest compare-and-swap failed for {database_id}/{branch_id}")]
    ManifestCasFailed {
        database_id: String,
        branch_id: String,
    },
    #[error("lease is held by {holder}")]
    LeaseHeld { holder: String },
    #[error("invalid storage configuration: {0}")]
    InvalidConfig(String),
    #[error("provider error: {0}")]
    Provider(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectKey {
    pub database_id: String,
    pub branch_id: String,
    pub kind: ObjectKind,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ObjectKind {
    WalChunk,
    Checkpoint,
    ManifestPointer,
    Lease,
    Segment,
    Snapshot,
    Export,
}

impl ObjectKey {
    pub fn path(&self) -> String {
        format!(
            "databases/{}/branches/{}/{}/{}",
            safe_component(&self.database_id),
            safe_component(&self.branch_id),
            self.kind.as_path(),
            safe_component(&self.name),
        )
    }
}

impl ObjectKind {
    fn as_path(&self) -> &'static str {
        match self {
            ObjectKind::WalChunk => "wal",
            ObjectKind::Checkpoint => "checkpoints",
            ObjectKind::ManifestPointer => "manifests",
            ObjectKind::Lease => "leases",
            ObjectKind::Segment => "segments",
            ObjectKind::Snapshot => "snapshots",
            ObjectKind::Export => "exports",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PutObjectRequest {
    pub key: ObjectKey,
    pub bytes: Vec<u8>,
    pub checksum_sha256: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub key: ObjectKey,
    pub bytes: Vec<u8>,
    pub checksum_sha256: String,
    pub metadata: BTreeMap<String, String>,
}

pub trait ObjectStore: Send + Sync {
    fn put_object(&self, request: PutObjectRequest) -> StorageFuture<'_, ()>;
    fn get_object(&self, key: ObjectKey) -> StorageFuture<'_, ObjectRecord>;
    fn delete_object(&self, key: ObjectKey) -> StorageFuture<'_, ()>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchManifestPointer {
    pub database_id: String,
    pub branch_id: String,
    pub generation: u64,
    pub latest_epoch: u64,
    pub durable_epoch: u64,
    pub object_key: ObjectKey,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestCasRequest {
    pub expected_generation: Option<u64>,
    pub next: BranchManifestPointer,
}

pub trait ManifestStore: Send + Sync {
    fn load_manifest_pointer(
        &self,
        database_id: String,
        branch_id: String,
    ) -> StorageFuture<'_, Option<BranchManifestPointer>>;

    fn compare_and_swap_manifest(&self, request: ManifestCasRequest) -> StorageFuture<'_, ()>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub database_id: String,
    pub branch_id: String,
    pub holder_id: String,
    pub fencing_token: u64,
    pub expires_at_unix: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AcquireLeaseRequest {
    pub database_id: String,
    pub branch_id: String,
    pub holder_id: String,
    pub ttl_ms: u64,
}

pub trait LeaseStore: Send + Sync {
    fn acquire_lease(&self, request: AcquireLeaseRequest) -> StorageFuture<'_, LeaseRecord>;
    fn refresh_lease(&self, lease: LeaseRecord, ttl_ms: u64) -> StorageFuture<'_, LeaseRecord>;
    fn release_lease(&self, lease: LeaseRecord) -> StorageFuture<'_, ()>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetricEvent {
    pub name: String,
    pub unit: String,
    pub value: u64,
    pub dimensions: BTreeMap<String, String>,
}

pub trait MetricsSink: Send + Sync {
    fn record_metric(&self, event: MetricEvent) -> StorageFuture<'_, ()>;
}

pub trait SecretLoader: Send + Sync {
    fn load_secret(&self, name: String) -> StorageFuture<'_, Option<String>>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenClaims {
    pub token_identity: String,
    pub database_id: Option<String>,
    pub branch_id: Option<String>,
    pub tenant_id: Option<String>,
    pub scopes: Vec<String>,
}

pub trait TokenVerifier: Send + Sync {
    fn verify_token(&self, token: String) -> StorageFuture<'_, Option<TokenClaims>>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3CompatibleStorageProfile {
    pub region: String,
    pub bucket: String,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub object_prefix: String,
}

pub type AwsStorageProfile = S3CompatibleStorageProfile;

impl S3CompatibleStorageProfile {
    pub fn object_path(&self, key: &ObjectKey) -> String {
        let prefix = self.object_prefix.trim_matches('/');
        if prefix.is_empty() {
            key.path()
        } else {
            format!("{prefix}/{}", key.path())
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PutCondition {
    None,
    IfAbsent,
    IfMatchEtag(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3PutObject {
    pub bucket: String,
    pub key: String,
    pub bytes: Vec<u8>,
    pub checksum_sha256: String,
    pub metadata: BTreeMap<String, String>,
    pub condition: PutCondition,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3GetObject {
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3HeadObject {
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3ObjectHead {
    pub etag: String,
    pub checksum_sha256: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3DeleteObject {
    pub bucket: String,
    pub key: String,
    pub if_match_etag: Option<String>,
}

pub trait S3CompatibleStorageRuntime: Send + Sync {
    fn s3_put_object(&self, command: S3PutObject) -> StorageFuture<'_, ()>;
    fn s3_get_object(&self, command: S3GetObject) -> StorageFuture<'_, ObjectRecord>;
    fn s3_head_object(&self, command: S3HeadObject) -> StorageFuture<'_, Option<S3ObjectHead>>;
    fn s3_delete_object(&self, command: S3DeleteObject) -> StorageFuture<'_, ()>;
}

pub trait AwsStorageRuntime: S3CompatibleStorageRuntime {}

impl<T: S3CompatibleStorageRuntime + ?Sized> AwsStorageRuntime for T {}

#[derive(Clone, Debug)]
pub struct AwsReferenceStorage<R> {
    profile: AwsStorageProfile,
    runtime: R,
}

impl<R> AwsReferenceStorage<R> {
    pub fn new(profile: AwsStorageProfile, runtime: R) -> Result<Self, StorageError> {
        if profile.bucket.trim().is_empty() {
            return Err(StorageError::InvalidConfig("S3 bucket is required".into()));
        }
        Ok(Self { profile, runtime })
    }

    pub fn profile(&self) -> &AwsStorageProfile {
        &self.profile
    }
}

impl<R: AwsStorageRuntime> ObjectStore for AwsReferenceStorage<R> {
    fn put_object(&self, request: PutObjectRequest) -> StorageFuture<'_, ()> {
        self.runtime.s3_put_object(S3PutObject {
            bucket: self.profile.bucket.clone(),
            key: self.profile.object_path(&request.key),
            bytes: request.bytes,
            checksum_sha256: request.checksum_sha256,
            metadata: request.metadata,
            condition: PutCondition::None,
        })
    }

    fn get_object(&self, key: ObjectKey) -> StorageFuture<'_, ObjectRecord> {
        self.runtime.s3_get_object(S3GetObject {
            bucket: self.profile.bucket.clone(),
            key: self.profile.object_path(&key),
        })
    }

    fn delete_object(&self, key: ObjectKey) -> StorageFuture<'_, ()> {
        self.runtime.s3_delete_object(S3DeleteObject {
            bucket: self.profile.bucket.clone(),
            key: self.profile.object_path(&key),
            if_match_etag: None,
        })
    }
}

impl<R: AwsStorageRuntime> ManifestStore for AwsReferenceStorage<R> {
    fn load_manifest_pointer(
        &self,
        database_id: String,
        branch_id: String,
    ) -> StorageFuture<'_, Option<BranchManifestPointer>> {
        let key = manifest_pointer_key(database_id, branch_id);
        let bucket = self.profile.bucket.clone();
        let path = self.profile.object_path(&key);
        let runtime = &self.runtime;
        Box::pin(async move {
            match runtime
                .s3_get_object(S3GetObject { bucket, key: path })
                .await
            {
                Ok(record) => serde_json::from_slice(&record.bytes)
                    .map(Some)
                    .map_err(|error| StorageError::Provider(error.to_string())),
                Err(StorageError::ObjectNotFound { .. }) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    fn compare_and_swap_manifest(&self, request: ManifestCasRequest) -> StorageFuture<'_, ()> {
        let key = manifest_pointer_key(
            request.next.database_id.clone(),
            request.next.branch_id.clone(),
        );
        let bucket = self.profile.bucket.clone();
        let path = self.profile.object_path(&key);
        let runtime = &self.runtime;
        Box::pin(async move {
            let head = runtime
                .s3_head_object(S3HeadObject {
                    bucket: bucket.clone(),
                    key: path.clone(),
                })
                .await?;
            let condition = match request.expected_generation {
                None => {
                    if head.is_some() {
                        return Err(StorageError::ManifestCasFailed {
                            database_id: request.next.database_id.clone(),
                            branch_id: request.next.branch_id.clone(),
                        });
                    }
                    PutCondition::IfAbsent
                }
                Some(expected_generation) => {
                    let current = runtime
                        .s3_get_object(S3GetObject {
                            bucket: bucket.clone(),
                            key: path.clone(),
                        })
                        .await?;
                    let pointer: BranchManifestPointer = serde_json::from_slice(&current.bytes)
                        .map_err(|error| StorageError::Provider(error.to_string()))?;
                    if pointer.generation != expected_generation {
                        return Err(StorageError::ManifestCasFailed {
                            database_id: request.next.database_id.clone(),
                            branch_id: request.next.branch_id.clone(),
                        });
                    }
                    PutCondition::IfMatchEtag(
                        head.ok_or_else(|| StorageError::ObjectNotFound { key: path.clone() })?
                            .etag,
                    )
                }
            };
            let bytes = serde_json::to_vec(&request.next)
                .map_err(|error| StorageError::Provider(error.to_string()))?;
            let checksum_sha256 = sha256_hex(&bytes);
            runtime
                .s3_put_object(S3PutObject {
                    bucket,
                    key: path,
                    bytes,
                    checksum_sha256,
                    metadata: BTreeMap::from([
                        ("database_id".to_string(), request.next.database_id),
                        ("branch_id".to_string(), request.next.branch_id),
                        (
                            "generation".to_string(),
                            request.next.generation.to_string(),
                        ),
                    ]),
                    condition,
                })
                .await
        })
    }
}

impl<R: AwsStorageRuntime> LeaseStore for AwsReferenceStorage<R> {
    fn acquire_lease(&self, request: AcquireLeaseRequest) -> StorageFuture<'_, LeaseRecord> {
        let key = lease_key(request.database_id.clone(), request.branch_id.clone());
        let bucket = self.profile.bucket.clone();
        let path = self.profile.object_path(&key);
        let runtime = &self.runtime;
        Box::pin(async move {
            let head = runtime
                .s3_head_object(S3HeadObject {
                    bucket: bucket.clone(),
                    key: path.clone(),
                })
                .await?;
            let now = unix_now();
            let (condition, fencing_token) = if let Some(head) = head {
                let current = load_lease(runtime, bucket.clone(), path.clone()).await?;
                if current.expires_at_unix > now && current.holder_id != request.holder_id {
                    return Err(StorageError::LeaseHeld {
                        holder: current.holder_id,
                    });
                }
                (
                    PutCondition::IfMatchEtag(head.etag),
                    current.fencing_token + 1,
                )
            } else {
                (PutCondition::IfAbsent, 1)
            };
            let lease = LeaseRecord {
                database_id: request.database_id,
                branch_id: request.branch_id,
                holder_id: request.holder_id,
                fencing_token,
                expires_at_unix: now + ttl_seconds(request.ttl_ms),
            };
            put_lease(runtime, bucket, path, lease.clone(), condition).await?;
            Ok(lease)
        })
    }

    fn refresh_lease(&self, lease: LeaseRecord, ttl_ms: u64) -> StorageFuture<'_, LeaseRecord> {
        let key = lease_key(lease.database_id.clone(), lease.branch_id.clone());
        let bucket = self.profile.bucket.clone();
        let path = self.profile.object_path(&key);
        let runtime = &self.runtime;
        Box::pin(async move {
            let head = runtime
                .s3_head_object(S3HeadObject {
                    bucket: bucket.clone(),
                    key: path.clone(),
                })
                .await?
                .ok_or_else(|| StorageError::ObjectNotFound { key: path.clone() })?;
            let current = load_lease(runtime, bucket.clone(), path.clone()).await?;
            if current.holder_id != lease.holder_id || current.fencing_token != lease.fencing_token
            {
                return Err(StorageError::LeaseHeld {
                    holder: current.holder_id,
                });
            }
            let refreshed = LeaseRecord {
                expires_at_unix: unix_now() + ttl_seconds(ttl_ms),
                ..lease
            };
            put_lease(
                runtime,
                bucket,
                path,
                refreshed.clone(),
                PutCondition::IfMatchEtag(head.etag),
            )
            .await?;
            Ok(refreshed)
        })
    }

    fn release_lease(&self, lease: LeaseRecord) -> StorageFuture<'_, ()> {
        let key = lease_key(lease.database_id.clone(), lease.branch_id.clone());
        let bucket = self.profile.bucket.clone();
        let path = self.profile.object_path(&key);
        let runtime = &self.runtime;
        Box::pin(async move {
            let head = runtime
                .s3_head_object(S3HeadObject {
                    bucket: bucket.clone(),
                    key: path.clone(),
                })
                .await?
                .ok_or_else(|| StorageError::ObjectNotFound { key: path.clone() })?;
            let current = load_lease(runtime, bucket.clone(), path.clone()).await?;
            if current.holder_id != lease.holder_id || current.fencing_token != lease.fencing_token
            {
                return Err(StorageError::LeaseHeld {
                    holder: current.holder_id,
                });
            }
            runtime
                .s3_delete_object(S3DeleteObject {
                    bucket,
                    key: path,
                    if_match_etag: Some(head.etag),
                })
                .await
        })
    }
}

fn manifest_pointer_key(database_id: String, branch_id: String) -> ObjectKey {
    ObjectKey {
        database_id,
        branch_id,
        kind: ObjectKind::ManifestPointer,
        name: "pointer.json".to_string(),
    }
}

fn lease_key(database_id: String, branch_id: String) -> ObjectKey {
    ObjectKey {
        database_id,
        branch_id,
        kind: ObjectKind::Lease,
        name: "writer.json".to_string(),
    }
}

async fn load_lease<R: S3CompatibleStorageRuntime + ?Sized>(
    runtime: &R,
    bucket: String,
    path: String,
) -> Result<LeaseRecord, StorageError> {
    let record = runtime
        .s3_get_object(S3GetObject { bucket, key: path })
        .await?;
    serde_json::from_slice(&record.bytes).map_err(|error| StorageError::Provider(error.to_string()))
}

async fn put_lease<R: S3CompatibleStorageRuntime + ?Sized>(
    runtime: &R,
    bucket: String,
    path: String,
    lease: LeaseRecord,
    condition: PutCondition,
) -> Result<(), StorageError> {
    let bytes =
        serde_json::to_vec(&lease).map_err(|error| StorageError::Provider(error.to_string()))?;
    let checksum_sha256 = sha256_hex(&bytes);
    runtime
        .s3_put_object(S3PutObject {
            bucket,
            key: path,
            bytes,
            checksum_sha256,
            metadata: BTreeMap::from([
                ("database_id".to_string(), lease.database_id),
                ("branch_id".to_string(), lease.branch_id),
                ("holder_id".to_string(), lease.holder_id),
                ("fencing_token".to_string(), lease.fencing_token.to_string()),
                (
                    "expires_at_unix".to_string(),
                    lease.expires_at_unix.to_string(),
                ),
            ]),
            condition,
        })
        .await
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity("sha256:".len() + digest.len() * 2);
    output.push_str("sha256:");
    for byte in digest {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn ttl_seconds(ttl_ms: u64) -> u64 {
    ttl_ms.saturating_add(999) / 1000
}

fn safe_component(value: &str) -> String {
    let mut output = String::new();
    for byte in value.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {
                output.push(byte as char)
            }
            _ => output.push_str(&format!("_{byte:02x}")),
        }
    }
    if output.is_empty() {
        "_empty".to_string()
    } else {
        output
    }
}
