# Cloud Storage Foundation v0.1.1

TraceDB Cloud uses Railway for compute and AWS for durable storage, but the
runtime interface belongs to the public Apache TraceDB repository.

## Boundary

Public runtime owns:

- Object, manifest, and lease interfaces.
- WAL/checkpoint/segment/snapshot object layout.
- Manifest compare-and-swap semantics.
- Single-writer lease semantics.
- Local, mounted-volume, S3, and S3-compatible adapter traits usable by
  self-hosted operators.

Private TraceDB Cloud owns:

- Account, invite, quota, and support workflows.
- Deployment orchestration.
- Operator dashboards.
- Managed backup scheduling and incident workflow.

No query, storage-format, tenant-isolation, branching, or SDK feature may be
TraceDB-Cloud-only.

## New crate

`tracedb-remote-storage` defines:

- `ObjectStore`
- `ManifestStore`
- `LeaseStore`
- `MetricsSink`
- `SecretLoader`
- `TokenVerifier`
- `S3CompatibleStorageProfile`
- `S3CompatibleStorageRuntime`
- `AwsStorageRuntime`

The S3-compatible adapter is intentionally split into public TraceDB commands
and a runtime trait. TraceDB Cloud, self-hosted AWS operators, MinIO users, and
local mounted-volume deployments can use the same database semantics.

The public self-host AWS baseline lives in `deploy/aws/` and creates the S3 and
IAM resources expected by this interface.

## v0.1.1 runtime shape

- S3 or an S3-compatible object store stores WAL chunks, checkpoints, manifests,
  lease records, segments, snapshots, and exports.
- Local or mounted storage remains a first-class runtime backend.
- Manifest compare-and-swap and lease fencing use the selected storage backend:
  local atomic writes/locks for filesystem deployments and conditional object
  writes, conditional lease release, and object metadata for S3-compatible
  deployments.
- Railway engine compute hydrates from AWS-backed state and uses local disk only
  as cache/spool.
- Databases are single-writer and pinned to one primary region for v0.1.1.
- US East and US West are deployment regions, not an automatic database failover
  claim yet.

## Remote validation

Local validation is intentionally not run on this workstation. Remote CI should
cover:

- Object path stability.
- Local, S3, and MinIO-compatible put/get/head/delete behavior.
- Manifest compare-and-swap success and conflict.
- Lease acquire, refresh, release, fencing-token progression, and conflict.
- Stale lease holders cannot delete a newer lease during release.
- Engine restart and rehydration from AWS-backed state.

## Day-0 isolation gate

TraceDB already has public runtime actor and policy primitives, including
tenant, database, branch, token identity, policy epoch, and scopes. TraceDB
Cloud must prove that the hosted gateway is the only authority that maps an API
key to those actor fields.

Before invited developers can use hosted databases, staging must show:

- Client-supplied `x-tracedb-tenant-id`, `x-tracedb-database-id`,
  `x-tracedb-branch-id`, `x-tracedb-token-identity`, `x-tracedb-policy-epoch`,
  and `x-tracedb-scopes` headers are stripped or rejected at public ingress.
- The gateway injects actor fields only after API-key verification.
- Cross-tenant, cross-database, cross-branch, revoked-key, and missing-scope
  requests fail before reaching engine state mutation.
- Cursor/idempotency scopes include actor identity and cannot be replayed across
  tenants, databases, branches, or token identities.
