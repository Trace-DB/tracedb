---
title: Backup And Restore Runbook
tags:
  - tracedb/operations
  - tracedb/backup
  - tracedb/restore
status: active
type: runbook
updated: 2026-06-02
---

# Backup And Restore Runbook

This runbook defines the minimum backup/restore operating procedure for hybrid
AWS + Railway hosted-alpha deployments. It separates local TraceDB snapshot
helpers from managed-backup claims and keeps RPO/RTO internal-only until restore
drills prove them.

## Boundary Summary

- TraceDB local snapshot/restore helpers copy a local data directory and verify
  recovery semantics for that copied directory.
- Local snapshots are useful operator tools, but they are not managed-cloud
  backup/DR proof.
- Provider backups, EBS snapshots, S3 objects, Railway volume backups, and
  restore receipts must be tracked separately.
- RPO/RTO values are internal planning numbers until a dated restore drill proves
  them for the exact deployment shape.

The current local durability contract is documented in
`docs/durability-semantics-v0.md`.

## Local Snapshot Helper Vs Managed Backup Claims

TraceDB exposes local snapshot/restore admin helpers:

- `POST /v1/admin/snapshot`
- `POST /v1/admin/restore`
- `TraceDb::create_snapshot`
- `TraceDb::restore_snapshot`

The helper copies the current local data directory, including manifest, WAL,
checkpoints, segments, indexes, hot data, jobs, and lock-file artifacts. Restore
copies a snapshot source into a separate target directory and opens the target to
run recovery.

Do not describe a passed local snapshot/restore check as:

- Managed backup.
- Managed disaster recovery.
- Cross-region restore.
- Point-in-time recovery.
- Railway volume backup validation.
- AWS EBS/S3 backup validation.
- Production RPO/RTO evidence.

It is local filesystem/admin-route evidence only unless paired with a provider
backup receipt and a successful restore drill.

## AWS EBS/S3 Baseline

For an AWS-hosted stateful engine baseline:

1. Run `tracedb-engine` on one encrypted EBS-backed data volume mounted at the
   configured data path, usually `/data/tracedb`.
2. Keep the engine single-writer. Do not mount the same TraceDB data directory
   writable from gateway, worker, benchmark, or another engine process.
3. Enable scheduled EBS snapshots for the engine volume.
4. Tag snapshots with service, environment, database/branch scope when safe,
   creation time, owner, and retention class.
5. Use lifecycle rules to expire old snapshots according to the internal
   retention plan.
6. If exporting TraceDB snapshots or debug bundles to S3, use a dedicated bucket
   with versioning, server-side encryption, least-privilege IAM, and restricted
   public access. Consider Object Lock only after retention/legal requirements
   are explicit.
7. Do not use S3 as hot WAL, active mutable index state, or live query paging
   storage.
8. Keep backup credentials separate from runtime public API tokens.

Minimum AWS controls:

- EBS encryption enabled.
- S3 SSE-S3 or SSE-KMS enabled for snapshot/export buckets.
- IAM role scoped to the exact bucket/prefix needed for snapshot/export tasks.
- CloudTrail or provider audit records retained for backup and restore actions.
- Restore target in an isolated environment before touching production volumes.

## Railway Baseline

For Railway hosted-alpha labs:

1. Attach a single persistent volume to `tracedb-engine` only.
2. Keep `tracedb-gateway`, `tracedb-worker`, and benchmark jobs off the TraceDB
   data volume.
3. Use Railway private networking for gateway/worker calls to the engine.
4. Treat Railway volume backup/snapshot features as provider-managed artifacts
   that require their own receipt.
5. Keep TraceDB local snapshot checks separate from Railway volume backup checks.
6. Validate restore into an isolated service or environment before claiming the
   backup is usable.

## Backup Receipt Fields

A backup receipt is an evidence artifact, not the backup itself. It must be
created only after the operator confirms backup creation and restore validation.

Required fields for Railway benchmark gates that declare backup requirements:

```yaml
kind: railway_backup_receipt
status: passed
confirmed: true
backup_created: true
restore_validated: true
service_id: <TraceDB Railway service ID>
backup_id: <provider backup/snapshot ID>
restore_validation_method: <non-empty method description>
```

Recommended additional fields for all providers:

```yaml
provider: aws|railway|other
environment: <environment name>
region: <region or provider location>
operator: <person or automation identity>
created_at: <RFC3339 timestamp>
validated_at: <RFC3339 timestamp>
source_service: tracedb-engine
source_data_path: /data/tracedb
backup_type: ebs_snapshot|s3_export|railway_volume_backup|local_snapshot
snapshot_or_object_uri: <redacted provider reference>
retention_class: <short retention label>
encryption: ebs_encrypted|sse_kms|sse_s3|provider_default|unknown
tde_key_id: <non-secret key identifier when available>
marker_record_id: <restore drill marker>
restore_target: <isolated target service/env>
restore_result: passed|failed
restore_notes: <short notes>
redactions: [tokens, dsns, internal_urls]
```

Receipt rules:

- Never include secret values, bearer tokens, DSNs, S3 keys, Railway tokens, or
  `TRACEDB_MASTER_KEY_B64`.
- Do not mark `status: passed` unless a restore target was opened and a marker
  read succeeded.
- Keep provider backup IDs and object URIs in private artifacts unless approved
  for sharing.

## Restore Drill Steps

Run restore drills on a schedule and before making any external backup/DR claim.
Use an isolated target; never overwrite production as the first validation step.

1. **Prepare**
   - Identify source service, data path, provider backup ID, and expected TDE
     key material.
   - Confirm no public direct-engine diagnostic endpoint is active unless the
     drill explicitly requires it.
   - Pick a unique marker ID and tenant/database/branch scope.
2. **Write a marker**
   - Through the gateway when testing hosted-alpha public behavior, write a
     small marker record.
   - Read the marker back and record its ID, epoch/receipt if available, and
     timestamp.
3. **Create backup**
   - For local helper validation, call `POST /v1/admin/snapshot` to a scratch
     path that is not the active data directory.
   - For AWS, create or identify the scheduled EBS snapshot and/or S3 export.
   - For Railway, create or identify the Railway volume backup/snapshot.
4. **Restore to isolation**
   - Provision a new volume/service or isolated environment.
   - Attach/copy the backup into the restore target.
   - Configure the same `TRACEDB_MASTER_KEY_B64` if the source data used TDE.
   - Do not expose the restore target publicly unless the drill specifically
     requires a time-boxed diagnostic endpoint.
5. **Open and recover**
   - Start `tracedb-engine` against the restored data directory.
   - Check readiness through private routing where possible.
   - Watch for manifest, WAL, checkpoint, TDE, or lock-file errors.
6. **Validate marker**
   - Read the pre-backup marker from the restored target.
   - Record success/failure, elapsed restore time, and any manual steps.
7. **Record receipt**
   - Write a backup receipt with the required fields above.
   - Redact secrets and internal URLs before sharing artifacts.
8. **Clean up**
   - Remove temporary public domains/listeners.
   - Destroy isolated restore services or volumes when no longer needed.
   - Preserve only approved receipt and audit artifacts.

## RPO/RTO Policy

Until repeat restore drills prove otherwise, RPO and RTO are internal planning
numbers only.

- Do not publish RPO/RTO externally.
- Do not include RPO/RTO in customer-facing collateral.
- Do not imply managed DR from local snapshot helper output.
- Track measured restore time, backup age, and failed/manual steps in internal
  receipts.

A candidate RPO/RTO may be promoted only after the same deployment shape has a
repeatable drill history, documented failure handling, and approval from the
operator/owner responsible for the claim.

## Key Management And TDE Notes

TraceDB TDE uses `TRACEDB_MASTER_KEY_B64` or equivalent open options to provide
a 32-byte root key. With TDE configured, TraceDB stores wrapped data-encryption
key metadata in `manifest.tdb`; the manifest remains plaintext metadata, while
new WAL payloads, v3 checkpoints, segment objects, and index artifacts are
encrypted under the configured TDE context.

Operational requirements:

- Store `TRACEDB_MASTER_KEY_B64` only in a secret manager or KMS-backed secret
  store.
- Back up key material separately from data backups. A valid encrypted backup is
  unrecoverable without the correct master key.
- Do not write master keys into receipts, logs, manifests, benchmark reports, or
  command lines.
- Restore drills for encrypted data must prove wrong-key and missing-key failure
  behavior only in safe non-production tests.
- Treat TDE as artifact encryption for configured TraceDB data, not as a full
  compliance claim.
- Plan key rotation carefully; do not rotate or delete a key until every backup
  that depends on it is expired or has been re-encrypted and tested.
