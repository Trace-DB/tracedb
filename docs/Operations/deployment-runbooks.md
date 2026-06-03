---
title: Deployment Runbooks
tags:
  - tracedb/operations
  - tracedb/deployment
  - tracedb/aws
  - tracedb/railway
status: active
type: runbook
updated: 2026-06-02
---

# Deployment Runbooks

This document captures hosted-alpha deployment procedures for hybrid AWS +
Railway operation. It is documentation only; it does not replace provider runbook
execution, incident command, or release approval.

## Preflight Checklist

Run this checklist before deploy, rollback, restart, backup drill, or public
exposure change.

### Boundary checks

- Public ingress points only at `tracedb-gateway`.
- `tracedb-engine` has no public domain/listener unless there is a time-boxed
  diagnostic note.
- `tracedb-worker` is private.
- Engine private URL resolves from gateway and worker.
- `TRACEDB_HOSTED_ALPHA=true` is set for hosted-alpha services.
- `TRACEDB_REQUIRE_API_KEY=true` is set on gateway.
- `TRACEDB_ENGINE_INTERNAL_TOKEN` is set on gateway, engine, and worker.
- `tracedb-engine` is the only service with write access to `/data/tracedb`.

### Secret checks

- `TRACEDB_API_TOKEN` is present only in provider secrets.
- `TRACEDB_ENGINE_INTERNAL_TOKEN` is present only in provider secrets.
- `TRACEDB_MASTER_KEY_B64`, if used, is present only in provider secrets.
- Postgres, Redis/Valkey, S3, Railway, AWS, OpenRouter, and benchmark secrets are
  not present in git, reports, or command lines.
- The operator has a rotation plan before changing tokens.

### Health and data checks

- Gateway health/readiness is public-safe.
- Engine readiness is reachable privately.
- Current data path and volume mount are known.
- Backup status is known before risky operations.
- A marker write/read smoke has passed if persistence must be proven.
- Disk free space is above the internal threshold for compaction, snapshot, and
  WAL growth.
- Lock-file errors are absent or understood.

### Claim checks

- No deployment output is described as SLA, managed DR, cross-region failover,
  or production compliance evidence.
- Direct engine diagnostics are labeled diagnostic-only.
- Backup, restart, redeploy, and rollback claims have receipts when used in
  benchmark gates or external-facing summaries.

## Emergency Takedown

Use this when a token leaks, private engine becomes public unintentionally,
traffic is abusive, or the environment may expose customer data.

1. **Stop public ingress**
   - Remove or disable public gateway domain/listener.
   - If the engine has a public domain/listener, remove it first.
   - On AWS, tighten security groups, ALB listener rules, WAF rules, or API
     Gateway routes.
   - On Railway, remove public domains from private services and disable exposed
     service routes.
2. **Preserve evidence**
   - Save provider audit logs, deployment IDs, and sanitized request/error
     summaries.
   - Do not dump full environment variables or request bodies into public issue
     trackers.
3. **Rotate secrets**
   - Rotate public API token.
   - Rotate internal engine token if private services or logs may be exposed.
   - Rotate provider tokens and S3/DSN credentials if they were in the exposure
     path.
4. **Check data integrity**
   - Restart only when safe.
   - Check engine readiness and WAL/manifest recovery errors.
   - Run marker read/write or read-only validation depending on incident scope.
5. **Restore public gateway only**
   - Re-enable `tracedb-gateway` public ingress after tokens and private routing
     are corrected.
   - Confirm direct engine exposure remains removed.
6. **Record incident receipt**
   - Include timeline, affected endpoints, secrets rotated, validation result,
     and remaining risks.
   - Redact secrets and internal URLs.

## Secret Rotation

### Public API token rotation

1. Generate a new high-entropy public API token.
2. Update gateway `TRACEDB_API_TOKEN` in the provider secret store.
3. Redeploy or restart gateway according to provider behavior.
4. Update approved clients/benchmarks to send the new bearer token.
5. Run gateway health/readiness and an authenticated smoke.
6. Revoke/delete the old token from all client stores.
7. Search recent logs/artifacts for accidental token exposure and redact.

### Internal engine token rotation

Rotate the internal token in a coordinated window because gateway/worker and
engine must agree.

1. Generate a new high-entropy `TRACEDB_ENGINE_INTERNAL_TOKEN`.
2. Prepare gateway, worker, and engine secret updates.
3. Prefer updating engine and dependents in a short maintenance window.
4. Restart/redeploy services so all processes read the same token.
5. Verify gateway-to-engine and worker-to-engine calls.
6. Confirm direct private engine calls without the token fail.
7. Redact old token from any copied env files or reports.

If the provider supports dual token validation in the future, use staged
overlap. Until then, assume single-token cutover.

### TDE master key handling

Do not rotate or delete `TRACEDB_MASTER_KEY_B64` casually. If TDE is configured,
losing the correct key can make encrypted backups unrecoverable. Plan key
rotation as a separate maintenance project with backup inventory, restore drill,
and rollback criteria.

## Restart/Redeploy Persistence Receipt

A restart or redeploy proves persistence only when a pre-operation marker remains
visible after the operation and the operation itself is confirmed.

Minimum receipt fields:

```yaml
kind: railway_operation_receipt
operation: restart|redeploy
status: passed
executed: true
confirmed: true
service_id: <TraceDB Railway service ID>
suite_id: <suite or run ID>
operator: <operator or automation identity>
command: <sanitized command or provider action>
created_at: <RFC3339 timestamp>
```

Persistence evidence should pair that receipt with:

- A pre-operation marker write/read manifest.
- The provider restart/redeploy receipt.
- A post-operation read-only marker check using the same marker ID.

Do not call restart/redeploy persistence proven when the receipt is missing,
`executed` is false, `confirmed` is false, the service ID does not match, or the
postcheck wrote a new marker instead of reading the old one.

## Railway Deployment And Rollback

### Deploy

1. Confirm service roles and variables match `deploy/railway/env.*.example`.
2. Ensure only `tracedb-gateway` has public ingress for hosted alpha.
3. Ensure `tracedb-engine` has the persistent volume mounted and
   `TRACEDB_DATA_DIR=/data/tracedb`.
4. Deploy engine first when data path or image changes are involved.
5. Deploy gateway after engine readiness passes privately.
6. Deploy worker after engine and queue readiness are understood.
7. Run gateway `/health` and `/ready` checks.
8. Run an authenticated marker write/read smoke through the gateway.
9. Save sanitized deployment IDs, service IDs, image/version, and smoke result.

### Rollback

1. Stop or pause public gateway ingress if the release is actively harmful.
2. Identify the last known-good Railway deployment for the affected service.
3. Roll back gateway first for public route regressions.
4. Roll back worker for background-job regressions that do not corrupt data.
5. Treat engine rollback as high risk if the release changed data format, WAL,
   manifest, checkpoints, indexes, or encryption behavior.
6. Before engine rollback, capture current backup status and operator approval.
7. After rollback, run readiness and marker checks.
8. Record whether the marker existed before rollback or was created after it.

Do not use a direct public engine endpoint as rollback ingress except for a
specific time-boxed diagnostic.

## AWS Deployment And Rollback

### Deploy

1. Confirm VPC/subnet/security-group boundaries: gateway public, engine/worker
   private.
2. Confirm EBS volume attachment and mount path for `tracedb-engine`.
3. Confirm AWS Secrets Manager or SSM parameters are available to the runtime.
4. Confirm S3 bucket policy, KMS policy, and IAM role if snapshot/export support
   is used.
5. Deploy engine task/instance/service and wait for private readiness.
6. Deploy worker after engine private readiness.
7. Deploy gateway or update ALB/API Gateway/CloudFront routing last.
8. Run public-safe gateway health/readiness.
9. Run authenticated marker write/read through the gateway.
10. Save sanitized deployment IDs, AMI/image digests, task revisions, and smoke
    results.

### Rollback

1. Disable or drain public gateway traffic if the release is unsafe.
2. Roll back gateway route/task/image for edge/auth/routing regressions.
3. Roll back worker separately when only background jobs are affected.
4. For engine rollback, check data-format compatibility and current backup
   status first.
5. Never attach the same writable TraceDB volume to two engine instances.
6. Restore from an EBS snapshot only into an isolated target unless incident
   command explicitly approves production replacement.
7. Verify marker visibility after rollback or restore.
8. Record rollback receipt and any backup/restore receipt used.

## Disk Pressure Runbook

Disk pressure can corrupt operations indirectly by preventing WAL, manifest,
checkpoint, segment, index, or snapshot writes from completing.

Signals:

- Provider disk/volume usage alert.
- Engine readiness failure or write errors mentioning no space left.
- Snapshot, checkpoint, compaction, or index job failures.
- Rapid WAL or segment growth.

Steps:

1. Reduce public traffic at the gateway if writes are failing.
2. Stop nonessential benchmark, import, or background workload.
3. Check provider volume usage and recent growth.
4. Avoid creating new snapshots on the same full volume.
5. If safe, increase volume size before deleting data.
6. Run compaction only when there is enough temporary free space for it.
7. Export or remove approved scratch/debug artifacts; do not delete WAL,
   manifest, checkpoints, segments, indexes, or jobs by guesswork.
8. After recovery, run readiness and marker read/write checks.
9. Record free-space before/after and the exact intervention.

## Lock-file Intervention

TraceDB uses lock files and process locks to preserve single-writer safety.
Blind deletion can cause data loss if another engine is active.

Relevant local lock artifacts include:

- `engine.lock`
- `engine.write.lock`
- `wal/000001.twal.lock`

Intervention steps:

1. Confirm there is no active engine process using the data directory.
2. Confirm no deploy, restart, restore, compaction, snapshot, or benchmark job is
   still running.
3. Inspect the engine logs for stale PID, active owner, invalid owner, or timeout
   messages.
4. If the lock is stale and the owning process is confirmed dead, remove only the
   stale lock file named in the error.
5. Never remove lock files to allow two writers.
6. Restart the engine and check readiness.
7. Run a marker read/write or read-only persistence check.
8. Record the lock path, reason, owner check, operator, and validation result.

Active-owner locks, invalid-owner lock files, and ambiguous process ownership
require operator judgment and should be treated as safety stops.
