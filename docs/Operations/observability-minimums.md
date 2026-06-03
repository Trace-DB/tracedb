---
title: Observability Minimums
tags:
  - tracedb/operations
  - tracedb/observability
status: active
type: runbook
updated: 2026-06-02
---

# Observability Minimums

This runbook defines the minimum metrics, logs, and alerts for hosted-alpha
TraceDB deployments and benchmark lanes. It is an alpha operations baseline, not
a complete production observability program.

## Global Requirements

All services should emit structured logs with enough context to debug an
incident without exposing secrets or record contents.

Required dimensions where available:

- service: `gateway`, `engine`, `worker`, `benchmark`
- environment: `aws`, `railway`, `local-cloud`, or specific environment name
- deployment ID or image digest
- region/provider location
- request ID or run ID
- database/branch identifiers only when safe and necessary
- route or operation name
- status code / error code

Never log raw secrets, bearer tokens, DSNs, S3 keys, Railway/AWS tokens,
`TRACEDB_MASTER_KEY_B64`, or full user record bodies.

## Gateway

The gateway is the public edge and should be the primary source for public
traffic visibility.

### Metrics

Minimum gateway metrics:

- Request count by route, method, status code, and environment.
- Latency histogram by route and method.
- In-flight requests and concurrency-limit rejections.
- Request body size distribution or oversized-body rejection count.
- Auth failures: missing token, invalid token, and forbidden/private route
  attempts.
- Rate-limit decisions: allowed, throttled, and backend/cache unavailable.
- Upstream engine request count, latency, timeout, and failure count.
- Public health/readiness status.
- 4xx and 5xx rates.

### Logs

Minimum gateway log events:

- Request start/finish with request ID, route, method, status, duration, and
  redacted actor/tenant context.
- Authentication failures without token values.
- Rate-limit rejections with policy name/window, not secret material.
- Upstream engine timeout/failure with private URL redacted or coarsened.
- Gateway startup configuration summary with secrets redacted.
- Public diagnostic route access.

### Alerts

Minimum gateway alerts:

- Gateway readiness unhealthy for the agreed alpha window.
- 5xx rate above threshold.
- p95 or p99 latency above threshold.
- Sudden spike in 401/403/429 responses.
- Upstream engine timeout or failure spike.
- Public requests hitting routes that should be private.
- Rate limiter disabled unexpectedly in hosted alpha.
- Public direct-engine diagnostic still active past planned removal time.

## Engine

The engine is the private stateful owner of the TraceDB data directory.

### Metrics

Minimum engine metrics:

- Health/readiness state.
- Write/admin operation count and latency.
- Read/query operation count and latency.
- WAL append count, bytes written, latest LSN/epoch where safe.
- Manifest write count and failure count.
- Checkpoint count, duration, and failure count.
- Snapshot/restore count, duration, bytes copied when available, and failures.
- Compaction/index job count, duration, and failures.
- Data volume disk usage, free bytes, inode usage if available.
- Process CPU, memory, file descriptors, and restart count.
- Lock acquisition failures or stale-lock recovery events.
- TDE open failures for wrong/missing key in test contexts; do not expose key
  material.
- Internal engine-token authentication failures.

### Logs

Minimum engine log events:

- Startup with data directory role and hosted-alpha/internal-token enforcement
  status, with paths and secrets redacted as needed.
- Recovery summary: manifest/checkpoint/WAL status, torn-tail recovery, and hard
  corruption errors.
- WAL/manifest/checkpoint write failures.
- Snapshot and restore start/finish/failure with source/target coarsened or kept
  private.
- Lock-file safety stops and stale-lock interventions.
- Disk pressure errors.
- Internal-token failures without token values.
- TDE wrong-key/missing-key failures without key material.

### Alerts

Minimum engine alerts:

- Engine readiness unhealthy.
- Engine restart loop.
- Disk usage above warning and critical thresholds.
- WAL/manifest/checkpoint write failure.
- Snapshot/restore failure.
- Lock-file active-owner or ambiguous-owner safety stop.
- Internal-token authentication failures above low threshold.
- Recovery hard corruption error.
- Memory or CPU saturation sustained above threshold.

## Worker

The worker is private and performs background queue work through the engine API.

### Metrics

Minimum worker metrics:

- Queue depth by job type.
- Jobs started, completed, failed, retried, and dead-lettered by job type.
- Job latency and time in queue.
- Lease acquisition/renewal failures.
- Engine API call count, latency, timeout, and failure count.
- Worker process CPU, memory, and restart count.
- Backoff/retry counts.

### Logs

Minimum worker log events:

- Worker startup and queue/backend configuration summary with secrets redacted.
- Job start/finish/failure with job ID/type and sanitized database/branch scope.
- Lease conflicts, expirations, and retries.
- Engine API failures with private URL and internal token redacted.
- Dead-letter events and operator-required interventions.

### Alerts

Minimum worker alerts:

- Queue depth above threshold for sustained period.
- Job failure or retry rate above threshold.
- Dead-letter count nonzero for critical job types.
- Worker cannot reach engine private API.
- Worker restart loop.
- Lease renewal failures sustained above threshold.

## Benchmarks And Proof Lanes

Benchmark lanes are evidence generators, not production monitors. Their artifacts
must be observable, reproducible, and explicit about claim boundaries.

### Metrics And Artifacts

Minimum benchmark observability:

- Suite ID, scenario, target, surface, run start/end time, and git revision.
- Target URL class: gateway, private engine, or diagnostic public engine.
- Explicit claim scope: local-only, Railway lab, AWS lab, benchmark-only, or
  not-checked.
- Health/readiness probe result.
- Stateful marker write/read result when requested.
- Snapshot/restore result when requested.
- Restart/redeploy persistence verdict when requested.
- Backup receipt verdict when required.
- Request count, latency summary, timeout count, retry count, and error summary.
- Environment manifest with secrets redacted.

### Logs

Minimum benchmark log behavior:

- Redact `TRACEDB_HTTP_BEARER_TOKEN`, `TRACEDB_API_TOKEN`, provider tokens,
  DSNs, OpenRouter keys, and S3 credentials.
- Record whether the benchmark targeted gateway or diagnostic direct engine.
- Record skipped/not-checked claims explicitly.
- Preserve enough suite command/config context to reproduce the run without
  exposing secrets.

### Alerts Or Gates

Minimum benchmark gates:

- Block when required Railway/AWS config is missing.
- Block when health/readiness probes fail for requested remote lanes.
- Block when marker write/read is requested and fails.
- Block when snapshot/restore is requested and fails.
- Block backup-required specs without a valid backup receipt.
- Block restart/redeploy persistence claims without pre-marker, operation
  receipt, and post-read-only marker evidence.
- Fail closed when generated artifacts contain unredacted known secret fields.

## Cross-service Alerts

Hosted-alpha environments should have at least these combined alerts:

- Gateway public readiness unhealthy.
- Engine private readiness unhealthy.
- Gateway can reach public health but cannot reach engine privately.
- Any private service has a public domain/listener unexpectedly.
- Disk critical on the engine volume.
- Sustained 5xx rate across gateway or engine.
- Authentication failure spike.
- Rate-limit spike or rate limiter disabled unexpectedly.
- Backup/restore drill overdue.
- Restore drill failed.
- Restart/redeploy persistence postcheck failed.

## Minimum Dashboard Layout

A minimal hosted-alpha dashboard should show:

1. Public gateway availability, request rate, latency, 4xx/5xx, and auth/rate
   limit events.
2. Engine readiness, write/read latency, WAL/checkpoint/snapshot failures, disk,
   CPU, memory, and restart count.
3. Worker queue depth, job failures, retries, and engine API failures.
4. Backup/restore drill status and latest receipt timestamp.
5. Benchmark/proof lane status with claim scope and latest gate result.
6. Active diagnostics, especially any time-boxed public engine exposure.

## Retention

Suggested minimum retention for hosted alpha:

- Hot logs: 7 to 14 days.
- Incident logs and receipts: 90 days or the internal audit window.
- Backup/restore receipts: at least as long as the backups they describe.
- Benchmark receipts used in claims: retain with the claim/release evidence.

Adjust retention for privacy, cost, and provider limits. Retention does not turn
alpha evidence into managed-service proof.
