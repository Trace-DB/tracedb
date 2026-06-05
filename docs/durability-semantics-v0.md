---
title: TraceDB Durability Semantics v0
tags:
  - tracedb
  - durability
  - idempotency
  - wal
  - platform-contract
status: contract-freeze
type: durability-boundary
updated: 2026-06-03
---

# TraceDB Durability Semantics v0

This document states what the current TraceDB local engine does and does not
guarantee. It is a diligence boundary for the development-stage product, not a
managed-service SLA.

## Scope

TraceDB v0 durability applies to the local-first, single-process engine opened
from one data directory. The HTTP server and gateway routes use that engine
state; the gateway forwards product routes but does not add replication,
consensus, managed-cloud backup/DR, or stronger write durability.

The current contract assumes one active writer per data directory. Multiple
long-lived engine handles, multiple processes writing the same directory, or
distributed replicas are outside v0.

## Durable Artifacts

The local data directory contains the durability contract:

- `manifest.tdb` stores database identity, branch identity, schemas, latest
  epoch, durable epoch, checkpoint epoch, manifest generation, module metadata,
  segment/index metadata, and a manifest checksum. Manifest writes rotate the
  previous manifest to `manifest.tdb.bak`, recompute the checksum, write a
  temporary file, sync that file, rename it into place, and sync the parent
  directory.
- `wal/000001.twal` stores WAL commit frames. Each mutation/admin state change
  that modifies engine records or schema is appended as a committed frame before
  the manifest advances. The append path writes the frame and calls
  `file.sync_data()`.
- WAL commit frames include magic, format version, LSN, previous payload
  checksum, payload length, payload checksum, JSON commit payload, a committed
  footer, and the commit marker inside the payload. The previous checksum forms
  a chain across frames.
- `checkpoints/checkpoint-<epoch>.tchk` stores framed checkpoint payloads with a
  magic prefix, payload checksum, schemas, records, and checkpoint epoch.
- `segments/`, `indexes/`, `hot/`, and `jobs/` are copied by snapshot/backup
  helpers as part of the current local data directory shape.
- `engine.lock` is opened with a process-wide exclusive file lock during engine
  open. A second active process using the same data directory fails fast instead
  of writing concurrently.
- `engine.write.lock` serializes TraceDB engine write sections. `000001.twal.lock`
  serializes WAL appends. Both are create-new lock files that are removed when
  their guards drop.
- `IdempotencyReceipt` entries for supported mutation/admin routes are stored
  in WAL commit frames and checkpoint payloads. On open, the server rebuilds
  its in-memory replay cache from those receipts; there is no separate durable
  `http-idempotency-cache.json` authority.

## Transparent Data Encryption

When `TRACEDB_MASTER_KEY_B64` or `TraceDbOpenOptions` provides a 32-byte root
key, TraceDB creates a per-database data encryption key and stores wrapped DEK
metadata in `manifest.tdb`. The manifest remains plaintext metadata; WAL
payloads, framed v3 checkpoints, segment objects, and index artifacts are
encrypted when written under that configured TDE context.

Wrong-key and missing-key opens fail closed for encrypted data. Legacy
plaintext WAL, checkpoint, and segment artifacts remain readable and are not
rewritten merely by opening with TDE configured. New WAL/checkpoint/segment/index
artifacts written after TDE is configured are encrypted. WAL/checkpoint-backed
idempotency receipts inherit the same artifact behavior as the frames that
carry them.

## Recovery Semantics

On open, TraceDB creates the data-directory layout, acquires `engine.lock`, reads
`manifest.tdb`, checks the manifest checksum, opens the WAL, and scans committed
WAL frames. If
`manifest.checkpoint_epoch` is nonzero, the engine reads the matching
checkpoint first, checks the checkpoint frame/checksum, and rebuilds visible
records from that checkpoint. It then replays WAL commits whose epoch is greater
than the checkpoint epoch.

If WAL replay finds committed records beyond `manifest.latest_epoch`, open
advances `latest_epoch`, `durable_epoch`, and manifest generation, then rewrites
the manifest. This lets a successfully appended WAL commit recover even if the
manifest advance did not finish.

The WAL scanner treats a torn WAL tail as recoverable only when the incomplete
data is at the tail:

- short header
- short payload
- missing commit footer

In those cases, committed frames before the torn tail are replayed and the torn
tail is reported through engine recovery metadata. The incomplete tail frame is
not applied.

The scanner treats these conditions as hard corruption, not best-effort replay:

- invalid WAL magic
- unsupported WAL version or frame kind
- previous checksum mismatch
- oversized payload
- payload checksum mismatch
- commit footer mismatch
- missing commit marker
- invalid parent epoch
- previous commit hash mismatch

Manifest corruption first attempts the rotated `manifest.tdb.bak` fallback.
If both the current manifest and backup are unusable, missing manifest checksum,
manifest checksum mismatch, checkpoint epoch greater than latest epoch,
checkpoint epoch mismatch, missing checkpoint checksum, checkpoint checksum
mismatch, unsupported checkpoint format, and checkpoint parse failure stop open
instead of silently repairing state.

## Snapshot And Restore

`POST /v1/admin/snapshot` and `TraceDb::create_snapshot` copy the current local
data directory into a target directory. Snapshot copies `manifest.tdb`,
`engine.lock`, `wal`, `hot`, `segments`, `indexes`, `checkpoints`, and `jobs`.
The target must not be the same directory as the source.

`POST /v1/admin/restore` and `TraceDb::restore_snapshot` restore by copying a
snapshot source directory into a separate target directory, then opening the
target as a TraceDB data directory. Restore removes an existing target directory
before copying. The copy-path guard rejects identical source and target paths
and rejects target paths inside the source tree; source and target directories
must differ. The route-level error text is `source and target directories must differ`.

Snapshots are local filesystem copies. The v0 snapshot API is intended for
local scratch/admin workflows and controlled hosted-service checks. It is not
managed-cloud backup/DR, cross-region restore, point-in-time recovery across
many WAL files, or a replacement for operator-managed backups.

Snapshot and restore route handlers run through the async engine handle with
bounded admin work. That avoids request-path global mutex blocking for health,
readiness, and metrics, but it does not make external filesystem mutation safe
and does not coordinate with another process writing the same data directory.

## Idempotency Semantics

Supported HTTP mutation/admin routes accept `Idempotency-Key`. For the same
method, path, key, and request body, the server returns the cached successful
response. If the same method, path, and key are reused with a different request
body, the server returns `409` with code `idempotency_conflict`.

The replay authority is local-engine-only and WAL/checkpoint-backed. Mutation
routes embed receipts in the mutation WAL commit; successful admin responses
record a receipt-only WAL entry. Receipts are scoped by method, path, key, body
hash, actor tenant, database, branch, and token identity. They are not cross-replica,
not shared across independent data directories, not a managed-cloud exactly-once
guarantee, and not crash-atomic exactly-once across
all failure points.

SDK idempotency retries remain opt-in and only apply to mutation/admin requests
that carry a caller-provided idempotency key.

## Known Non-Guarantees

TraceDB v0 does not yet claim:

- distributed consensus or replication
- multi-process active writer support on the same data directory
- cross-replica idempotency
- cross-replica exactly-once semantics
- crash-atomic exactly-once semantics
- managed-cloud backup/DR semantics
- SQL/PostgreSQL durability semantics
- managed service RPO/RTO
- online snapshot isolation against external filesystem mutation
- production web-server behavior, TLS, HTTP/2, or proxy-hardening semantics

Stale PID lock files for `engine.write.lock` and `000001.twal.lock` are
best-effort recovered after explicit owner checks. Active-owner locks,
invalid-owner lock files, timeout cases, or ambiguous process ownership still
require operator judgment instead of blind removal.

## Operator Checks

For a local or hosted-service lab run, use these checks before making
durability claims:

- Run the platform/product gate that covers schema, writes, reads, query,
  delete, idempotency, snapshot, and restore for the surface under test.
- Inspect `manifest.tdb` and WAL metadata through existing CLI/HTTP diagnostics
  when debugging recovery.
- After any restart/redeploy, write a marker before restart and read it after
  restart through the same product surface.
- After snapshot/restore, verify a known marker record from the restored target.
- Inspect `engine.write.lock` and `000001.twal.lock` errors carefully: stale
  dead-PID locks should recover, while active/invalid-owner cases remain
  operator-visible safety stops.
- Run `cargo run -p tracedb-cli -- durability-faults` for the local durability
  receipt at `target/tracedb/durability-faults.json`.
- Keep exported performance or durability claims separate from internal-only
  development evidence unless the relevant remote gate and backup receipt are
  present for that run.
