# Kernel & Module Design: TraceDB

## 1. Small Kernel Design

The TraceDB kernel is designed to be as small and minimal as possible. A feature or capability belongs in the kernel only if its failure or corruption breaks the database's core laws of physics, visibility semantics, recovery paths, or physical manifest publication.

### Kernel Responsibilities:
*   **Record Identity**: Standardizes `RecordId`, `TableId`, and `VersionId` formats.
*   **Schema Catalog**: Holds table metadata, column configurations, indices, policies, and module definitions.
*   **WAL Framing**: Defines the length-delimited byte layouts, Lsn tracking, and frame-level checksums.
*   **Commit Epochs**: Assigns transaction epochs and coordinates multi-writer serialization.
*   **MVCC Visibility**: Computes version snapshots using epoch comparison rules.
*   **Manifest Publication**: Performs copy-on-write manifest updates and CAS checks.
*   **Module Registry**: Validates, registers, and routes calls to extension modules.
*   **Policy Oracle**: Integrates global and table-level access rules before data retrieval.
*   **Job Registry**: Orchestrates background job queues, leases, and retry states.
*   **Recovery & Backup/Restore**: Ensures consistent WAL replay and file-based state verification.
*   **Explain/Analyze & Metrics Envelopes**: Defines the diagnostic interfaces that modules populate.

By delegating index formats (BM25, HNSW), distance calculations, model connections, and graph traversals to modules, the kernel remains easy to test, maintain, and split in serverless environments.

---

## 2. Standard Modules

Standard modules ship by default with TraceDB, providing the batteries-included feel of an AI-native database while complying with kernel boundaries.

```text
+---------------------------------------------------------------------------------+
|                                 TraceDB Kernel                                  |
|  (Catalog, WAL, Epochs, MVCC, Manifest, Module Registry, Policy Oracle, Jobs)  |
+---------------------------------------------------------------------------------+
         |                  |                 |                  |
+-----------------+ +---------------+ +---------------+ +-----------------+
| tracedb-vector  | | tracedb-text  | | tracedb-graph | | tracedb-features|
| (Vectors & ANN) | | (BM25 & post) | | (Adjacency)   | | (Embeddings)    |
+-----------------+ +---------------+ +---------------+ +-----------------+
         |                  |                 |                  |
+-----------------+ +---------------+ +---------------+ +-----------------+
| tracedb-temporal| |tracedb-policy | |tracedb-proven | |tracedb-retrieval|
| (Time-travel)   | |(ACLs & Masks) | |(Citations)    | |(Suppression)    |
+-----------------+ +---------------+ +---------------+ +-----------------+
```

### 2.1 `tracedb-vector`
*   **Role**: Implements vector types (`VECTOR<F32, N, COSINE|DOT|L2>`, `VECTOR<F16, N>`, `SPARSE_VECTOR`, `MULTIVECTOR`). Handles exact vector scans, local HNSW segment index blocks, distance metric calculations, and prefix/Matryoshka optimizations.

### 2.2 `tracedb-text`
*   **Role**: Implements tokenization, normalizers, symbol/path token rules, inverted postings lists, and BM25 candidate stream scoring.

### 2.3 `tracedb-graph`
*   **Role**: Manages edge tables and incoming/outgoing adjacency pages. Supports weighted traversals, temporal traversals, policy-aware neighborhood expansion, and dynamic edge metadata (e.g., access frequency, decay).

### 2.4 `tracedb-temporal`
*   **Role**: Tracks transaction time and valid time (`TEMPORAL_RANGE`). Executes as-of historical queries and bitemporal diffs.

### 2.5 `tracedb-policy`
*   **Role**: Compiles declarative row-level access rules and tenant contexts into segment-level policy bitmaps. Enforces legal holds and sensitivity-level masks.

### 2.6 `tracedb-provenance`
*   **Role**: Annotates records with source spans, document URLs, tool runs, model versions, and citation offsets.

### 2.7 `tracedb-features`
*   **Role**: Governs the derived feature lifecycle. Evaluates source hashes, coordinates embedding recomputation jobs, and handles STRICT, LAZY, and ALLOW_STALE freshness modes.

### 2.8 `tracedb-retrieval-core`
*   **Role**: Provides native support for suppression state, retrieval metadata overlays, and visibility-safe retrieval. (Advanced stateful recall, reinforcement, and consolidation can be loaded via the optional `tracedb-memory-runtime` module).

---

## 3. Capability Levels (0-6)

Modules are classified by capability level, determining which kernel extension interfaces they must implement. Higher-level modules have larger impact surface areas and require stricter validation.

| Level | Capability Name | Description / Scope | Extension Interfaces |
| :--- | :--- | :--- | :--- |
| **0** | **Function** | Stateless utility functions (e.g., URL normalizers, hash builders). | `FunctionModule` |
| **1** | **Type** | Custom column types and binary layouts (e.g., Vector formats, GeoPoints). | `TypeModule` |
| **2** | **Index** | Local index formats and segment-level summaries (e.g., HNSW, BM25, adjacency pages). | `IndexModule`, `StorageModule` |
| **3** | **Planner** | Heuristic rewrite rules, cost estimators, and stream fusion strategies. | `PlannerModule` |
| **4** | **Storage** | Custom physical block layouts, codecs, and mmap-safe paging models. | `StorageModule` |
| **5** | **Runtime/Job** | Asynchronous operations and background task execution (e.g., embeddings, compactions). | `JobModule` |
| **6** | **Bridge** | Connectors for data importing, exporting, and external replication. | `ImportExportModule` |

---

## 4. Trust & Conformance Levels

### 4.1 Module Trust Classifications
To prevent unverified code from corrupting the database or leaking tenant data, TraceDB enforces four trust levels:

*   **`CORE_SIGNED`**: Code compiled directly into the TraceDB binary. Permitted to implement WAL decoders, physical block layouts, planner optimizations, and recovery hooks.
*   **`FIRST_PARTY_SIGNED`**: Official modules distributed by the TraceDB project. Signed cryptographically and granted the same access permissions as `CORE_SIGNED`.
*   **`THIRD_PARTY_SANDBOXED`**: User-defined extensions executed inside a constrained sandboxed environment. Restricted to stateless functions, custom importers/exporters, and index types certified safe by a conformance suite.
*   **`LOCAL_DEV_UNSAFE`**: Experimental local modules. Requires starting the engine with `--allow-unsafe-modules` and opting-in at the database level. Outputs warnings in manifests, backups, and support logs.

### 4.2 Conformance Rules (The Module Law)
Every module must conform to the database's logical invariants:

```text
If it is queryable, it is cataloged.
If it is durable, it is logged.
If it is visible, it obeys policy.
If it is indexed, it is restorable.
If it is ranked, it is explainable.
If it is background work, it is idempotent and job-cataloged.
```

1.  **No Private WALs**: A module cannot log to its own private write-ahead file. All mutation events must be serialized into the kernel's WAL.
2.  **No Private Stores**: Modules cannot spin up private database files or sidecar databases. All durable structures must be written as blocks within kernel-managed segments.
3.  **No Policy Bypass**: Access checks must use the kernel's visibility oracle and segment bitmaps. Modules cannot filter records post-query in a way that bypasses policy logging.
4.  **No Hidden State**: All background indexing and compaction jobs must be logged in the database manifest and job catalog.
5.  **Auditability**: Access paths must expose cost estimates, candidate budgets, and detailed explain/analyze traces.
