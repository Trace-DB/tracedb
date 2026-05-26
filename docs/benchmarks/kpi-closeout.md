# TraceDB KPI Closeout & Benchmarks

This document synthesizes the KPI observations, benchmark results, and scientific method logs for the TraceDB project. The primary source files include the TraceDB KPI Closeout Observations, TraceDB KPI Testing Loop, Modal DB Suite Benchmark Plan, and the Real-World Benchmark Lab.

## Current System State
As of the closeout checkpoint, TraceDB is functional as a development system, but it is not yet a product benchmark winner.
- **Canonical Repository:** `/Users/zgrogan/Repos/tracedb`
- **Benchmark Source Commit:** `88c9223acbf99060b918d23e41f8c77721ec202a` ("bench: split store apply write timing")
- **Repo State at Benchmark:** Clean `main`, source_dirty `false`
- **Working Surfaces:** 
  - **Product Engine:** HTTP API, `/v1/records/put-batch` batch ingest, query, explain, report summaries, exported bundles, and pgvector control comparison.
  - **Benchmark Harness:** Modal app-identity repeats, redacted manifests, source commit/dirty-state provenance, compact summary JSON, and full tar bundles.
  - **Evidence Classification:** Generated-smoke development evidence with a real `pgvector` external control.

---

## Median Performance Benchmarks vs. pgvector Control (1024 Records)
Benchmarks were executed on Modal CPU/RAM workers using a generated dataset of **1024 records** (seed `42`, dataset digest `9b9fe4114d4539b9c122daed4dff0d3817a756b0f78dbc548baffcbdfd631bec`).

### Comparative Metrics Table

| Metric / KPI | pgvector Control (Median) | TraceDB Ingest (Median) | Gate Result |
| :--- | :--- | :--- | :--- |
| **Query Latency (p95)** | **1.348 ms** | 2.355 ms | **Failed** (pgvector is faster) |
| **Transaction Ingest (Total)** | **184.992 ms** | 216.380 ms | **Failed** (pgvector is faster) |
| **Storage (After Ingest)** | **335,872 B** | 495,401 B | **Failed** (pgvector is smaller) |
| **Storage (After Workload)** | N/A | 1,656,922 B | **Failed** (TraceDB storage inflates) |
| **Recall / nDCG / MRR @ 5** | 0.233 / 0.375 / 1.000 | 0.233 / 0.375 / 1.000 | **Tied** (No quality regression) |

### Gate Verdicts
- **Query Latency Gate:** Rejected. pgvector remains faster.
- **Ingest Latency Gate:** Rejected. pgvector remains faster for single transactions vs. TraceDB batch transactions.
- **Storage Gate:** Rejected. pgvector relation storage is smaller.
- **Quality Gate:** Accepted (tied).
- **Product Claim Status:** Rejected.
- **Development Checkpoint Status:** Accepted.

---

## Store-Apply Latency Attribution
Through split timing instrumentation added in `88c9223`, Modal store application is no longer a black box. Below are the median subphase write latencies for TraceDB batch writes at 1024 records:

| Subphase / Phase | Median Latency (ms) | Percentage of `store_apply` | Description / Notes |
| :--- | :--- | :--- | :--- |
| **`store_apply` (Total)** | **138.739 ms** | 100.0% | Parent span enclosing replacement write operations. |
| ├─ `store_apply_features` | 47.314 ms | 34.1% | Feature-state and source-hash work. |
| ├─ `store_apply_install` | 46.981 ms | 33.9% | In-memory version install. |
| ├─ `store_apply_fields` | 26.969 ms | 19.4% | Field map cloning and ID/tenant insertion. |
| ├─ `store_apply_key` | 16.284 ms | 11.7% | Tenant and key construction. |
| ├─ `store_apply_validate_identity` | 3.191 ms | 2.3% | Initial record identity validation. |
| ├─ `store_apply_validate_vector` | 0.293 ms | 0.2% | Vector dimension checks. |
| └─ `store_apply_finalize_identity` | 0.128 ms | 0.1% | Final identity validation. |
| **`wal_total`** | **3.222 ms** | N/A | WAL frame append and fsync (not dominant). |
| **`manifest_total`** | **0.534 ms** | N/A | Manifest update write (not dominant). |

### Key Attributions & Insights
- **Write-Path Bottlenecks:** The dominant drivers of transaction latency on Modal are **feature-state/source-hash work** and **in-memory version install**, representing ~68% of the total store-apply time.
- **WAL and Manifest Exonerated:** Write-Ahead Log (WAL) logging and manifest updates are not the primary latency bottlenecks in this environment.
- **Local vs. Remote Mismatch:** Local runs show much lower `store_apply` times (~12.02 ms) and higher relative WAL impact. Modal replication exhibits severe environment-specific store-apply amplification. This highlights that local-only optimizations do not translate directly to production/Modal environments.
- **Regression Note:** Compared with commit `d848566` (unused return clone removal), the median batch transaction latency rose slightly from `246.828 ms` to `216.380 ms` (with `store_apply` going from `161.596 ms` to `138.739 ms`), but pgvector remains faster.

---

## Footprint KPIs
- **TraceDB Ingest Footprint:** 495,401 B. This represents a minor regression compared to `d848566` (which had `486,167 B`), treated as a separate footprint target.
- **TraceDB After-Workload Footprint:** 1,656,922 B. There is substantial size amplification following workload execution, which is much higher than after-ingest and far exceeds pgvector's storage footprint of `335,872 B`.
- **Primary Footprint Target:** The Write-Ahead Log (WAL) payload represents the largest block of after-ingest storage. Reducing the WAL format size is a critical footprint goal.

---

## Scientific Method Observations

Every benchmark run complies with a rigorous 10-stage scientific loop:

1. **Observation:** TraceDB is functional enough for development (HTTP API, batch ingest, query, pgvector controls, and exported bundles work), but it fails to beat pgvector on latency and footprint.
2. **Question:** Can the recurring KPI loop stop at a stable checkpoint while preserving enough evidence to resume safely?
3. **Research Grounding:** Evaluated paths like `/v1/records/put-batch`, batch timing structures, local-only vs. Modal filesystem behaviors, and prior control runs.
4. **Hypothesis:** A closeout checkpoint plus hub updates preserves state better than continuing the heartbeats, which run the risk of context drift.
5. **Experiment / Testing:** Validated three clean Modal repeats (`modal-tracedb-storeapply-subphase-88c9223-r1024-a/b/c`) against same-run pgvector controls.
6. **Data Collection:** Captured summary JSONs, redacted environment manifests, stable generated dataset digest (`9b9fe4114d4539b9c122daed4dff0d3817a756b0f78dbc548baffcbdfd631bec`), and exported bundle tarballs.
7. **Analysis:** The new subphase instrumentation successfully narrowed the bottleneck suspects to feature hash, install, field, and key subphases, but the overall product gates failed.
8. **Conclusion:** Close the loop. Establish commit `88c9223` as the benchmark source commit.
9. **Replication & Peer Review:** Audited by Research Scout, Benchmark Engineer, and the Vault/Obsidian audit agents.
10. **Theory Building:** TraceDB near-term strategy must prioritize making the engine measurable rather than asserting premature performance claims.

---

## Next Test Target
If the KPI loop is explicitly resumed, it must target a single, narrow task:
- **Task:** Reduce or prove irreducible the `88c9223` Modal `store_apply_features`, `store_apply_install`, `store_apply_fields`, and `store_apply_key` cost families.
- **Control:** Same-run pgvector at 1024 generated records.
- **Gate:** TraceDB median and max transaction ingest must beat pgvector without regressing query p95, recall/nDCG/MRR, or after-ingest bytes.
- **Methodology:** 
  1. Same-machine parent/branch microbenchmark first.
  2. Three clean Modal app-identity repeats with exported bundles.
- **Alternative (Product Positioning):** If the next phase focuses on product claims, switch entirely to external-qrels CodeSearchNet/SciFact retrieval lanes instead of using generated-smoke datasets.
