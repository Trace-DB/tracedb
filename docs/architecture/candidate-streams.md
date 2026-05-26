# Candidate Stream Planner: TraceDB

## 1. Transactional Candidate-Stream Planner Model

TraceDB rejects the conventional two-step pattern of querying a database for metadata and calling a separate service for vector search. Instead, it relies on a **transactional candidate-stream planner**. 

The planner compiles queries (written in TraceQL or a SQL-ish dialect) into a unified logical AST. It evaluates the query against the database's current transactional snapshot and schedules the execution of multiple parallel, lightweight candidate streams.

```text
       [ Query AST ]
             │
             ▼
  [ Predicate Normalization ]
             │
             ▼
  [ Visibility Mask Planning ]  <-- (Compute tenant / ACL bitmaps first)
             │
             ▼
  [ Access Path Enumeration ]   <-- (Select Relational, Lexical, Vector, etc.)
             │
             ▼
 [ Candidate Stream Scheduling ] <-- (Open streams under budget limits)
             │
             ▼
    [ Candidate Fusion ]        <-- (Reciprocal Rank Fusion / Scoring)
             │
             ▼
  [ Late Materialization ]       <-- (Load full row payloads by ID)
             │
             ▼
  [ Final Visibility Guard ]     <-- (Safety net policy check)
             │
             ▼
      [ Rerank & Project ]
```

---

## 2. The `AccessPath` Trait

Every retrieval pathway in TraceDB (relational indices, text postings, vector graphs, temporal tables, and custom modules) implements the same unified interface. This trait allows the planner to pull batches of candidates incrementally, refine search parameters mid-query, and stop execution once confidence targets are met.

```rust
pub trait AccessPath {
    /// Estimates the execution cost and candidate selectivity of the path
    fn estimate(&self, predicates: &[Predicate], stats: &DatabaseStats) -> CostEstimate;

    /// Opens the candidate stream under a visibility mask and work budget
    fn open(
        &self,
        query: QueryFragment,
        snapshot: &ReadSnapshot,
        visibility: &VisibilityMask,
        budget: CandidateBudget,
    ) -> Result<Box<dyn CandidateStream>, TraceError>;
}

pub trait CandidateStream {
    /// Pulls the next batch of candidates under a strict work budget
    fn next_batch(&mut self, budget: WorkBudget) -> Result<CandidateBatch, TraceError>;

    /// Provides dynamic feedback to refine access path parameters (e.g., ANN pruning metrics)
    fn refine(&mut self, feedback: PlannerFeedback);

    /// Generates diagnostic trace info for EXPLAIN/ANALYZE output
    fn explain(&self) -> AccessPathExplain;
}
```

### Candidate Output
Streams output uniform candidate records containing score bounds and lineage:

```rust
struct Candidate {
    record_id: RecordId,
    version_id: VersionId,
    score_components: ScoreComponents,
    score_upper_bound: Option<f32>,
    source: AccessPathId,
    freshness: FeatureFreshness,
    visibility_checked: bool,
}
```

---

## 3. Candidate Fusion & Reciprocal Rank Fusion (RRF)

When multiple access streams (e.g., text keyword and vector semantic) return candidate lists, TraceDB merges their ranks and scores using candidate fusion.

### 3.1 Reciprocal Rank Fusion (RRF)
RRF computes a unified rank score by summing the reciprocal of each candidate's rank in the active streams:

$$\text{RRF\_Score}(d \in D) = \sum_{m \in M} \frac{w_m}{k + r_m(d)}$$

*   $M$: The set of active access paths (e.g., Lexical, Vector, Graph).
*   $r_m(d)$: The 1-based rank of document $d$ in the candidate stream of path $m$. If $d$ is missing from path $m$, $r_m(d) = \infty$.
*   $w_m$: A configurable weight multiplier for path $m$ (e.g., `vector_weight = 0.65`, `text_weight = 0.35`).
*   $k$: A smoothing constant (default: 60) to prevent top ranks from disproportionately overpowering lower ranks.

### 3.2 Score Decomposition & Penalties
The final score of a materialized candidate is a combination of base rankings and dynamic runtime attributes:

$$\text{final\_score} = \text{vector\_score} + \text{lexical\_score} + \text{graph\_score} + \text{relational\_score} + \text{activation\_score} - \text{suppression\_penalty} - \text{freshness\_penalty}$$

*   **Freshness Penalty**: Dynamically calculated based on the feature state. A dirty embedding with an unchanged source hash receives a minor penalty; a dirty embedding with a modified source hash receives a large penalty.
*   **Temporal Decay**: Multiplies candidate scores by time-based weights (e.g., exponential decay for older memories).
*   **Suppression Penalty**: Applied to records with a high suppression coefficient in retrieval-core, burying contradicted or cooling facts.

---

## 4. Derived Feature Freshness Modes

Derived features (such as embeddings generated from text fields) have distinct lifecycle states (`READY`, `DIRTY`, `PENDING`, `FAILED`, `MISSING`). The query planner handles these states through four freshness modes:

*   **`STRICT`**:
    *   **Behavior**: Rejects all `DIRTY`, `FAILED`, `MISSING`, or `PENDING` features.
    *   **Fallback**: Excludes the vector access path for those records, falling back to lexical, relational, or graph paths.
*   **`LAZY`**:
    *   **Behavior**: Uses `READY` features as-is. For dirty or missing records, it bypasses the vector index, routes retrieval through alternative access paths, and schedules background feature jobs to recompute the embeddings.
*   **`ALLOW_STALE`**:
    *   **Behavior**: Accepts `DIRTY` features if their current age or source model version falls within a caller-declared freshness window. Applies a small freshness penalty to the candidate score.
*   **`ON_READ`**:
    *   **Behavior**: Automatically triggers synchronous embedding recomputation during query execution for missing or dirty records, using a latency budget.

---

## 5. Policy-Safe Retrieval Pushdown

To prevent data leaks in multi-tenant environments, TraceDB enforces policies during the candidate generation phase, rather than filtering results after retrieval.

```text
               [ Relational / Index Segment ]
                             │
                             ▼
  [ Level 1 Partitioning ]   --> Hard isolate by tenant_id, workspace_id
                             │
                             ▼
  [ Level 2 Policy Bitmaps ] --> Apply ACL, sensitivity & retention masks
                             │
                             ▼
  [ Level 3 Path Selector ]  --> Selective Mask?
                                   ├── YES: Exact fallback (bitmap -> exact dist)
                                   └── NO:  Graph traversal (ANN HNSW)
                             │
                             ▼
  [ Late Materialization ]
                             │
                             ▼
  [ Level 4 Final Guard ]    --> Mandatory visibility check (rejection only)
```

### Level 1: Hard Partitioning
`database_id`, `branch_id`, `tenant_id`, and `workspace_id` isolate segment groups. Queries cannot cross these boundaries unless the actor possesses global administration credentials.

### Level 2: Segment-Level Policy Bitmaps
Before candidate generation, the policy engine compiles tenant, workspace, visibility, sensitivity, and retention rules into segment-local policy bitmaps. These bitmaps are passed down to the `AccessPath` streams. Vector and text indexes use these bitmaps to skip scoring records that the actor is forbidden to read.

### Level 3: Exact Fallback for Highly Selective Filters
Traversing approximate nearest neighbor (ANN) graphs with highly selective filters (e.g., searching for a vector matching a specific tenant or metadata flag) leads to the **pre-filtering problem** (where traversals get stuck in disconnected graph regions). 
TraceDB solves this dynamically: if the policy bitmap reveals that only a tiny fraction of records in a segment are visible, the planner bypasses the ANN graph traversal entirely. It uses the policy bitmap to locate the visible record IDs and runs an exact vector distance calculation over them.

### Level 4: Final Visibility Guard
As a safety backstop, a final visibility guard runs on the late-materialized result set. This guard can only discard rows; it can never add hidden records back.
