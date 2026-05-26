# Product Thesis: TraceDB

## 1. Core Vision

Modern AI-native applications suffer from architectural complexity due to the separation of application state (stored in relational databases) and semantic retrieval state (stored in vector databases, search indexes, graph databases, and memory layers). This separation introduces the fundamental problem of **external sync drift**.

TraceDB collapses these boundaries with a simple, non-negotiable core vision:

```text
One logical record.
One transaction epoch.
Many native views.
No external sync drift.
```

Under this model, TraceDB stores and queries:
*   Structured rows
*   Typed columns
*   Vectors/embeddings
*   Full-text indexes
*   Graph edges
*   Temporal versions
*   Provenance
*   Policy/security state
*   Derived AI features (with activation/suppression metadata)
*   Background feature jobs

These capabilities are managed under:
*   One write-ahead log (WAL)
*   One visibility model
*   One query planner
*   One storage engine
*   One consistency boundary

Instead of treating vectors, text search, or graph relationships as separate data stores or sidecar plugins, TraceDB treats them as native views over the same committed logical record. When a record is updated:
1.  The logical record change is committed to the WAL.
2.  The update is instantly visible through hot overlays (maintaining transactional consistency).
3.  Asynchronous background indexing composes optimized index blocks (HNSW, BM25 postings, graph adjacencies) per sealed segment.
4.  Derived features (like embeddings) are marked dirty and scheduled for recomputation without manual pipeline glue.

---

## 2. What TraceDB Replaces

In serious production stacks, developers typically combine multiple distinct technologies to build AI applications. This leads to common failure modes such as embedding drift, stale search results, separate relational and vector truths, post-query security filtering leaks, and dirty vectors hidden from the application.

TraceDB is designed to directly replace the following fragile combinations:

*   **Postgres + pgvector**: Eliminates pgvector's limitations as an extension in a general-purpose engine by providing a unified relational-vector query planner.
*   **Postgres + Vector DB (e.g., Qdrant, Pinecone, Weaviate)**: Eliminates application-side synchronization, complex ID mapping, and post-query metadata filtering.
*   **Postgres + Vector DB + Search Engine (e.g., Elasticsearch, Meilisearch)**: Eliminates split-brain indexing between full-text searches and vector lookups, replacing them with a unified candidate-stream fusion model.
*   **Postgres + Search Engine + Graph DB (e.g., Neo4j)**: Collapses entity/symbol relationships, text search, and row data into a single transactional kernel.
*   **SQLite + Vector Sidecar**: Provides a clean, lightweight, single-directory local/embedded database option with native semantic capabilities.
*   **AI Memory Tables + External Embeddings / Sync Jobs**: Replaces application-side RAG orchestration pipelines with native database-level derived feature tracking.

---

## 3. Scope of the Alpha (What TraceDB is NOT)

During its alpha phase, TraceDB does not try to replace every mature database feature or chase standard relational database parity. 

### What TraceDB does NOT try to be in Alpha:
*   **Full SQL Standard Compatibility**: TraceDB prioritizes a SQL-ish dialect and its native hybrid query language, `TraceQL`, which is easier for AI agents to write.
*   **Postgres Wire Compatibility**: The engine uses its own native protocols rather than pretending to be Postgres.
*   **Distributed Consensus & Multi-Region Replication**: The focus is on a correct, single-writer stateful engine.
*   **Advanced Analytical Warehouse Workloads**: Not designed for massive, arbitrary OLAP scanning.
*   **Arbitrary Stored Procedures**: Parity with complex PL/pgSQL engines is out of scope.
*   **Global Serverless Compute/Storage Split**: The alpha runs on Railway using a single stateful engine instance with a mounted persistent volume, verifying local durability, recovery, and indexing before splitting the planes.

### What TraceDB MUST win at in Alpha:
*   Correct, crash-safe, local and hosted AI-native app state.
*   Semantic retrieval with zero sync drift.
*   Policy-safe filtered vector search (applying policies before ANN traversal).
*   Freshness-aware ranking (exposing dirty, stale, or missing embeddings as queryable database facts).

---

## 4. User Personas

TraceDB is designed for developers building stateful, semantic, and agentic software.

### 4.1 AI Application Developer
*   **Objective**: Build chat platforms, knowledge bases, or workflow automations without gluing five databases together.
*   **Requirements**: Simple storage and retrieval of messages, user memories, documents, attachments, and tool runs. Needs tenant safety (ACLs) and reliable hybrid search (vector + keyword) that doesn't leak data.

### 4.2 Coding-Agent Platform Developer
*   **Objective**: Build autonomous coding agents and code-search engines that reason over files, commits, and symbols.
*   **Requirements**: Hybrid retrieval over files, code symbols, AST nodes, git commits, issues, tool runs, agent memories, and error traces. Exact symbol lookup must work flawlessly alongside semantic code-context matching.

### 4.3 Business Software Developer
*   **Objective**: Add AI capabilities to traditional relational business systems (e.g., CRMs, helpdesks).
*   **Requirements**: Relational tables for customers, tickets, orders, and contracts that natively support semantic lookup. Needs transactional guarantee that when a ticket status changes, its retrieval state stays consistent.

### 4.4 Local-First AI Developer
*   **Objective**: Build desktop applications, local agents, or privacy-centric tools.
*   **Requirements**: Zero cloud dependencies, single local database directory, fast startup, portable file format, and simple backups.

### 4.5 Research / Agent-Memory Developer
*   **Objective**: Implement advanced cognitive architectures, long-term memory systems, and episodic recall.
*   **Requirements**: Native support for stateful recall, activation overlays (recently accessed records), suppression/inhibition (contradicted or stale facts), surprise-aware write hooks, and background consolidation jobs.
