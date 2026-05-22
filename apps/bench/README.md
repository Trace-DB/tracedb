# TraceDB Bench App

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

Thin wrapper for `tracedb-bench` workload descriptors and Railway benchmark
jobs for 100k, 1M, 10M, codebase, document, and mixed metadata corpora.

Current benchmark truth lives under `benchmarks/realworld/`. As of the
`88c9223` closeout, the working lane is Modal actual-engine HTTP batch ingest
against pgvector controls with exported bundles and source-provenance manifests.
TraceDB has development benchmark evidence as a benchmark subject, but pgvector
remains the 1024-record generated-smoke number to beat on query p95,
transaction ingest, and storage.
This app does not make TraceField runtime, Agent Memory Flight Recorder, tensor
artifact, fundraising, or product-win claims.
