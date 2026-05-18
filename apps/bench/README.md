# TraceDB Bench App

Thin wrapper for `tracedb-bench` workload descriptors and Railway benchmark
jobs for 100k, 1M, 10M, codebase, document, and mixed metadata corpora.

Current benchmark truth lives under `benchmarks/realworld/`. As of the
`88c9223` closeout, the working lane is Modal actual-engine HTTP batch ingest
against pgvector controls with exported bundles and source-provenance manifests.
TraceDB is semi-working as a benchmark subject, but pgvector remains the
1024-record generated-smoke number to beat on query p95, transaction ingest, and
storage.
