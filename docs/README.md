# TraceDB Documentation

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

This directory contains public documentation for the downloadable TraceDB
database distribution. Hosted TraceDB is a separate proprietary service that
uses the same HTTP contract, SDKs, and protocol docs.

## Start Here

- [Getting Started](getting-started.md)
- [Product Thesis](product/thesis.md)
- [Contributing](../CONTRIBUTING.md)
- [Release Notes](../RELEASE.md)

## Architecture

- [Kernel and Module Design](architecture/kernel-and-modules.md)
- [Candidate Stream Planner](architecture/candidate-streams.md)
- [Durability Semantics v0](durability-semantics-v0.md)

## API And Protocol

- [Platform Contract v0](platform-contract-v0.md), stable repository path
  `docs/platform-contract-v0.md`
- [Platform Contract v0 Manifest](platform-contract-v0.json)
- [v1 HTTP API Reference](api/v1-http.md)
- [OpenAPI v1 Spec](api/v1-openapi.json)

SDK implementations live in the standalone public SDK repositories:
`tracedb-rust`, `tracedb-python`, `tracedb-js`, and the planned `tracedb-go`.

When local executable policy or workstation resources block product checks, use
the remote Linux product verification lane:

```bash
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json
```
