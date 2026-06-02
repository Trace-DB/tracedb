# Architecture Decision Record (ADR)

# Docker Over Railpack for Database Services

* **Status:** Accepted
* **Date:** 2026-05-09
* **Tags:** `tracedb/decisions`, `tracedb/railway`

---

## 1. Context and Problem Statement

TraceDB compiles into multiple distinct core service binaries, including the
storage engine, API gateway, and queue worker. Benchmark/proof runners are now
owned by the sibling `../tracedb-benchmarks` repository and target the core
HTTP surfaces. To establish reliable testing and deployment loops, we must
guarantee identical container image behavior across core operational surfaces.
These surfaces include:
* Local embedded development
* Docker Compose local cloud environments
* Railway production labs
* Future Kubernetes clusters or alternative container runtimes
* Self-hosted or MIT-licensed distribution setups

Railway offers **Railpack** (an automatic builder infrastructure) as an alternative to writing custom Dockerfiles. We need to decide whether to adopt Railpack or stick to standard Dockerfile builds for our core database service images.

---

## 2. Decision

We will use **Dockerfile builds** as the canonical build path for the following core images:
* `tracedb-engine`
* `tracedb-gateway`
* `tracedb-worker`

---

## 3. Rationale

Using Dockerfile builds provides explicit control over compiling and packaging:
1. **Workspace Multi-Binaries:** The Rust cargo workspace produces multiple distinct binaries. Dockerfiles let us explicitly select compile targets, package dependencies, and structure multi-stage builds.
2. **Predictable Image Contracts:** Standardizing on Dockerfiles ensures that the exact same compiled binary artifacts, environment expectations, default ports, and data directory assumptions (`/data/tracedb`) carry over from Docker Compose on local machines to Railway nodes in the cloud.
3. **No Autodetection Surprises:** Relying on Railpack introduces builder heuristics that can drift or change over time. Explicit Dockerfiles prevent unexpected detection failures during compilation.
4. **Environment Portability:** By keeping the build definition inside standard Dockerfiles, the project avoids vendor lock-in, enabling easy migrations to standard container runtimes (such as Kubernetes) or self-hosted deployments.

---

## 4. Railpack Position

Railpack is not discarded entirely. It may be reconsidered in the future for non-core, single-purpose components of the TraceDB project, including:
* Documentation sites
* Auxiliary administration dashboards
* Helper utilities
* Pure Python benchmark workers or script runners

It remains excluded from all core database engines, gateways, and workers.

---

## 5. Constraints and Security Boundaries

* **No Docker-in-Docker (DinD):** The running TraceDB containers do not run Docker or Docker Compose internally.
* **Remote Railway Builds:** Railway reads the Dockerfile in the repository and executes the image build steps remotely on its build nodes. The resulting containers execute as lightweight, isolated processes.
