# TraceDB Railway Product Lab

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

Railway is the hosted-alpha/product-lab/simple microservice runtime for TraceDB.
Durable production and science workloads remain AWS-oriented. Treat Railway as a
controlled lab for gateway, simple service, and diagnostic deployments unless a
release runbook explicitly says otherwise.

## Service Layout

Railway product-lab service layout:

- `tracedb-gateway`: the only intended hosted-alpha public HTTP ingress, with API-key auth, routing, rate limiting, and usage metering.
- `tracedb-engine`: private stateful engine, owns `/data/tracedb`, single replica.
- `tracedb-worker`: private worker, consumes queues and mutates state through engine API.
- `../tracedb-benchmarks`: sibling benchmark/proof harness repo. Core TraceDB no longer ships a `tracedb-bench` binary.
- `postgres-catalog`: planned/optional control-plane metadata only, not user records.
- `redis-queue`: planned/optional queues, leases, rate limits, short-lived coordination only.
- `tracedb-bucket`: planned/optional snapshots, exports, restore bundles, benchmark datasets, debug dumps.

Hard rules:

- Services bind to `TRACEDB_BIND` when explicitly set, otherwise `PORT`, otherwise `8080`.
- Private service URLs use `*.railway.internal`.
- `tracedb-engine` is the only service that writes the TraceDB volume.
- Public ingress must be `tracedb-gateway` for hosted-alpha/product-lab access.
- Public `tracedb-engine` exposure is diagnostic-only for temporary benchmark
  or disk validation runs. It is not hosted-alpha ingress and must be removed
  before presenting a Railway environment as gateway-fronted.
- Bucket storage is not hot WAL or active mutable index state.
- Engine initializes `/data/tracedb` at runtime, not during image build.
- The Railway product lab is a TraceDB deployment proving ground, not a
  managed-service promise, not a TraceField runtime claim, not Agent Memory
  Flight Recorder, and not tensor artifact infrastructure.

## Release Images vs Source-Build Diagnostics

Official release deploys should promote an already-built GHCR image into
Railway. Prefer an immutable digest where Railway supports it, or a pinned
release tag otherwise. Do not treat a Railway source rebuild from `main` as an
official release deployment.

The root `railway.toml` and Dockerfile builder path are diagnostic/source-build
conveniences for lab smoke tests. The service-specific TOML files under this
folder are also lab conveniences unless a release runbook pins them to a
published GHCR image/tag/digest.

## Environment Examples

Environment examples are split by role so Railway services do not inherit
unrelated variables:

- `env.gateway.example`: public gateway ingress and private engine routing.
- `env.engine.example`: private stateful engine and volume settings.
- `env.worker.example`: private worker and engine routing.
- `env.benchmark.example`: benchmark runner targets and planned/optional external controls.

Set real tokens and DSNs through Railway variables or CI secrets. Do not copy
secrets into these files.

Postgres, Redis, and S3/object-storage variables are planned/optional unless the
specific lab path being exercised has that integration fully wired. Leave them
unset for minimal engine/gateway smoke tests; set them only for catalog, queue,
snapshot/export, or comparative-baseline experiments that explicitly need them.

## Benchmark and Proof Harness Boundary

Core TraceDB no longer ships `tracedb-bench`. `deploy/railway/railway.bench.toml`
is intentionally comment-only and not release-active. Benchmark/proof harnesses
are deferred to the sibling `../tracedb-benchmarks` repository.

For a diagnostic benchmark phase only, you may start with a temporary public
`tracedb-engine` service so the external benchmark runner can target real
Railway disk without needing the gateway/catalog path first. This is not
hosted-alpha ingress and is not evidence that the public service shape is ready:

```bash
railway login
railway init --name TraceDB
railway add --service tracedb-engine \
  --variables "TRACEDB_SERVICE_MODE=engine" \
  --variables "TRACEDB_DATA_DIR=/data/tracedb" \
  --variables "PORT=8080"
railway volume add --service tracedb-engine --mount-path /data
railway up --service tracedb-engine --detach -m "Deploy TraceDB diagnostic engine"
railway domain --service tracedb-engine --port 8080 --json
```

Then run the benchmark/proof harness from `../tracedb-benchmarks` using its
current instructions, targeting either the gateway or the temporary diagnostic
engine URL. For authenticated gateway benchmarks, set
`TRACEDB_HTTP_BEARER_TOKEN` locally. The runner sends it as a bearer token and
must not write it to reports.

Before calling the environment gateway-fronted or hosted-alpha, add
`tracedb-gateway` as the public service and keep `tracedb-engine` private:

```bash
railway add --service tracedb-gateway \
  --variables "TRACEDB_SERVICE_MODE=gateway" \
  --variables "PORT=8080" \
  --variables "TRACEDB_ENGINE_URL=http://tracedb-engine.railway.internal:8080" \
  --variables "TRACEDB_REQUIRE_API_KEY=true" \
  --variables "TRACEDB_API_TOKEN=<set-a-long-random-token>"
railway up --service tracedb-gateway --detach -m "Deploy TraceDB gateway"
railway domain --service tracedb-gateway --port 8080 --json
```

The root `railway.toml` deploys an engine-shaped source-build diagnostic service.
For gateway and worker services, either set the matching Railway dashboard start
command or use the matching `deploy/railway/railway.*.toml` config path for that
service. For official release promotion, prefer pinned GHCR image deployment
instead of these source-build TOML conveniences.

## Hybrid AWS + Railway

Use AWS for durable/science workloads and Railway for the gateway, simple
microservices, and lab validation.

A Railway `tracedb-gateway` may route to an AWS-hosted `tracedb-engine` only when
all of the following are true:

- The AWS engine endpoint is HTTPS-only.
- The gateway and engine share an internal token or equivalent service-to-service authorization secret.
- Network restrictions limit who can reach the AWS engine endpoint, such as a private network path, allowlist, security-group policy, or equivalent boundary.

If those controls are not in place, keep `tracedb-gateway` and `tracedb-engine`
on the same Railway private network for lab environments and route via
`http://tracedb-engine.railway.internal:8080`.

## Remote Baseline Variables

The real-world runner can target Railway or other hosted competitor services via
planned/optional environment variables:

- `BENCH_POSTGRES_DSN`
- `BENCH_PGVECTOR_DSN`
- `BENCH_MONGO_URI`
- `BENCH_QDRANT_URL`
- `BENCH_OPENSEARCH_URL`
- `TRACEDB_HTTP_URL` or `RAILWAY_TRACEDB_URL`

Use managed services where Railway supports them directly. Qdrant and
OpenSearch should be separate image services with volumes, or hosted externally,
until we formalize Railway templates for them.
