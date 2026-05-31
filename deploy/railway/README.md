# TraceDB Railway Product Lab

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

Railway product-lab service layout:

- `tracedb-gateway`: the only intended hosted-alpha public HTTP ingress, with API-key auth, routing, rate limiting, and usage metering.
- `tracedb-engine`: private stateful engine, owns `/data/tracedb`, single replica.
- `tracedb-worker`: private worker, consumes queues and mutates state through engine API.
- `tracedb-bench`: bounded benchmark jobs.
- `postgres-catalog`: control-plane metadata only, not user records.
- `redis-queue`: queues, leases, rate limits, short-lived coordination only.
- `tracedb-bucket`: snapshots, exports, restore bundles, benchmark datasets, debug dumps.

Hard rules:

- Services bind to `TRACEDB_BIND` when explicitly set, otherwise `PORT`, otherwise `8080`.
- Private service URLs use `*.railway.internal`.
- `tracedb-engine` is the only service that writes the TraceDB volume.
- Public `tracedb-engine` exposure is diagnostic-only for temporary benchmark
  or disk validation runs. It is not hosted-alpha ingress and must be removed
  before presenting a Railway environment as gateway-fronted.
- Bucket storage is not hot WAL or active mutable index state.
- Engine initializes `/data/tracedb` at runtime, not during image build.
- The Railway product lab is a TraceDB deployment proving ground, not a
  managed-service promise, not a TraceField runtime claim, not Agent Memory
  Flight Recorder, and not tensor artifact infrastructure.

## Environment Examples

Environment examples are split by role so Railway services do not inherit
unrelated variables:

- `env.gateway.example`: public gateway ingress and private engine routing.
- `env.engine.example`: private stateful engine and volume/bucket settings.
- `env.worker.example`: private worker queue and engine routing.
- `env.benchmark.example`: benchmark runner target and optional controls.

Set real tokens and DSNs through Railway variables or CI secrets. Do not copy
secrets into these files.

## Current Benchmark Deployment Shape

For the benchmark phase, use Railway for the stateful services and keep the Mac
as the orchestration/reporting machine.

For a diagnostic benchmark phase only, you may start with a temporary public
`tracedb-engine` service so the benchmark runner can target real Railway disk
without needing the gateway/catalog path first. This is not hosted-alpha ingress
and is not evidence that the public service shape is ready:

```bash
railway login
railway init --name TraceDB
railway add --service tracedb-engine \
  --variables "TRACEDB_SERVICE_MODE=engine" \
  --variables "TRACEDB_DATA_DIR=/data/tracedb" \
  --variables "PORT=8080"
railway volume add --service tracedb-engine --mount-path /data
railway up --service tracedb-engine --detach -m "Deploy TraceDB benchmark engine"
railway domain --service tracedb-engine --port 8080 --json
```

Then run from the Mac:

```bash
RAILWAY_TRACEDB_URL=https://<generated-domain> \
OPENROUTER_MODE=required \
RECORDS=1000 \
benchmarks/realworld/scripts/run_railway_target.sh
```

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

The root `railway.toml` deploys the engine-shaped service. For gateway and
worker services, either set the matching Railway dashboard start command or use
the matching `deploy/railway/railway.*.toml` config path for that service.

For authenticated gateway benchmarks, set `TRACEDB_HTTP_BEARER_TOKEN` locally.
The runner sends it as a bearer token and does not write it to reports.

## Remote Baseline Variables

The real-world runner can target Railway or other hosted competitor services via
environment variables:

- `BENCH_POSTGRES_DSN`
- `BENCH_PGVECTOR_DSN`
- `BENCH_MONGO_URI`
- `BENCH_QDRANT_URL`
- `BENCH_OPENSEARCH_URL`
- `TRACEDB_HTTP_URL` or `RAILWAY_TRACEDB_URL`

Use managed services where Railway supports them directly. Qdrant and
OpenSearch should be separate image services with volumes, or hosted externally,
until we formalize Railway templates for them.
