# TraceDB Gateway App

Thin Railway service wrapper around `tracedb-gateway`.

The gateway binary lives in **`crates/tracedb-gateway/`**.
The server binary that runs it lives in **`crates/tracedb-server/`**.

Start command:

```bash
TRACEDB_SERVICE_MODE=gateway tracedb-server
```

Public endpoints are routed through the gateway. The gateway owns API-key checks,
org/project/database/branch routing, rate limits, request shaping, and metering.

For the current invite-alpha path, the gateway supports two auth modes:

- Legacy shared-token alpha: `TRACEDB_REQUIRE_API_KEY=true` plus
  `TRACEDB_API_TOKEN=<secret>`.
- File-backed per-tester keys: set `TRACEDB_API_KEYS_PATH` and issue
  `tdb_live_...` keys with the admin-only `/v1/gateway/api-keys` routes. The
  registry stores key hashes and metadata only; the raw key is returned once.

The key registry is manual invite-alpha infrastructure, not self-serve API-key
management, billing, or a customer dashboard.
