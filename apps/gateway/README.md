# TraceDB Gateway App

Thin Railway service wrapper around `tracedb-gateway`.

Start command:

```bash
TRACEDB_SERVICE_MODE=gateway tracedb-server
```

Public endpoints are routed through the gateway. The gateway owns API-key checks,
org/project/database/branch routing, rate limits, request shaping, and metering.

