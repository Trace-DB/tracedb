# TraceDB Engine App

Private Railway service wrapper around `tracedb-server` in engine mode.

Start command:

```bash
TRACEDB_SERVICE_MODE=engine TRACEDB_DATA_DIR=/data/tracedb tracedb-server
```

Only the engine writes the TraceDB volume. Gateway and workers must mutate
database state through the private engine API.

