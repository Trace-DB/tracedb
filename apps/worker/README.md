# TraceDB Worker App

Private Railway worker wrapper around `tracedb-worker`.

Workers lease durable jobs, call the private engine API, and never write the
TraceDB volume directly.

