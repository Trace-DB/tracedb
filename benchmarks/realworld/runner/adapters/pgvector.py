from __future__ import annotations

import os
from typing import Any

from .base import BenchmarkAdapter, optional_import, query_result_record
from ..metrics import MetricRecorder, mrr_at_k, ndcg_at_k, recall_at_k
from ..types import DatasetBundle, RunConfig


class PgVectorAdapter(BenchmarkAdapter):
    name = "pgvector"
    role = "vector in relational database"

    def run(self, dataset: DatasetBundle, config: RunConfig) -> dict[str, Any]:
        psycopg = optional_import("psycopg")
        if psycopg is None:
            return self.unavailable("python dependency missing: psycopg", dataset)
        dsn = os.environ.get(
            "BENCH_PGVECTOR_DSN",
            "postgresql://tracedb:tracedb@localhost:25433/tracedb_bench",
        )
        try:
            with psycopg.connect(dsn, connect_timeout=2) as conn:
                with conn.cursor() as cur:
                    setup_recorder = MetricRecorder()
                    ingest_recorder = MetricRecorder()
                    commit_recorder = MetricRecorder()
                    query_recorder = MetricRecorder()
                    setup_recorder.timed(lambda: _setup_table(cur, dataset))
                    for record in dataset.records:
                        ingest_recorder.timed(
                            lambda record=record: _insert_record(cur, record, dataset)
                        )
                    commit_recorder.timed(conn.commit)
                    disk_bytes = _table_disk_bytes(cur)
                    recalls = []
                    ndcgs = []
                    mrrs = []
                    query_results = []
                    for query in dataset.queries:
                        rows = query_recorder.timed(
                            lambda query=query: cur.execute(
                                """
                                SELECT id
                                FROM bench_vectors
                                WHERE tenant_id = %s AND category = %s
                                ORDER BY embedding <=> %s::vector
                                LIMIT %s
                                """,
                                (
                                    query.tenant_id,
                                    query.category,
                                    _vector_literal(query.vector, dataset.embedding_dimensions),
                                    query.top_k,
                                ),
                            ).fetchall()
                        )
                        ids = [row[0] for row in rows]
                        query_results.append(query_result_record(query, ids))
                        recalls.append(recall_at_k(query.expected_ids, ids, query.top_k))
                        ndcgs.append(ndcg_at_k(query.expected_ids, ids, query.top_k))
                        mrrs.append(mrr_at_k(query.expected_ids, ids, query.top_k))
            query_summary = query_recorder.summary()
            ingest_summary = ingest_recorder.summary()
            setup_summary = setup_recorder.summary()
            commit_summary = commit_recorder.summary()
            ingest_transaction_total_ms = round(
                sum(ingest_recorder.latencies_ms) + sum(commit_recorder.latencies_ms),
                3,
            )
            metrics = dict(query_summary)
            metrics.update(
                {
                    f"query_{key}": value
                    for key, value in query_summary.items()
                }
            )
            metrics.update(
                {
                    f"ingest_{key}": value
                    for key, value in ingest_summary.items()
                }
            )
            metrics.update(
                {
                    f"setup_{key}": value
                    for key, value in setup_summary.items()
                }
            )
            metrics.update(
                {
                    f"ingest_commit_{key}": value
                    for key, value in commit_summary.items()
                }
            )
            metrics.update(
                {
                    "ingest_count": len(dataset.records),
                    "ingest_transaction_count": 1,
                    "ingest_transaction_total_latency_ms": ingest_transaction_total_ms,
                    "single_transaction_row_insert_latency_p95_ms": ingest_summary[
                        "latency_p95_ms"
                    ],
                    "single_transaction_commit_latency_p95_ms": commit_summary[
                        "latency_p95_ms"
                    ],
                    "query_count": len(dataset.queries),
                    "failure_count": 0,
                    "recall_at_5": round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
                    "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
                    "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
                    "disk_bytes": disk_bytes,
                }
            )
            return self.ok_result(
                dataset,
                metrics,
                [
                    "real pgvector metadata-filtered vector workload executed through psycopg",
                    "pgvector ingest latency is per-row insert inside one bulk transaction; commit latency is reported separately",
                    "pgvector storage bytes measured with pg_total_relation_size after load and index creation",
                ],
                query_results=query_results,
            )
        except Exception as error:
            if config.require_services:
                raise
            return self.unavailable(f"pgvector unavailable: {error}", dataset)


def _setup_table(cur: Any, dataset: DatasetBundle) -> None:
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    cur.execute("DROP TABLE IF EXISTS bench_vectors")
    cur.execute(
        f"""
        CREATE TABLE bench_vectors (
          id text PRIMARY KEY,
          tenant_id text NOT NULL,
          category text NOT NULL,
          body text NOT NULL,
          embedding vector({dataset.embedding_dimensions})
        )
        """
    )
    cur.execute("CREATE INDEX bench_vectors_tenant_category ON bench_vectors (tenant_id, category)")


def _insert_record(cur: Any, record: Any, dataset: DatasetBundle) -> None:
    cur.execute(
        """
        INSERT INTO bench_vectors
          (id, tenant_id, category, body, embedding)
        VALUES (%s, %s, %s, %s, %s::vector)
        """,
        (
            record.record_id,
            record.tenant_id,
            record.category,
            record.body,
            _vector_literal(record.vector, dataset.embedding_dimensions),
        ),
    )


def _table_disk_bytes(cur: Any) -> int:
    row = cur.execute(
        "SELECT pg_total_relation_size('public.bench_vectors'::regclass)"
    ).fetchone()
    return int(row[0]) if row else 0


def _vector_literal(vector: list[float], dimensions: int) -> str:
    return "[" + ",".join(f"{value:.6f}" for value in vector[:dimensions]) + "]"
