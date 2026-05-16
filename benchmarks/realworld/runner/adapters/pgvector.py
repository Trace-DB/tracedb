from __future__ import annotations

import os
from typing import Any

from .base import BenchmarkAdapter, optional_import
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
                    cur.executemany(
                        """
                        INSERT INTO bench_vectors
                          (id, tenant_id, category, body, embedding)
                        VALUES (%s, %s, %s, %s, %s::vector)
                        """,
                        [
                            (
                                record.record_id,
                                record.tenant_id,
                                record.category,
                                record.body,
                                _vector_literal(record.vector, dataset.embedding_dimensions),
                            )
                            for record in dataset.records
                        ],
                    )
                    conn.commit()
                    recorder = MetricRecorder()
                    recalls = []
                    ndcgs = []
                    mrrs = []
                    for query in dataset.queries:
                        rows = recorder.timed(
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
                        recalls.append(recall_at_k(query.expected_ids, ids, query.top_k))
                        ndcgs.append(ndcg_at_k(query.expected_ids, ids, query.top_k))
                        mrrs.append(mrr_at_k(query.expected_ids, ids, query.top_k))
            metrics = recorder.summary()
            metrics.update(
                {
                    "ingest_count": len(dataset.records),
                    "query_count": len(dataset.queries),
                    "failure_count": 0,
                    "recall_at_5": round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
                    "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
                    "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
                    "disk_bytes": 0,
                }
            )
            return self.ok_result(
                dataset,
                metrics,
                ["real pgvector metadata-filtered vector workload executed through psycopg"],
            )
        except Exception as error:
            if config.require_services:
                raise
            return self.unavailable(f"pgvector unavailable: {error}", dataset)


def _vector_literal(vector: list[float], dimensions: int) -> str:
    return "[" + ",".join(f"{value:.6f}" for value in vector[:dimensions]) + "]"
