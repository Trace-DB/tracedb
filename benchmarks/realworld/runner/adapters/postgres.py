from __future__ import annotations

import os
from typing import Any

from ..metrics import MetricRecorder, mrr_at_k, ndcg_at_k, recall_at_k
from ..types import DatasetBundle, RunConfig
from .base import BenchmarkAdapter, optional_import

# Number of warmup query iterations before measurement begins.
WARMUP_ITERATIONS = 3


class PostgresAdapter(BenchmarkAdapter):
    name = "postgres"
    role = "relational filters updates scans"

    def run(self, dataset: DatasetBundle, _config: RunConfig) -> dict[str, Any]:
        psycopg = optional_import("psycopg")
        if psycopg is None:
            return self.unavailable("python dependency missing: psycopg", dataset)
        dsn = os.environ.get(
            "BENCH_POSTGRES_DSN",
            "postgresql://tracedb:tracedb@localhost:25432/tracedb_bench",
        )
        try:
            with psycopg.connect(dsn, connect_timeout=2) as conn:
                with conn.cursor() as cur:
                    cur.execute("DROP TABLE IF EXISTS bench_records")
                    cur.execute(
                        """
                        CREATE TABLE bench_records (
                          id text PRIMARY KEY,
                          tenant_id text NOT NULL,
                          category text NOT NULL,
                          status text NOT NULL,
                          rating double precision NOT NULL,
                          year integer NOT NULL,
                          body text NOT NULL
                        )
                        """
                    )
                    cur.execute(
                        "CREATE INDEX bench_records_tenant_category ON bench_records (tenant_id, category)"
                    )
                    cur.executemany(
                        """
                        INSERT INTO bench_records
                          (id, tenant_id, category, status, rating, year, body)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                record.record_id,
                                record.tenant_id,
                                record.category,
                                record.status,
                                record.rating,
                                record.year,
                                record.body,
                            )
                            for record in dataset.records
                        ],
                    )
                    conn.commit()
                    # Warmup phase: run a few query iterations (discarded from
                    # measurements) to stabilize filesystem cache and query planner.
                    if dataset.queries:
                        warmup_query = dataset.queries[0]
                        for _ in range(WARMUP_ITERATIONS):
                            cur.execute(
                                """
                                SELECT id
                                FROM bench_records
                                WHERE tenant_id = %s AND category = %s AND body ILIKE %s
                                ORDER BY rating DESC, id ASC
                                LIMIT %s
                                """,
                                (
                                    warmup_query.tenant_id,
                                    warmup_query.category,
                                    f"%{warmup_query.category.split('_')[0]}%",
                                    warmup_query.top_k,
                                ),
                            ).fetchall()
                    recorder = MetricRecorder()
                    recalls = []
                    ndcgs = []
                    mrrs = []
                    for query in dataset.queries:
                        rows = recorder.timed(
                            lambda query=query: cur.execute(
                                """
                                SELECT id
                                FROM bench_records
                                WHERE tenant_id = %s AND category = %s AND body ILIKE %s
                                ORDER BY rating DESC, id ASC
                                LIMIT %s
                                """,
                                (
                                    query.tenant_id,
                                    query.category,
                                    f"%{query.category.split('_')[0]}%",
                                    query.top_k,
                                ),
                            ).fetchall()
                        )
                        ids = [row[0] for row in rows]
                        recalls.append(
                            recall_at_k(query.expected_ids, ids, query.top_k)
                        )
                        ndcgs.append(ndcg_at_k(query.expected_ids, ids, query.top_k))
                        mrrs.append(mrr_at_k(query.expected_ids, ids, query.top_k))
            metrics = recorder.summary()
            metrics.update(
                {
                    "ingest_count": len(dataset.records),
                    "query_count": len(dataset.queries),
                    "failure_count": 0,
                    "recall_at_5": round(sum(recalls) / len(recalls), 3)
                    if recalls
                    else 0.0,
                    "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
                    "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
                    "disk_bytes": 0,
                }
            )
            return self.ok_result(
                dataset,
                metrics,
                ["real PostgreSQL relational workload executed through psycopg"],
            )
        except Exception as error:
            if _config.require_services:
                raise
            return self.unavailable(f"postgres unavailable: {error}", dataset)
