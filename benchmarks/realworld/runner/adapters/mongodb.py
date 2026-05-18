from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .base import BenchmarkAdapter, optional_import, query_result_record
from ..metrics import MetricRecorder, mrr_at_k, ndcg_at_k, recall_at_k
from ..types import DatasetBundle, RunConfig


class MongoAdapter(BenchmarkAdapter):
    name = "mongodb"
    role = "nested sparse document database"

    def run(self, dataset: DatasetBundle, config: RunConfig) -> dict[str, Any]:
        pymongo = optional_import("pymongo")
        if pymongo is None:
            return self.unavailable("python dependency missing: pymongo", dataset)
        uri = os.environ.get("BENCH_MONGO_URI", "mongodb://localhost:27027")
        client = None
        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            database = client.tracedb_bench
            collection = database.records
            setup_recorder = MetricRecorder()
            ingest_recorder = MetricRecorder()
            query_recorder = MetricRecorder()
            setup_recorder.timed(lambda: _setup_collection(collection))
            if dataset.records:
                ingest_recorder.timed(
                    lambda: collection.insert_many([record.to_json() for record in dataset.records])
                )
            storage_after_ingest = _storage_snapshot(database)
            recalls = []
            ndcgs = []
            mrrs = []
            query_results = []
            for query in dataset.queries:
                rows = query_recorder.timed(
                    lambda query=query: list(
                        collection.find(
                            {
                                "tenant_id": query.tenant_id,
                                "category": query.category,
                                "metadata.nested.priority": {"$exists": True},
                            },
                            {"id": 1},
                        )
                        .sort([("rating", -1), ("id", 1)])
                        .limit(query.top_k)
                    )
                )
                ids = [row["id"] for row in rows]
                query_results.append(query_result_record(query, ids))
                recalls.append(recall_at_k(query.expected_ids, ids, query.top_k))
                ndcgs.append(ndcg_at_k(query.expected_ids, ids, query.top_k))
                mrrs.append(mrr_at_k(query.expected_ids, ids, query.top_k))
            storage_after_workload = _storage_snapshot(database)
            query_summary = query_recorder.summary()
            ingest_summary = ingest_recorder.summary()
            setup_summary = setup_recorder.summary()
            metrics = dict(query_summary)
            metrics.update({f"query_{key}": value for key, value in query_summary.items()})
            metrics.update({f"ingest_{key}": value for key, value in ingest_summary.items()})
            metrics.update({f"setup_{key}": value for key, value in setup_summary.items()})
            metrics.update(
                {
                    "ingest_count": len(dataset.records),
                    "ingest_transaction_count": 1 if dataset.records else 0,
                    "ingest_transaction_total_latency_ms": round(
                        sum(ingest_recorder.latencies_ms), 3
                    ),
                    "query_count": len(dataset.queries),
                    "failure_count": 0,
                    "recall_at_5": round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
                    "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
                    "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
                    "disk_bytes": storage_after_ingest["disk_bytes"],
                    "disk_bytes_after_ingest": storage_after_ingest["disk_bytes"],
                    "disk_bytes_after_workload": storage_after_workload["disk_bytes"],
                    "mongodb_data_dir_bytes": storage_after_ingest["data_dir_bytes"],
                    "mongodb_data_dir_bytes_after_ingest": storage_after_ingest[
                        "data_dir_bytes"
                    ],
                    "mongodb_data_dir_bytes_after_workload": storage_after_workload[
                        "data_dir_bytes"
                    ],
                    "mongodb_dbstats_data_size_bytes": storage_after_ingest[
                        "dbstats_data_size_bytes"
                    ],
                    "mongodb_dbstats_data_size_bytes_after_ingest": storage_after_ingest[
                        "dbstats_data_size_bytes"
                    ],
                    "mongodb_dbstats_data_size_bytes_after_workload": storage_after_workload[
                        "dbstats_data_size_bytes"
                    ],
                    "mongodb_dbstats_storage_size_bytes": storage_after_ingest[
                        "dbstats_storage_size_bytes"
                    ],
                    "mongodb_dbstats_storage_size_bytes_after_ingest": storage_after_ingest[
                        "dbstats_storage_size_bytes"
                    ],
                    "mongodb_dbstats_storage_size_bytes_after_workload": storage_after_workload[
                        "dbstats_storage_size_bytes"
                    ],
                    "mongodb_dbstats_index_size_bytes": storage_after_ingest[
                        "dbstats_index_size_bytes"
                    ],
                    "mongodb_dbstats_index_size_bytes_after_ingest": storage_after_ingest[
                        "dbstats_index_size_bytes"
                    ],
                    "mongodb_dbstats_index_size_bytes_after_workload": storage_after_workload[
                        "dbstats_index_size_bytes"
                    ],
                    "mongodb_dbstats_total_size_bytes": storage_after_ingest[
                        "dbstats_total_size_bytes"
                    ],
                    "mongodb_dbstats_total_size_bytes_after_ingest": storage_after_ingest[
                        "dbstats_total_size_bytes"
                    ],
                    "mongodb_dbstats_total_size_bytes_after_workload": storage_after_workload[
                        "dbstats_total_size_bytes"
                    ],
                }
            )
            return self.ok_result(
                dataset,
                metrics,
                [
                    "real MongoDB nested sparse document workload executed through pymongo",
                    "MongoDB disk_bytes measures BENCH_MONGO_STORAGE_DIR when available and falls back to dbStats storageSize/dataSize",
                    "MongoDB dbStats data/storage/index/total size metrics are reported separately from data-dir footprint",
                ],
                query_results=query_results,
            )
        except Exception as error:
            if config.require_services:
                raise
            return self.unavailable(f"mongodb unavailable: {error}", dataset)
        finally:
            if client is not None:
                close = getattr(client, "close", None)
                if close is not None:
                    close()


def _setup_collection(collection: Any) -> None:
    collection.delete_many({})
    collection.create_index([("tenant_id", 1), ("category", 1)])


def _storage_snapshot(database: Any) -> dict[str, int]:
    stats: dict[str, Any] = {}
    dbstats_data_size_bytes = 0
    dbstats_storage_size_bytes = 0
    dbstats_index_size_bytes = 0
    dbstats_total_size_bytes = 0
    try:
        stats = database.command("dbStats")
    except Exception:
        pass
    if stats:
        dbstats_data_size_bytes = _int_stat(stats, "dataSize")
        dbstats_storage_size_bytes = _int_stat(stats, "storageSize")
        dbstats_index_size_bytes = _int_stat(stats, "indexSize")
        dbstats_total_size_bytes = _int_stat(stats, "totalSize")
        if dbstats_total_size_bytes == 0 and (
            dbstats_storage_size_bytes or dbstats_index_size_bytes
        ):
            dbstats_total_size_bytes = dbstats_storage_size_bytes + dbstats_index_size_bytes
    data_dir_bytes = _directory_bytes(os.environ.get("BENCH_MONGO_STORAGE_DIR"))
    disk_bytes = data_dir_bytes or dbstats_storage_size_bytes or dbstats_data_size_bytes
    return {
        "disk_bytes": disk_bytes,
        "data_dir_bytes": data_dir_bytes,
        "dbstats_data_size_bytes": dbstats_data_size_bytes,
        "dbstats_storage_size_bytes": dbstats_storage_size_bytes,
        "dbstats_index_size_bytes": dbstats_index_size_bytes,
        "dbstats_total_size_bytes": dbstats_total_size_bytes,
    }


def _int_stat(stats: dict[str, Any], key: str) -> int:
    try:
        return int(stats.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _directory_bytes(path_text: str | None) -> int:
    if not path_text:
        return 0
    root = Path(path_text)
    if not root.exists():
        return 0
    total = 0
    for path in root.rglob("*"):
        if path.is_file():
            try:
                total += path.stat().st_size
            except OSError:
                continue
    return total
