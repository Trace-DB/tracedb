from __future__ import annotations

import os
from pathlib import Path
from typing import Any
import uuid

from .base import BenchmarkAdapter, optional_import, query_result_record
from ..metrics import MetricRecorder, mrr_at_k, ndcg_at_k, recall_at_k
from ..types import DatasetBundle, RunConfig


class MilvusAdapter(BenchmarkAdapter):
    name = "milvus"
    role = "vector-native scalar-filtered search"

    def run(self, dataset: DatasetBundle, config: RunConfig) -> dict[str, Any]:
        pymilvus = optional_import("pymilvus")
        if pymilvus is None:
            return self.unavailable("python dependency missing: pymilvus", dataset)
        uri = os.environ.get("BENCH_MILVUS_URI", "http://localhost:19530")
        token = os.environ.get("BENCH_MILVUS_TOKEN")
        collection = f"tracedb_bench_{uuid.uuid4().hex[:8]}"
        client = None
        try:
            _prepare_local_uri(uri)
            client = _milvus_client(pymilvus, uri, token)
            setup_recorder = MetricRecorder()
            ingest_recorder = MetricRecorder()
            query_recorder = MetricRecorder()
            setup_recorder.timed(
                lambda: client.create_collection(
                    collection_name=collection,
                    dimension=dataset.embedding_dimensions,
                    metric_type="COSINE",
                    auto_id=False,
                )
            )
            if dataset.records:
                batch_size = int(os.environ.get("BENCH_MILVUS_BATCH_SIZE", "64"))
                for start in range(0, len(dataset.records), batch_size):
                    rows = [
                        {
                            "id": idx,
                            "vector": record.vector,
                            "record_id": record.record_id,
                            "tenant_id": record.tenant_id,
                            "category": record.category,
                            "body": record.body,
                        }
                        for idx, record in enumerate(
                            dataset.records[start : start + batch_size],
                            start=start,
                        )
                    ]
                    ingest_recorder.timed(
                        lambda rows=rows: client.insert(
                            collection_name=collection,
                            data=rows,
                        )
                    )
                _maybe_flush(client, collection)
            disk_bytes_after_ingest = _storage_bytes(uri)
            recalls = []
            ndcgs = []
            mrrs = []
            query_results = []
            for query in dataset.queries:
                rows = query_recorder.timed(
                    lambda query=query: client.search(
                        collection_name=collection,
                        data=[query.vector],
                        filter=_milvus_filter(query.tenant_id, query.category),
                        limit=query.top_k,
                        output_fields=["record_id"],
                    )
                )
                ids = _search_ids(rows)
                query_results.append(query_result_record(query, ids))
                recalls.append(recall_at_k(query.expected_ids, ids, query.top_k))
                ndcgs.append(ndcg_at_k(query.expected_ids, ids, query.top_k))
                mrrs.append(mrr_at_k(query.expected_ids, ids, query.top_k))
            disk_bytes_after_workload = _storage_bytes(uri)
            query_summary = query_recorder.summary()
            ingest_summary = ingest_recorder.summary()
            setup_summary = setup_recorder.summary()
            ingest_transaction_total_ms = round(sum(ingest_recorder.latencies_ms), 3)
            metrics = dict(query_summary)
            metrics.update({f"query_{key}": value for key, value in query_summary.items()})
            metrics.update({f"ingest_{key}": value for key, value in ingest_summary.items()})
            metrics.update({f"setup_{key}": value for key, value in setup_summary.items()})
            metrics.update(
                {
                    "ingest_count": len(dataset.records),
                    "ingest_transaction_count": 1 if dataset.records else 0,
                    "ingest_transaction_total_latency_ms": ingest_transaction_total_ms,
                    "query_count": len(dataset.queries),
                    "failure_count": 0,
                    "recall_at_5": round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
                    "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
                    "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
                    "disk_bytes": disk_bytes_after_ingest,
                    "disk_bytes_after_ingest": disk_bytes_after_ingest,
                    "disk_bytes_after_workload": disk_bytes_after_workload,
                }
            )
            return self.ok_result(
                dataset,
                metrics,
                [
                    "real Milvus Lite vector scalar-filter workload executed through pymilvus",
                    "Milvus Lite ingestion used batched inserts into one local embedded database file",
                    "Milvus Lite disk_bytes measures the local BENCH_MILVUS_URI file or BENCH_MILVUS_STORAGE_DIR, not a standalone Milvus server storage metric",
                ],
                query_results=query_results,
            )
        except Exception as error:
            if config.require_services:
                raise
            return self.unavailable(f"milvus unavailable: {error}", dataset)
        finally:
            if client is not None:
                close = getattr(client, "close", None)
                if close is not None:
                    close()


def _milvus_client(pymilvus: Any, uri: str, token: str | None) -> Any:
    kwargs: dict[str, Any] = {"uri": uri}
    if token:
        kwargs["token"] = token
    return pymilvus.MilvusClient(**kwargs)


def _prepare_local_uri(uri: str) -> None:
    if uri.startswith(("http://", "https://", "grpc://", "tcp://")):
        return
    path = Path(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()


def _maybe_flush(client: Any, collection: str) -> None:
    flush = getattr(client, "flush", None)
    if flush is not None:
        flush(collection_name=collection)


def _milvus_filter(tenant_id: str, category: str) -> str:
    return f'tenant_id == "{_escape_filter_value(tenant_id)}" and category == "{_escape_filter_value(category)}"'


def _escape_filter_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _search_ids(rows: Any) -> list[str]:
    first_result_set = rows[0] if rows else []
    ids = []
    for row in first_result_set:
        entity = row.get("entity", {}) if isinstance(row, dict) else {}
        record_id = entity.get("record_id")
        if record_id is None and isinstance(row, dict):
            record_id = row.get("record_id")
        if record_id is not None:
            ids.append(str(record_id))
    return ids


def _storage_bytes(uri: str) -> int:
    storage_dir = os.environ.get("BENCH_MILVUS_STORAGE_DIR")
    if storage_dir:
        return _directory_bytes(storage_dir)
    if uri.startswith(("http://", "https://", "grpc://", "tcp://")):
        return 0
    path = Path(uri)
    if path.is_file():
        return path.stat().st_size
    if path.is_dir():
        return _directory_bytes(str(path))
    return 0


def _directory_bytes(path: str) -> int:
    root = Path(path)
    if not root.exists():
        return 0
    total = 0
    for item in root.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except OSError:
            continue
    return total
