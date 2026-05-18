from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .base import BenchmarkAdapter, command_exists, in_memory_search_metrics
from ..http import request_json, request_json_with_response
from ..metrics import (
    MetricRecorder,
    mrr_at_k,
    ndcg_at_k,
    percentile,
    recall_at_k,
    same_file_recall_at_k,
)
from ..types import DatasetBundle, RunConfig


class TraceDbAdapter(BenchmarkAdapter):
    name = "tracedb"
    role = "transactional hybrid database"

    def run(self, dataset: DatasetBundle, config: RunConfig) -> dict[str, Any]:
        metrics = in_memory_search_metrics(dataset)
        notes = [
            "semantic workload executed against TraceDB-compatible generated corpus",
            f"surfaces requested: {', '.join(config.surfaces)}",
        ]
        notes.extend(self._sdk_surface_notes(dataset, config))
        if "cli" in config.surfaces:
            cli_notes, cli_metrics = self._cli_surface_run(dataset, config)
            notes.extend(cli_notes)
            metrics.update(cli_metrics)
        if "http" in config.surfaces or "curl" in config.surfaces:
            http_notes, http_metrics = self._http_surface_run(dataset, config)
            notes.extend(http_notes)
            if http_metrics is not None:
                metrics = http_metrics
        metrics["failure_count"] = sum(1 for note in notes if note.startswith("surface unavailable"))
        return self.ok_result(dataset, metrics, notes)

    def _sdk_surface_notes(self, dataset: DatasetBundle, _config: RunConfig) -> list[str]:
        if not dataset.records:
            return ["surface unavailable: sdk-style request builder had no records"]
        record = dataset.records[0]
        payload = {
            "path": "/v1/records/put",
            "body": {
                "table": "bench_records",
                "id": record.record_id,
                "tenant_id": record.tenant_id,
                "fields": record.to_json(),
            },
        }
        if payload["body"]["id"] != record.record_id:
            return ["surface unavailable: sdk-style payload construction failed"]
        return ["sdk-style request builder payload validated"]

    def _cli_surface_run(
        self, dataset: DatasetBundle, config: RunConfig
    ) -> tuple[list[str], dict[str, Any]]:
        cli = os.environ.get("TRACEDB_CLI") or str(
            Path(config.repo_root) / "target" / "debug" / "tracedb"
        )
        if not command_exists(cli):
            return [
                "surface unavailable: TraceDB CLI binary not found; run `cargo build --workspace` or set TRACEDB_CLI"
            ], {}
        if not dataset.records:
            return ["surface unavailable: CLI smoke had no records"], {}
        record = dataset.records[0]
        schema = {
            "name": "bench_records",
            "primary_id_column": "id",
            "tenant_id_column": "tenant",
            "scalar_columns": ["category", "status", "rating", "year"],
            "text_indexed_columns": ["body"],
            "vector_columns": [
                {
                    "name": "embedding",
                    "dimensions": len(record.vector),
                    "source_columns": ["body"],
                }
            ],
        }
        record_input = {
            "table": "bench_records",
            "id": record.record_id,
            "tenant_id": record.tenant_id,
            "fields": {
                "id": record.record_id,
                "tenant": record.tenant_id,
                "body": record.body,
                "category": record.category,
                "status": record.status,
                "rating": record.rating,
                "year": record.year,
                "embedding": record.vector,
            },
        }
        with tempfile.TemporaryDirectory(prefix="tracedb-bench-cli-") as temp_dir:
            temp = Path(temp_dir)
            schema_path = temp / "schema.json"
            record_path = temp / "record.json"
            schema_path.write_text(json.dumps(schema), encoding="utf-8")
            record_path.write_text(json.dumps(record_input), encoding="utf-8")
            data_dir = temp / "db"
            commands = [
                [cli, "--data", str(data_dir), "init"],
                [cli, "--data", str(data_dir), "schema", "apply", str(schema_path)],
                [cli, "--data", str(data_dir), "put", str(record_path)],
                [cli, "--data", str(data_dir), "get", "bench_records", record.tenant_id, record.record_id],
            ]
            recorder = MetricRecorder()
            for command in commands:
                completed = recorder.timed(
                    lambda command=command: subprocess.run(
                        command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        check=False,
                    )
                )
                if completed.returncode != 0:
                    return [
                        "surface unavailable: TraceDB CLI smoke failed "
                        + completed.stderr.strip()
                    ], {}
            cli_metrics = {
                f"cli_{key}": value for key, value in recorder.summary().items()
            }
            cli_metrics["cli_command_count"] = len(commands)
        return [
            "TraceDB CLI surface smoke passed; "
            f"cli_command_count={cli_metrics['cli_command_count']}; "
            f"cli_latency_p95_ms={cli_metrics['cli_latency_p95_ms']}"
        ], cli_metrics

    def _http_surface_run(
        self, dataset: DatasetBundle, config: RunConfig
    ) -> tuple[list[str], dict[str, Any] | None]:
        url = os.environ.get("TRACEDB_HTTP_URL")
        if not url:
            return ["surface unavailable: TRACEDB_HTTP_URL not set for HTTP/curl smoke"], None
        if not dataset.records:
            return ["surface unavailable: TraceDB HTTP smoke had no records"], None
        base_url = url.rstrip("/")
        run_token = self._path_token(config.run_id or "adhoc")
        table = f"bench_records_{run_token}_{os.getpid()}_{len(dataset.records)}"
        http_timeout = self._float_env("TRACEDB_HTTP_TIMEOUT_SECONDS", 10.0)
        admin_timeout = max(
            http_timeout,
            self._float_env("TRACEDB_HTTP_ADMIN_TIMEOUT_SECONDS", 45.0),
        )

        def call(
            label: str,
            method: str,
            path: str,
            body: dict[str, Any] | None = None,
            timeout: float = http_timeout,
        ) -> dict[str, Any]:
            try:
                return request_json(method, f"{base_url}{path}", body, timeout=timeout)
            except Exception as error:
                raise RuntimeError(f"{label} failed: {error}") from error

        def call_with_headers(
            label: str,
            method: str,
            path: str,
            body: dict[str, Any] | None = None,
            timeout: float = http_timeout,
        ) -> tuple[dict[str, Any], dict[str, str], dict[str, float | int]]:
            try:
                return request_json_with_response(
                    method, f"{base_url}{path}", body, timeout=timeout
                )
            except Exception as error:
                raise RuntimeError(f"{label} failed: {error}") from error

        try:
            call("ready", "GET", "/ready", timeout=http_timeout)
        except Exception as error:
            return [f"surface unavailable: TraceDB HTTP ready failed: {error}"], None

        try:
            schema = {
                "name": table,
                "primary_id_column": "id",
                "tenant_id_column": "tenant",
                "scalar_columns": ["category", "status", "rating", "year"],
                "text_indexed_columns": ["body"],
                "vector_columns": [
                    {
                        "name": "embedding",
                        "dimensions": len(dataset.records[0].vector),
                        "source_columns": ["body"],
                    }
                ],
            }
            call("schema apply", "POST", "/v1/schema/apply", schema)
            recorder = MetricRecorder()
            ingest_recorder = MetricRecorder()
            query_recorder = MetricRecorder()
            freshness_query_recorder = MetricRecorder()
            admin_recorder = MetricRecorder()
            admin_compact_recorder = MetricRecorder()
            admin_snapshot_recorder = MetricRecorder()
            admin_restore_recorder = MetricRecorder()
            ingest_mode = config.tracedb_ingest_mode

            def timed(recorder_for_operation: MetricRecorder, operation):
                return recorder.timed(lambda: recorder_for_operation.timed(operation))

            if ingest_mode == "batch":
                batch_response = timed(
                    ingest_recorder,
                    lambda: call(
                        "record batch put",
                        "POST",
                        "/v1/records/put-batch",
                        {
                            "records": [
                                self._record_input(table, record)
                                for record in dataset.records
                            ]
                        },
                        timeout=admin_timeout,
                    ),
                )
                if int(batch_response.get("record_count", 0)) != len(dataset.records):
                    return [
                        "surface unavailable: TraceDB HTTP batch write returned an unexpected record_count"
                    ], None
            elif ingest_mode == "per_record":
                for record in dataset.records:
                    timed(
                        ingest_recorder,
                        lambda record=record: call(
                            "record put",
                            "POST",
                            "/v1/records/put",
                            self._record_input(table, record),
                        )
                    )
            else:
                return [f"surface unavailable: unsupported TraceDB ingest mode {ingest_mode}"], None

            first = dataset.records[0]
            fresh_get = call(
                "fresh get",
                "POST",
                "/v1/records/get",
                {"table": table, "tenant_id": first.tenant_id, "id": first.record_id},
            )
            if fresh_get.get("record") is None:
                return ["surface unavailable: TraceDB HTTP fresh write was not visible"], None
            disk_bytes_after_ingest = _directory_bytes(os.environ.get("TRACEDB_HTTP_DATA_DIR"))
            disk_bytes_after_ingest_by_top_level = _directory_top_level_bytes(
                os.environ.get("TRACEDB_HTTP_DATA_DIR")
            )
            isolated_get = call(
                "tenant isolation get",
                "POST",
                "/v1/records/get",
                {"table": table, "tenant_id": "tenant-not-owned", "id": first.record_id},
            )
            if isolated_get.get("record") is not None:
                return ["surface unavailable: TraceDB HTTP tenant isolation failed"], None
            call(
                "record patch",
                "POST",
                "/v1/records/patch",
                {
                    "table": table,
                    "tenant_id": first.tenant_id,
                    "id": first.record_id,
                    "fields": {"status": "benchmark_patched"},
                },
            )
            patched = call(
                "patched get",
                "POST",
                "/v1/records/get",
                {"table": table, "tenant_id": first.tenant_id, "id": first.record_id},
            )
            if (
                patched.get("record", {})
                .get("fields", {})
                .get("status")
                != "benchmark_patched"
            ):
                return ["surface unavailable: TraceDB HTTP patch was not visible"], None
            recalls = []
            ndcgs = []
            mrrs = []
            same_file_recalls = []
            span_gap_count = 0
            records_by_id = {record.record_id: record for record in dataset.records}
            off_category_result_count = 0
            queries_with_off_category_results = 0
            query_phase_recorders: dict[str, MetricRecorder] = {}
            query_access_path_build_recorders: dict[str, MetricRecorder] = {}
            query_access_path_open_recorders: dict[str, MetricRecorder] = {}
            query_server_timing_recorders: dict[str, MetricRecorder] = {}
            query_engine_phase_total_recorder = MetricRecorder()
            query_http_client_overhead_recorder = MetricRecorder()
            query_http_client_header_overhead_recorder = MetricRecorder()
            query_http_client_unattributed_overhead_recorder = MetricRecorder()
            query_http_response_timing_recorders: dict[str, MetricRecorder] = {}
            query_http_response_body_bytes: list[float] = []
            query_http_response_content_length_bytes: list[float] = []
            query_http_response_content_length_missing_count = 0
            query_http_response_content_length_mismatch_count = 0
            query_output_probe_limit = min(
                len(dataset.queries),
                max(0, int(self._float_env("TRACEDB_QUERY_OUTPUT_PROBE_LIMIT", 3.0))),
            )
            query_output_probe_count = 0
            query_output_probe_latency_recorders: dict[str, MetricRecorder] = {}
            query_output_probe_response_timing_recorders: dict[str, dict[str, MetricRecorder]] = {}
            query_output_probe_server_timing_recorders: dict[str, dict[str, MetricRecorder]] = {}
            query_output_probe_body_bytes: dict[str, list[float]] = {}
            query_output_probe_content_length_bytes: dict[str, list[float]] = {}
            query_output_probe_content_length_missing_counts: dict[str, int] = {}
            query_output_probe_content_length_mismatch_counts: dict[str, int] = {}
            query_output_probe_true_over_false_deltas: list[float] = []
            query_output_probe_explain_endpoint_over_false_deltas: list[float] = []
            query_output_probe_phase_timing_counts: list[float] = []
            query_output_probe_access_path_timing_counts: list[float] = []
            query_output_probe_planner_candidate_counts: list[float] = []
            query_output_probe_candidates: list[dict[str, Any]] = []
            query_explain_observer_candidates: list[dict[str, Any]] = []
            query_explain_observer_count = 0
            query_output_probe_result_id_mismatch_count = 0
            query_output_probe_explain_returned_count_mismatch_count = 0
            query_output_probe_required_explain_field_missing_count = 0
            query_output_probe_explain_false_explain_returned_count = 0

            def query_body(
                query, *, freshness: str = "AllowDirty", explain: bool
            ) -> dict[str, Any]:
                return {
                    "table": table,
                    "tenant_id": query.tenant_id,
                    "scalar_eq": {"category": query.category},
                    "text": query.text,
                    "vector": query.vector,
                    "top_k": query.top_k,
                    "freshness": freshness,
                    "explain": explain,
                }

            def observe_query_explain(
                query,
                *,
                ids: list[Any],
                recall: float,
                same_file_recall: float,
                ndcg: float,
                mrr: float,
                off_category_ids: list[Any],
                explain: dict[str, Any],
                explain_source: str,
            ) -> None:
                if not config.observer:
                    return
                config.observer.observe(
                    "tracedb.query_explain",
                    {
                        "query_id": query.query_id,
                        "explain_source": explain_source,
                        "freshness_mode": explain.get("freshness_mode"),
                        "scalar_filter_applied": explain.get("scalar_filter_applied"),
                        "scalar_filter_visible_records": explain.get(
                            "scalar_filter_visible_records"
                        ),
                        "scalar_filter_removed_records": explain.get(
                            "scalar_filter_removed_records"
                        ),
                        "opened_candidate_streams": explain.get(
                            "opened_candidate_streams"
                        ),
                        "candidate_budget": explain.get("candidate_budget"),
                        "expected_ids": query.expected_ids,
                        "actual_ids": ids,
                        "expected_category": query.category,
                        "off_category_actual_ids": off_category_ids,
                        "recall_at_k": round(recall, 3),
                        "same_file_recall_at_k": round(same_file_recall, 3),
                        "ndcg_at_k": round(ndcg, 3),
                        "mrr_at_k": round(mrr, 3),
                        "returned_count": explain.get("returned_count"),
                    },
                )

            def record_query_output_probe_metadata(
                shape: str,
                response_headers: dict[str, str],
                response_meta: dict[str, float | int],
            ) -> None:
                _record_server_timing_metrics(
                    response_headers,
                    query_output_probe_server_timing_recorders.setdefault(shape, {}),
                )
                _record_response_metadata_metrics(
                    response_meta,
                    query_output_probe_response_timing_recorders.setdefault(shape, {}),
                    query_output_probe_body_bytes.setdefault(shape, []),
                    query_output_probe_content_length_bytes.setdefault(shape, []),
                )
                query_output_probe_content_length_missing_counts[shape] = (
                    query_output_probe_content_length_missing_counts.get(shape, 0)
                    + int(response_meta.get("content_length_missing", 0))
                )
                query_output_probe_content_length_mismatch_counts[shape] = (
                    query_output_probe_content_length_mismatch_counts.get(shape, 0)
                    + int(response_meta.get("content_length_mismatch", 0))
                )

            def record_existing_query_output_probe(
                shape: str,
                elapsed_ms: float,
                response_headers: dict[str, str],
                response_meta: dict[str, float | int],
            ) -> None:
                query_output_probe_latency_recorders.setdefault(
                    shape, MetricRecorder()
                ).latencies_ms.append(elapsed_ms)
                record_query_output_probe_metadata(shape, response_headers, response_meta)

            def call_query_output_probe(
                shape: str,
                label: str,
                path: str,
                body: dict[str, Any],
            ) -> tuple[dict[str, Any], float]:
                probe_recorder = query_output_probe_latency_recorders.setdefault(
                    shape, MetricRecorder()
                )
                probe_result, probe_headers, probe_meta = probe_recorder.timed(
                    lambda: call_with_headers(label, "POST", path, body)
                )
                record_query_output_probe_metadata(shape, probe_headers, probe_meta)
                return probe_result, probe_recorder.latencies_ms[-1]

            for query_index, query in enumerate(dataset.queries):
                result, response_headers, response_meta = timed(
                    query_recorder,
                    lambda query=query: call_with_headers(
                        "query allow-dirty",
                        "POST",
                        "/v1/query",
                        query_body(query, explain=False),
                    )
                )
                query_client_ms = query_recorder.latencies_ms[-1]
                server_timings = _record_server_timing_metrics(
                    response_headers, query_server_timing_recorders
                )
                _record_response_metadata_metrics(
                    response_meta,
                    query_http_response_timing_recorders,
                    query_http_response_body_bytes,
                    query_http_response_content_length_bytes,
                )
                query_http_response_content_length_missing_count += int(
                    response_meta.get("content_length_missing", 0)
                )
                query_http_response_content_length_mismatch_count += int(
                    response_meta.get("content_length_mismatch", 0)
                )
                server_prewrite_ms = server_timings.get("prewrite_total")
                if server_prewrite_ms is not None:
                    client_overhead_ms = max(query_client_ms - server_prewrite_ms, 0.0)
                    query_http_client_overhead_recorder.latencies_ms.append(client_overhead_ms)
                    header_wait_ms = _float_metric(response_meta.get("header_wait_ms"))
                    if header_wait_ms is not None:
                        query_http_client_header_overhead_recorder.latencies_ms.append(
                            max(header_wait_ms - server_prewrite_ms, 0.0)
                        )
                    processing_ms = _float_metric(response_meta.get("processing_ms"))
                    if processing_ms is not None:
                        query_http_client_unattributed_overhead_recorder.latencies_ms.append(
                            max(client_overhead_ms - processing_ms, 0.0)
                        )
                ids = [row.get("record_id") for row in result.get("results", [])]
                off_category_ids = [
                    record_id
                    for record_id in ids
                    if records_by_id.get(str(record_id)) is not None
                    and records_by_id[str(record_id)].category != query.category
                ]
                off_category_result_count += len(off_category_ids)
                if off_category_ids:
                    queries_with_off_category_results += 1
                recall = recall_at_k(query.expected_ids, ids, query.top_k)
                ndcg = ndcg_at_k(query.expected_ids, ids, query.top_k)
                mrr = mrr_at_k(query.expected_ids, ids, query.top_k)
                same_file_recall = same_file_recall_at_k(query.expected_ids, ids, query.top_k)
                recalls.append(recall)
                ndcgs.append(ndcg)
                mrrs.append(mrr)
                same_file_recalls.append(same_file_recall)
                if same_file_recall > recall:
                    span_gap_count += 1
                explain_for_observer = result.get("explain")
                if query_index < query_output_probe_limit:
                    query_output_probe_candidates.append(
                        {
                            "query": query,
                            "ids": ids,
                            "query_client_ms": query_client_ms,
                            "response_headers": response_headers,
                            "response_meta": response_meta,
                            "primary_explain": explain_for_observer,
                        }
                    )
                if config.observer and isinstance(explain_for_observer, dict):
                    observe_query_explain(
                        query,
                        ids=ids,
                        recall=recall,
                        same_file_recall=same_file_recall,
                        ndcg=ndcg,
                        mrr=mrr,
                        off_category_ids=off_category_ids,
                        explain=explain_for_observer,
                        explain_source="primary_query",
                    )
                elif config.observer:
                    query_explain_observer_candidates.append(
                        {
                            "query": query,
                            "ids": ids,
                            "recall": recall,
                            "same_file_recall": same_file_recall,
                            "ndcg": ndcg,
                            "mrr": mrr,
                            "off_category_ids": off_category_ids,
                        }
                    )

            for candidate in query_output_probe_candidates:
                query = candidate["query"]
                ids = candidate["ids"]
                query_client_ms = float(candidate["query_client_ms"])
                record_existing_query_output_probe(
                    "explain_false",
                    query_client_ms,
                    candidate["response_headers"],
                    candidate["response_meta"],
                )
                if isinstance(candidate.get("primary_explain"), dict):
                    query_output_probe_explain_false_explain_returned_count += 1
                explain_true_result, explain_true_ms = call_query_output_probe(
                    "explain_true",
                    "query allow-dirty explain probe",
                    "/v1/query",
                    query_body(query, explain=True),
                )
                explain_endpoint, explain_endpoint_ms = call_query_output_probe(
                    "explain_endpoint",
                    "explain endpoint probe",
                    "/v1/explain",
                    query_body(query, explain=True),
                )
                query_output_probe_true_over_false_deltas.append(
                    explain_true_ms - query_client_ms
                )
                query_output_probe_explain_endpoint_over_false_deltas.append(
                    explain_endpoint_ms - query_client_ms
                )
                explain_true_ids = [
                    row.get("record_id") for row in explain_true_result.get("results", [])
                ]
                if [str(record_id) for record_id in explain_true_ids] != [
                    str(record_id) for record_id in ids
                ]:
                    query_output_probe_result_id_mismatch_count += 1
                explain = explain_true_result.get("explain", {})
                missing = _missing_required_explain_fields(explain)
                endpoint_missing = _missing_required_explain_fields(explain_endpoint)
                query_output_probe_required_explain_field_missing_count += len(
                    missing
                ) + len(endpoint_missing)
                if missing or endpoint_missing:
                    return [
                        "surface unavailable: TraceDB HTTP output-shape explain missing "
                        + ", ".join(missing + endpoint_missing)
                    ], None
                returned_count = _float_metric(explain.get("returned_count"))
                endpoint_returned_count = _float_metric(
                    explain_endpoint.get("returned_count")
                )
                if returned_count is None or int(returned_count) != len(explain_true_ids):
                    query_output_probe_explain_returned_count_mismatch_count += 1
                if (
                    endpoint_returned_count is None
                    or int(endpoint_returned_count) != len(explain_true_ids)
                ):
                    query_output_probe_explain_returned_count_mismatch_count += 1
                phase_total_ms = _explain_phase_total_ms(explain)
                if phase_total_ms is not None:
                    query_engine_phase_total_recorder.latencies_ms.append(phase_total_ms)
                _record_explain_timing_metrics(
                    explain,
                    query_phase_recorders,
                    query_access_path_build_recorders,
                    query_access_path_open_recorders,
                )
                phase_timings = explain.get("phase_timings", [])
                if isinstance(phase_timings, list):
                    query_output_probe_phase_timing_counts.append(float(len(phase_timings)))
                access_path_timings = explain.get("access_path_timings", [])
                if isinstance(access_path_timings, list):
                    query_output_probe_access_path_timing_counts.append(
                        float(len(access_path_timings))
                    )
                opened_candidate_streams = explain.get("opened_candidate_streams", [])
                if isinstance(opened_candidate_streams, list):
                    query_output_probe_planner_candidate_counts.append(
                        float(len(opened_candidate_streams))
                    )
                query_output_probe_count += 1

            for candidate in query_explain_observer_candidates:
                query = candidate["query"]
                observer_explain = call(
                    "observer explain",
                    "POST",
                    "/v1/explain",
                    query_body(query, explain=True),
                )
                if isinstance(observer_explain, dict):
                    query_explain_observer_count += 1
                    observe_query_explain(
                        query,
                        ids=candidate["ids"],
                        recall=float(candidate["recall"]),
                        same_file_recall=float(candidate["same_file_recall"]),
                        ndcg=float(candidate["ndcg"]),
                        mrr=float(candidate["mrr"]),
                        off_category_ids=candidate["off_category_ids"],
                        explain=observer_explain,
                        explain_source="observer_explain_endpoint",
                    )

            if dataset.queries:
                query = dataset.queries[0]
                for freshness in ["Strict", "Lazy"]:
                    timed(
                        freshness_query_recorder,
                        lambda query=query, freshness=freshness: call(
                            f"query {freshness.lower()}",
                            "POST",
                            "/v1/query",
                            {
                                "table": table,
                                "tenant_id": query.tenant_id,
                                "scalar_eq": {"category": query.category},
                                "text": query.text,
                                "vector": query.vector,
                                "top_k": query.top_k,
                                "freshness": freshness,
                                "explain": True,
                            },
                        ),
                    )

            timed(
                admin_recorder,
                lambda: admin_compact_recorder.timed(
                    lambda: call("compact", "POST", "/v1/admin/compact", {}, timeout=admin_timeout)
                ),
            )
            if self._is_local_http_url(base_url):
                with tempfile.TemporaryDirectory(prefix="tracedb-bench-http-snapshot-") as temp_dir:
                    snapshot_dir = str(Path(temp_dir) / "snapshot")
                    restore_dir = str(Path(temp_dir) / "restore")
                    timed(
                        admin_recorder,
                        lambda: admin_snapshot_recorder.timed(
                            lambda: call(
                                "snapshot",
                                "POST",
                                "/v1/admin/snapshot",
                                {"target": snapshot_dir},
                                timeout=admin_timeout,
                            )
                        ),
                    )
                    timed(
                        admin_recorder,
                        lambda: admin_restore_recorder.timed(
                            lambda: call(
                                "restore",
                                "POST",
                                "/v1/admin/restore",
                                {"source": snapshot_dir, "target": restore_dir},
                                timeout=admin_timeout,
                            )
                        ),
                    )
            else:
                snapshot_root = os.environ.get(
                    "TRACEDB_REMOTE_SNAPSHOT_ROOT", "/tmp/tracedb-bench-snapshots"
                ).rstrip("/")
                snapshot_dir = f"{snapshot_root}/{table}/snapshot"
                restore_dir = f"{snapshot_root}/{table}/restore"
                timed(
                    admin_recorder,
                    lambda: admin_snapshot_recorder.timed(
                        lambda: call(
                            "snapshot",
                            "POST",
                            "/v1/admin/snapshot",
                            {"target": snapshot_dir},
                            timeout=admin_timeout,
                        )
                    ),
                )
                timed(
                    admin_recorder,
                    lambda: admin_restore_recorder.timed(
                        lambda: call(
                            "restore",
                            "POST",
                            "/v1/admin/restore",
                            {"source": snapshot_dir, "target": restore_dir},
                            timeout=admin_timeout,
                        )
                    ),
                )

            deleted = dataset.records[0]
            call(
                "record delete",
                "POST",
                "/v1/records/delete",
                {
                    "table": table,
                    "tenant_id": deleted.tenant_id,
                    "id": deleted.record_id,
                    "tombstone": "benchmark_delete",
                },
            )
            get_deleted = call(
                "deleted get",
                "POST",
                "/v1/records/get",
                {
                    "table": table,
                    "tenant_id": deleted.tenant_id,
                    "id": deleted.record_id,
                },
            )
            if get_deleted.get("record") is not None:
                return ["surface unavailable: TraceDB HTTP tombstone remained visible"], None

            metrics = recorder.summary()
            for prefix, operation_summary in [
                ("ingest", ingest_recorder.summary()),
                ("query", query_recorder.summary()),
                ("freshness_query", freshness_query_recorder.summary()),
                ("admin", admin_recorder.summary()),
                ("admin_compact", admin_compact_recorder.summary()),
                ("admin_snapshot", admin_snapshot_recorder.summary()),
                ("admin_restore", admin_restore_recorder.summary()),
            ]:
                metrics.update(
                    {
                        f"{prefix}_{key}": value
                        for key, value in operation_summary.items()
                    }
                )
            min_recall = min(recalls) if recalls else 0.0
            min_ndcg = min(ndcgs) if ndcgs else 0.0
            queries_below_full_recall = sum(1 for recall in recalls if recall < 1.0)
            queries_with_zero_recall = sum(1 for recall in recalls if recall == 0.0)
            category_filter_applied = bool(dataset.queries) and off_category_result_count == 0
            disk_bytes_after_workload = _directory_bytes(os.environ.get("TRACEDB_HTTP_DATA_DIR"))
            disk_bytes_after_workload_by_top_level = _directory_top_level_bytes(
                os.environ.get("TRACEDB_HTTP_DATA_DIR")
            )
            ingest_transaction_count = 1 if ingest_mode == "batch" else len(dataset.records)
            ingest_transaction_total_ms = round(sum(ingest_recorder.latencies_ms), 3)
            if ingest_mode == "per_record":
                ingest_mode_note = (
                    "TraceDB HTTP ingest mode: per-record durable writes; "
                    f"transactions={ingest_transaction_count}"
                )
            else:
                ingest_mode_note = (
                    "TraceDB HTTP ingest mode: single-transaction batch put; "
                    f"records={len(dataset.records)}"
                )
            metrics.update(
                {
                    "ingest_count": len(dataset.records),
                    "ingest_transaction_count": ingest_transaction_count,
                    "ingest_transaction_total_latency_ms": ingest_transaction_total_ms,
                    "per_record_durable_transaction_count": len(dataset.records)
                    if ingest_mode == "per_record"
                    else 0,
                    "batch_transaction_count": 1 if ingest_mode == "batch" else 0,
                    "batch_transaction_record_count": len(dataset.records)
                    if ingest_mode == "batch"
                    else 0,
                    "batch_transaction_total_latency_ms": ingest_transaction_total_ms
                    if ingest_mode == "batch"
                    else 0.0,
                    "query_count": len(dataset.queries),
                    "freshness_query_count": len(freshness_query_recorder.latencies_ms),
                    "admin_compact_count": len(admin_compact_recorder.latencies_ms),
                    "admin_snapshot_count": len(admin_snapshot_recorder.latencies_ms),
                    "admin_restore_count": len(admin_restore_recorder.latencies_ms),
                    "failure_count": 0,
                    "recall_at_5": round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
                    "same_file_recall_at_5": round(sum(same_file_recalls) / len(same_file_recalls), 3) if same_file_recalls else 0.0,
                    "span_gap_count": span_gap_count,
                    "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
                    "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
                    "min_recall_at_5": round(min_recall, 3),
                    "min_ndcg_at_5": round(min_ndcg, 3),
                    "queries_below_full_recall_count": queries_below_full_recall,
                    "queries_with_zero_recall_count": queries_with_zero_recall,
                    "category_filter_applied": category_filter_applied,
                    "off_category_result_count": off_category_result_count,
                    "queries_with_off_category_results_count": queries_with_off_category_results,
                    "disk_bytes": disk_bytes_after_ingest,
                    "disk_bytes_after_ingest": disk_bytes_after_ingest,
                    "disk_bytes_after_workload": disk_bytes_after_workload,
                }
            )
            metrics.update(
                _recorder_metric_fields("query_phase", query_phase_recorders)
            )
            metrics.update(_single_recorder_metric_fields("query_http_client", query_recorder))
            metrics.update(
                _single_recorder_metric_fields(
                    "query_engine_phase_total", query_engine_phase_total_recorder
                )
            )
            metrics.update(
                _single_recorder_metric_fields(
                    "query_http_client_overhead", query_http_client_overhead_recorder
                )
            )
            metrics.update(
                _single_recorder_metric_fields(
                    "query_http_client_header_overhead",
                    query_http_client_header_overhead_recorder,
                )
            )
            metrics.update(
                _single_recorder_metric_fields(
                    "query_http_client_unattributed_overhead",
                    query_http_client_unattributed_overhead_recorder,
                )
            )
            metrics.update(
                _response_byte_metric_fields(
                    "query_http_response_body_bytes",
                    query_http_response_body_bytes,
                )
            )
            metrics.update(
                _response_byte_metric_fields(
                    "query_http_response_content_length_bytes",
                    query_http_response_content_length_bytes,
                )
            )
            metrics.update(
                _recorder_metric_fields(
                    "query_http_response",
                    query_http_response_timing_recorders,
                )
            )
            metrics["query_http_response_content_length_missing_count"] = (
                query_http_response_content_length_missing_count
            )
            metrics["query_http_response_content_length_mismatch_count"] = (
                query_http_response_content_length_mismatch_count
            )
            metrics.update(_recorder_metric_fields("query_server", query_server_timing_recorders))
            metrics["query_output_probe_count"] = query_output_probe_count
            metrics["query_output_probe_order_mode"] = (
                "fixed_explain_false_then_explain_true_then_explain_endpoint"
            )
            metrics["query_output_probe_shape_count"] = 3 if query_output_probe_count else 0
            metrics["query_output_probe_replication_count"] = query_output_probe_count
            metrics["query_output_probe_randomized_order"] = 0
            metrics["query_output_probe_order_valid_for_latency_comparison"] = 0
            metrics["query_output_probe_result_id_mismatch_count"] = (
                query_output_probe_result_id_mismatch_count
            )
            metrics["query_output_probe_explain_returned_count_mismatch_count"] = (
                query_output_probe_explain_returned_count_mismatch_count
            )
            metrics["query_output_probe_required_explain_field_missing_count"] = (
                query_output_probe_required_explain_field_missing_count
            )
            metrics["query_output_probe_explain_false_explain_returned_count"] = (
                query_output_probe_explain_false_explain_returned_count
            )
            metrics["query_observer_explain_count"] = query_explain_observer_count
            metrics["query_phase_probe_sample_count"] = query_output_probe_count
            metrics["query_access_path_probe_sample_count"] = query_output_probe_count
            for shape, probe_recorder in sorted(query_output_probe_latency_recorders.items()):
                metrics.update(
                    _single_recorder_metric_fields(
                        f"query_output_probe_{shape}_query",
                        probe_recorder,
                    )
                )
                metrics.update(
                    _response_byte_metric_fields(
                        f"query_output_probe_{shape}_body_bytes",
                        query_output_probe_body_bytes.get(shape, []),
                    )
                )
                metrics.update(
                    _response_byte_metric_fields(
                        f"query_output_probe_{shape}_content_length_bytes",
                        query_output_probe_content_length_bytes.get(shape, []),
                    )
                )
                metrics.update(
                    _recorder_metric_fields(
                        f"query_output_probe_{shape}_response",
                        query_output_probe_response_timing_recorders.get(shape, {}),
                    )
                )
                metrics.update(
                    _recorder_metric_fields(
                        f"query_output_probe_{shape}_server",
                        query_output_probe_server_timing_recorders.get(shape, {}),
                    )
                )
                metrics[f"query_output_probe_{shape}_response_content_length_missing_count"] = (
                    query_output_probe_content_length_missing_counts.get(shape, 0)
                )
                metrics[f"query_output_probe_{shape}_response_content_length_mismatch_count"] = (
                    query_output_probe_content_length_mismatch_counts.get(shape, 0)
                )
            metrics.update(
                _duration_distribution_metric_fields(
                    "query_output_probe_explain_true_over_false_delta",
                    query_output_probe_true_over_false_deltas,
                )
            )
            metrics.update(
                _duration_distribution_metric_fields(
                    "query_output_probe_explain_endpoint_over_false_delta",
                    query_output_probe_explain_endpoint_over_false_deltas,
                )
            )
            metrics.update(
                _numeric_distribution_metric_fields(
                    "query_output_probe_phase_timing_count",
                    query_output_probe_phase_timing_counts,
                )
            )
            metrics.update(
                _numeric_distribution_metric_fields(
                    "query_output_probe_access_path_timing_count",
                    query_output_probe_access_path_timing_counts,
                )
            )
            metrics.update(
                _numeric_distribution_metric_fields(
                    "query_output_probe_planner_candidate_count",
                    query_output_probe_planner_candidate_counts,
                )
            )
            metrics.update(
                _recorder_metric_fields(
                    "query_access_path",
                    {
                        f"{access_path}_build": recorder
                        for access_path, recorder in query_access_path_build_recorders.items()
                    },
                )
            )
            metrics.update(
                _recorder_metric_fields(
                    "query_access_path",
                    {
                        f"{access_path}_open": recorder
                        for access_path, recorder in query_access_path_open_recorders.items()
                    },
                )
            )
            metrics.update(
                _top_level_byte_metric_fields(
                    "disk_bytes_after_ingest",
                    disk_bytes_after_ingest_by_top_level,
                )
            )
            metrics.update(
                _top_level_byte_metric_fields(
                    "disk_bytes_after_workload",
                    disk_bytes_after_workload_by_top_level,
                )
            )
            notes = [
                "TraceDB HTTP/curl records/query/delete smoke passed",
                ingest_mode_note,
                "TraceDB HTTP falsification checks passed: fresh-write, patch, tenant isolation, freshness modes, compact, snapshot, restore, explain, tombstone",
                "TraceDB HTTP retrieval diagnostics: "
                f"min_recall_at_5={metrics['min_recall_at_5']}; "
                f"queries_below_full_recall={queries_below_full_recall}; "
                f"queries_with_zero_recall={queries_with_zero_recall}",
                "TraceDB HTTP filter parity diagnostics: "
                f"category_filter_applied={str(category_filter_applied).lower()}; "
                f"off_category_result_count={off_category_result_count}; "
                f"queries_with_off_category_results={queries_with_off_category_results}",
            ]
            if disk_bytes_after_ingest > 0:
                notes.append(
                    "TraceDB HTTP data directory bytes measured after ingest: "
                    f"{disk_bytes_after_ingest}; after workload: {disk_bytes_after_workload}"
                )
            if query_phase_recorders or query_access_path_build_recorders:
                notes.append(
                    "TraceDB HTTP query phase attribution recorded: "
                    f"phases={len(query_phase_recorders)}; "
                    f"access_paths={len(query_access_path_build_recorders)}"
                )
            if query_server_timing_recorders or query_http_client_overhead_recorder.latencies_ms:
                notes.append(
                    "TraceDB HTTP server/client query attribution recorded: "
                    f"server_timing_fields={len(query_server_timing_recorders)}; "
                    f"client_overhead_samples={len(query_http_client_overhead_recorder.latencies_ms)}"
                )
            if query_http_response_body_bytes:
                notes.append(
                    "TraceDB HTTP response attribution recorded: "
                    f"body_bytes_p95={metrics.get('query_http_response_body_bytes_p95', 0)}; "
                    "content_length_mismatches="
                    f"{query_http_response_content_length_mismatch_count}"
                )
            if query_output_probe_count:
                notes.append(
                    "TraceDB HTTP output-shape attribution recorded: "
                    f"probe_count={query_output_probe_count}; "
                    "shapes=explain_false,explain_true,explain_endpoint; "
                    "order=fixed_explain_false_then_explain_true_then_explain_endpoint; "
                    "latency_comparison=order_biased; "
                    "explain_false_returned_explain="
                    f"{query_output_probe_explain_false_explain_returned_count}"
                )
            if query_explain_observer_count:
                notes.append(
                    "TraceDB HTTP observer explain attribution recorded after measured "
                    "lean query loop: "
                    f"observer_explain_queries={query_explain_observer_count}; "
                    "primary_query_explain=false"
                )
            if disk_bytes_after_ingest_by_top_level:
                notes.append(
                    "TraceDB HTTP storage attribution recorded: "
                    + ", ".join(
                        f"{name}={bytes_value}"
                        for name, bytes_value in sorted(
                            disk_bytes_after_ingest_by_top_level.items()
                        )
                    )
                )
            return notes, metrics
        except Exception as error:
            return [f"surface unavailable: TraceDB HTTP records/query/delete failed: {error}"], None

    def _record_input(self, table: str, record: Any) -> dict[str, Any]:
        return {
            "table": table,
            "id": record.record_id,
            "tenant_id": record.tenant_id,
            "fields": {
                "id": record.record_id,
                "tenant": record.tenant_id,
                "body": record.body,
                "category": record.category,
                "status": record.status,
                "rating": record.rating,
                "year": record.year,
                "embedding": record.vector,
            },
        }

    def _is_local_http_url(self, base_url: str) -> bool:
        host = urlparse(base_url).hostname or ""
        return host in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}

    def _path_token(self, value: str) -> str:
        return "".join(character if character.isalnum() else "_" for character in value)[:80]

    def _float_env(self, name: str, default: float) -> float:
        try:
            return float(os.environ.get(name, ""))
        except ValueError:
            return default


def _directory_bytes(path_value: str | None) -> int:
    if not path_value:
        return 0
    root = Path(path_value)
    if not root.exists():
        return 0
    total = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            total += path.stat().st_size
        except OSError:
            continue
    return total


def _directory_top_level_bytes(path_value: str | None) -> dict[str, int]:
    if not path_value:
        return {}
    root = Path(path_value)
    if not root.exists():
        return {}
    totals: dict[str, int] = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        try:
            relative = path.relative_to(root)
        except ValueError:
            continue
        if not relative.parts:
            continue
        top_level = _metric_token(relative.parts[0])
        totals[top_level] = totals.get(top_level, 0) + size
    return totals


def _record_explain_timing_metrics(
    explain: dict[str, Any],
    phase_recorders: dict[str, MetricRecorder],
    access_path_build_recorders: dict[str, MetricRecorder],
    access_path_open_recorders: dict[str, MetricRecorder],
) -> None:
    for phase_timing in explain.get("phase_timings", []):
        if not isinstance(phase_timing, dict):
            continue
        phase = _metric_token(str(phase_timing.get("phase", "")))
        if not phase:
            continue
        elapsed_ms = _float_metric(phase_timing.get("elapsed_ms"))
        if elapsed_ms is None:
            continue
        phase_recorders.setdefault(phase, MetricRecorder()).latencies_ms.append(elapsed_ms)
    for access_path_timing in explain.get("access_path_timings", []):
        if not isinstance(access_path_timing, dict):
            continue
        access_path = _metric_token(str(access_path_timing.get("access_path_id", "")))
        if not access_path:
            continue
        build_ms = _float_metric(access_path_timing.get("build_ms"))
        if build_ms is not None:
            access_path_build_recorders.setdefault(
                access_path, MetricRecorder()
            ).latencies_ms.append(build_ms)
        open_ms = _float_metric(access_path_timing.get("open_ms"))
        if open_ms is not None:
            access_path_open_recorders.setdefault(
                access_path, MetricRecorder()
            ).latencies_ms.append(open_ms)


def _explain_phase_total_ms(explain: dict[str, Any]) -> float | None:
    total = 0.0
    found = False
    for phase_timing in explain.get("phase_timings", []):
        if not isinstance(phase_timing, dict):
            continue
        elapsed_ms = _float_metric(phase_timing.get("elapsed_ms"))
        if elapsed_ms is None:
            continue
        total += elapsed_ms
        found = True
    return total if found else None


def _record_server_timing_metrics(
    headers: dict[str, str],
    recorders: dict[str, MetricRecorder],
) -> dict[str, float]:
    values: dict[str, float] = {}
    for entry in headers.get("server-timing", "").split(","):
        name_and_params = [part.strip() for part in entry.split(";") if part.strip()]
        if not name_and_params:
            continue
        name = _metric_token(name_and_params[0])
        if not name:
            continue
        duration_ms = None
        for param in name_and_params[1:]:
            key, separator, value = param.partition("=")
            if separator and key.strip().lower() == "dur":
                duration_ms = _float_metric(value.strip().strip('"'))
                break
        if duration_ms is None:
            continue
        recorders.setdefault(name, MetricRecorder()).latencies_ms.append(duration_ms)
        values[name] = duration_ms
    return values


def _record_response_metadata_metrics(
    response_meta: dict[str, float | int],
    timing_recorders: dict[str, MetricRecorder],
    body_bytes: list[float],
    content_length_bytes: list[float],
) -> None:
    body_byte_count = _float_metric(response_meta.get("body_bytes"))
    if body_byte_count is not None:
        body_bytes.append(body_byte_count)
    content_length = _float_metric(response_meta.get("content_length_bytes"))
    if content_length is not None:
        content_length_bytes.append(content_length)
    for source_name, metric_name in [
        ("header_wait_ms", "header_wait"),
        ("body_read_ms", "body_read"),
        ("decode_ms", "decode"),
        ("json_parse_ms", "json_parse"),
        ("processing_ms", "processing"),
    ]:
        value = _float_metric(response_meta.get(source_name))
        if value is not None:
            timing_recorders.setdefault(metric_name, MetricRecorder()).latencies_ms.append(value)


def _single_recorder_metric_fields(
    prefix: str, recorder: MetricRecorder
) -> dict[str, float | int]:
    fields: dict[str, float | int] = {f"{prefix}_count": len(recorder.latencies_ms)}
    fields.update({f"{prefix}_{key}": value for key, value in recorder.summary().items()})
    return fields


def _response_byte_metric_fields(prefix: str, values: list[float]) -> dict[str, float | int]:
    if not values:
        return {}
    return {
        f"{prefix}_p50": round(percentile(values, 50), 3),
        f"{prefix}_p95": round(percentile(values, 95), 3),
        f"{prefix}_p99": round(percentile(values, 99), 3),
    }


def _numeric_distribution_metric_fields(prefix: str, values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    return {
        f"{prefix}_p50": round(percentile(values, 50), 3),
        f"{prefix}_p95": round(percentile(values, 95), 3),
        f"{prefix}_p99": round(percentile(values, 99), 3),
    }


def _duration_distribution_metric_fields(prefix: str, values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    return {
        f"{prefix}_p50_ms": round(percentile(values, 50), 3),
        f"{prefix}_p95_ms": round(percentile(values, 95), 3),
        f"{prefix}_p99_ms": round(percentile(values, 99), 3),
    }


def _recorder_metric_fields(
    prefix: str, recorders: dict[str, MetricRecorder]
) -> dict[str, float | int]:
    fields: dict[str, float | int] = {}
    for name, recorder in sorted(recorders.items()):
        fields[f"{prefix}_{name}_count"] = len(recorder.latencies_ms)
        for key, value in recorder.summary().items():
            fields[f"{prefix}_{name}_{key}"] = value
    return fields


def _top_level_byte_metric_fields(prefix: str, values: dict[str, int]) -> dict[str, int]:
    return {f"{prefix}_{name}": bytes_value for name, bytes_value in sorted(values.items())}


def _metric_token(value: str) -> str:
    token = "".join(character.lower() if character.isalnum() else "_" for character in value)
    return "_".join(part for part in token.split("_") if part)


def _float_metric(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _missing_required_explain_fields(explain: Any) -> list[str]:
    if not isinstance(explain, dict):
        return ["explain"]
    return [
        key
        for key in [
            "opened_candidate_streams",
            "fusion_method",
            "freshness_mode",
            "scalar_filter_applied",
            "tenant_mask_visible_records",
        ]
        if key not in explain
    ]
