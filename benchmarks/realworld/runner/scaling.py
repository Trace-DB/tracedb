from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any, Callable

from .datasets import generated_dataset
from .metrics import MetricRecorder, percentile


TABLE = "scaling_records"
FEATURE = "embedding"
CommandRunner = Callable[[list[str]], tuple[int, str, str]]


@dataclass
class TraceDbScalingRunner:
    repo_root: Path
    tracedb_cli: Path
    data_dir: Path
    output_json: Path
    output_md: Path
    record_targets: list[int]
    inspect_repetitions: int = 5
    query_repetitions: int = 3
    checkpoint_at_points: bool = False
    run_command: CommandRunner | None = None

    def run(self) -> dict[str, Any]:
        if not self.record_targets:
            raise ValueError("at least one record target is required")
        if any(target <= 0 for target in self.record_targets):
            raise ValueError("record targets must be positive")
        targets = sorted(set(self.record_targets))
        max_records = targets[-1]
        self.output_json.parent.mkdir(parents=True, exist_ok=True)
        self.output_md.parent.mkdir(parents=True, exist_ok=True)
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        dataset = generated_dataset(max_records, seed=42)
        commands: list[dict[str, Any]] = []
        put_latencies: list[float] = []
        points: list[dict[str, Any]] = []

        with tempfile.TemporaryDirectory(prefix="tracedb-scaling-payloads-") as temp_dir:
            payload_dir = Path(temp_dir)
            self._call(["init"], commands)
            schema_path = self._write_payload(payload_dir, "schema.json", scaling_schema())
            self._call(["schema", "apply", str(schema_path)], commands)

            next_target_idx = 0
            for index, record in enumerate(dataset.records, start=1):
                record_path = self._write_payload(
                    payload_dir,
                    f"record-{index:06d}.json",
                    record_payload(record),
                )
                put_recorder = MetricRecorder()
                put_recorder.timed(lambda record_path=record_path: self._call(["put", str(record_path)], commands))
                put_latencies.extend(put_recorder.latencies_ms)

                while next_target_idx < len(targets) and index == targets[next_target_idx]:
                    points.append(
                        self._measure_point(
                            records=index,
                            put_latencies=put_latencies,
                            recent_put_latencies=put_latencies[-min(64, len(put_latencies)) :],
                            payload_dir=payload_dir,
                            commands=commands,
                        )
                    )
                    next_target_idx += 1

        report = {
            "benchmark": "tracedb-cli-open-recovery-scaling",
            "repo_root": str(self.repo_root),
            "data_dir": str(self.data_dir),
            "record_targets": targets,
            "inspect_repetitions": self.inspect_repetitions,
            "query_repetitions": self.query_repetitions,
            "points": points,
            "summary": {
                "max_records": max_records,
                "point_count": len(points),
                "failures": [],
                "interpretation": "CLI measurements include process startup plus TraceDb::open. Checkpoint metrics measure the same data directory after tracedb checkpoint truncates covered WAL entries.",
                "checkpoint_at_points": self.checkpoint_at_points,
            },
            "commands": commands,
        }
        self.output_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.output_md.write_text(render_scaling_markdown(report), encoding="utf-8")
        return report

    def _measure_point(
        self,
        *,
        records: int,
        put_latencies: list[float],
        recent_put_latencies: list[float],
        payload_dir: Path,
        commands: list[dict[str, Any]],
    ) -> dict[str, Any]:
        inspect_recorder = MetricRecorder()
        latest_epoch = 0
        for _ in range(self.inspect_repetitions):
            manifest = inspect_recorder.timed(lambda: self._call(["inspect", "manifest"], commands))
            latest_epoch = manifest.get("latest_epoch", latest_epoch)

        query = scaling_query(records)
        query_path = self._write_payload(payload_dir, f"query-{records}.json", query)
        query_recorder = MetricRecorder()
        returned_count = 0
        for _ in range(self.query_repetitions):
            result = query_recorder.timed(lambda: self._call(["query", str(query_path)], commands))
            returned_count = len(result.get("results", []))

        wal_path = self.data_dir / "wal" / "000001.twal"
        wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
        point = {
            "records": records,
            "latest_epoch": latest_epoch,
            "wal_bytes": wal_bytes,
            "data_dir_bytes": directory_size(self.data_dir),
            "put_latency_p50_ms": round(percentile(put_latencies, 50), 3),
            "put_latency_p95_ms": round(percentile(put_latencies, 95), 3),
            "recent_put_latency_p95_ms": round(percentile(recent_put_latencies, 95), 3),
            "reopen_latency_p50_ms": inspect_recorder.summary()["latency_p50_ms"],
            "reopen_latency_p95_ms": inspect_recorder.summary()["latency_p95_ms"],
            "query_latency_p50_ms": query_recorder.summary()["latency_p50_ms"],
            "query_latency_p95_ms": query_recorder.summary()["latency_p95_ms"],
            "query_returned_count": returned_count,
        }
        if self.checkpoint_at_points:
            point.update(
                self._measure_checkpointed_point(
                    query_path=query_path,
                    commands=commands,
                )
            )
        return point

    def _measure_checkpointed_point(
        self,
        *,
        query_path: Path,
        commands: list[dict[str, Any]],
    ) -> dict[str, Any]:
        checkpoint_recorder = MetricRecorder()
        checkpoint = checkpoint_recorder.timed(lambda: self._call(["checkpoint"], commands))
        inspect_recorder = MetricRecorder()
        checkpoint_epoch = checkpoint.get("checkpoint_epoch", 0)
        latest_epoch = checkpoint_epoch
        for _ in range(self.inspect_repetitions):
            manifest = inspect_recorder.timed(lambda: self._call(["inspect", "manifest"], commands))
            latest_epoch = manifest.get("latest_epoch", latest_epoch)
            checkpoint_epoch = manifest.get("checkpoint_epoch", checkpoint_epoch)

        query_recorder = MetricRecorder()
        returned_count = 0
        for _ in range(self.query_repetitions):
            result = query_recorder.timed(lambda: self._call(["query", str(query_path)], commands))
            returned_count = len(result.get("results", []))

        wal_path = self.data_dir / "wal" / "000001.twal"
        wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
        return {
            "checkpoint_epoch": checkpoint_epoch,
            "checkpoint_latest_epoch": latest_epoch,
            "checkpoint_wal_bytes": wal_bytes,
            "checkpoint_data_dir_bytes": directory_size(self.data_dir),
            "checkpoint_latency_ms": checkpoint_recorder.summary()["latency_p50_ms"],
            "checkpoint_reopen_latency_p50_ms": inspect_recorder.summary()["latency_p50_ms"],
            "checkpoint_reopen_latency_p95_ms": inspect_recorder.summary()["latency_p95_ms"],
            "checkpoint_query_latency_p50_ms": query_recorder.summary()["latency_p50_ms"],
            "checkpoint_query_latency_p95_ms": query_recorder.summary()["latency_p95_ms"],
            "checkpoint_query_returned_count": returned_count,
        }

    def _write_payload(self, payload_dir: Path, name: str, value: dict[str, Any]) -> Path:
        path = payload_dir / name
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return path

    def _call(self, args: list[str], commands: list[dict[str, Any]]) -> dict[str, Any]:
        command = [str(self.tracedb_cli), "--data", str(self.data_dir), *args]
        runner = self.run_command or run_subprocess
        code, stdout, stderr = runner(command)
        commands.append(
            {
                "command": command,
                "exit_code": code,
                "stdout": stdout.strip(),
                "stderr": stderr.strip(),
            }
        )
        if code != 0:
            raise RuntimeError(f"command failed: {' '.join(command)}\n{stderr}")
        return json.loads(stdout) if stdout.strip() else {}


def scaling_schema() -> dict[str, Any]:
    return {
        "name": TABLE,
        "primary_id_column": "id",
        "tenant_id_column": "tenant",
        "scalar_columns": ["category", "status"],
        "text_indexed_columns": ["title", "body"],
        "vector_columns": [{"name": FEATURE, "dimensions": 8, "source_columns": ["body"]}],
    }


def record_payload(record) -> dict[str, Any]:
    return {
        "table": TABLE,
        "id": record.record_id,
        "tenant_id": record.tenant_id,
        "fields": {
            "id": record.record_id,
            "tenant": record.tenant_id,
            "title": record.title,
            "body": record.body,
            "category": record.category,
            "status": record.status,
            "rating": record.rating,
            "year": record.year,
            "embedding": record.vector,
        },
    }


def scaling_query(records: int) -> dict[str, Any]:
    return {
        "table": TABLE,
        "tenant_id": "tenant-a",
        "text": f"agent memory vector retrieval policy freshness record {records}",
        "vector": [0.1, 0.2, 0.3, 0.4, 0.1, 0.2, 0.3, 0.4],
        "top_k": 5,
        "freshness": "AllowDirty",
        "explain": True,
    }


def render_scaling_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# TraceDB CLI Open/Recovery Scaling",
        "",
        f"- Max records: `{report['summary']['max_records']}`",
        f"- Data dir: `{report['data_dir']}`",
        "",
        "> CLI measurements include process startup plus TraceDb::open. Checkpoint metrics measure the same data directory after `tracedb checkpoint` truncates covered WAL entries.",
        "",
        "| records | latest epoch | WAL bytes | put p95 ms | recent put p95 ms | reopen p95 ms | query p95 ms | returned | checkpoint WAL bytes | checkpoint reopen p95 ms | checkpoint query p95 ms |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for point in report["points"]:
        display = {
            "checkpoint_wal_bytes": "n/a",
            "checkpoint_reopen_latency_p95_ms": "n/a",
            "checkpoint_query_latency_p95_ms": "n/a",
            **point,
        }
        lines.append(
            "| {records} | {latest_epoch} | {wal_bytes} | {put_latency_p95_ms} | {recent_put_latency_p95_ms} | {reopen_latency_p95_ms} | {query_latency_p95_ms} | {query_returned_count} | {checkpoint_wal_bytes} | {checkpoint_reopen_latency_p95_ms} | {checkpoint_query_latency_p95_ms} |".format(
                **display,
            )
        )
    lines.extend(["", "## Next Use", "", "Use this curve to decide whether checkpoint files, WAL rotation, or store-clone reduction should be the next implementation target."])
    return "\n".join(lines) + "\n"


def compare_inprocess_scaling_reports(
    baseline_reports: list[dict[str, Any]],
    candidate_reports: list[dict[str, Any]],
    *,
    baseline_label: str,
    candidate_label: str,
    min_repeats: int = 2,
    required_write_improvement_pct: float = 25.0,
    allowed_query_regression_pct: float = 10.0,
    allowed_query_regression_ms: float = 5.0,
) -> dict[str, Any]:
    baseline_points = _collect_inprocess_points(baseline_reports)
    candidate_points = _collect_inprocess_points(candidate_reports)
    baseline_records = set(baseline_points)
    candidate_records = set(candidate_points)
    common_records = sorted(baseline_records & candidate_records)
    missing_baseline_records = sorted(candidate_records - baseline_records)
    missing_candidate_records = sorted(baseline_records - candidate_records)

    failures = []
    if not common_records:
        failures.append("baseline and candidate reports have no common record targets")
    if missing_baseline_records:
        failures.append(f"baseline is missing record targets: {missing_baseline_records}")
    if missing_candidate_records:
        failures.append(f"candidate is missing record targets: {missing_candidate_records}")
    for records in common_records:
        baseline_repeat_count = len(baseline_points[records])
        candidate_repeat_count = len(candidate_points[records])
        if baseline_repeat_count < min_repeats:
            failures.append(
                f"{records} baseline repeats {baseline_repeat_count} below min_repeats {min_repeats}"
            )
        if candidate_repeat_count < min_repeats:
            failures.append(
                f"{records} candidate repeats {candidate_repeat_count} below min_repeats {min_repeats}"
            )

    if failures:
        return {
            "comparison": "tracedb-inprocess-scaling-guard",
            "status": "invalid",
            "baseline_label": baseline_label,
            "candidate_label": candidate_label,
            "baseline_reports": len(baseline_reports),
            "candidate_reports": len(candidate_reports),
            "min_repeats": min_repeats,
            "required_write_improvement_pct": required_write_improvement_pct,
            "allowed_query_regression_pct": allowed_query_regression_pct,
            "allowed_query_regression_ms": allowed_query_regression_ms,
            "control_status": "same_machine_parent_baseline_required",
            "missing_baseline_records": missing_baseline_records,
            "missing_candidate_records": missing_candidate_records,
            "failures": failures,
            "points": [],
        }

    points = []
    for records in common_records:
        baseline_insert = _median_metric(baseline_points[records], "recent_insert_p95_ms")
        candidate_insert = _median_metric(candidate_points[records], "recent_insert_p95_ms")
        baseline_query = _median_metric(baseline_points[records], "engine_query_p95_ms")
        candidate_query = _median_metric(candidate_points[records], "engine_query_p95_ms")
        baseline_checkpoint_query = _optional_median_metric(
            baseline_points[records],
            "checkpoint_engine_query_p95_ms",
        )
        candidate_checkpoint_query = _optional_median_metric(
            candidate_points[records],
            "checkpoint_engine_query_p95_ms",
        )

        write_improvement_pct = _improvement_pct(baseline_insert, candidate_insert)
        hot_query_regression_ms = candidate_query - baseline_query
        hot_query_regression_pct = _regression_pct(baseline_query, candidate_query)
        hot_query_allowed_ms = max(
            baseline_query * (allowed_query_regression_pct / 100.0),
            allowed_query_regression_ms,
        )
        write_gate = (
            "passed"
            if write_improvement_pct >= required_write_improvement_pct
            else "failed"
        )
        hot_query_gate = (
            "passed"
            if candidate_query <= baseline_query + hot_query_allowed_ms
            else "failed"
        )

        checkpoint_query_gate = "not_applicable"
        checkpoint_query_regression_ms = None
        checkpoint_query_regression_pct = None
        checkpoint_query_allowed_ms = None
        if baseline_checkpoint_query is not None and candidate_checkpoint_query is not None:
            checkpoint_query_regression_ms = candidate_checkpoint_query - baseline_checkpoint_query
            checkpoint_query_regression_pct = _regression_pct(
                baseline_checkpoint_query,
                candidate_checkpoint_query,
            )
            checkpoint_query_allowed_ms = max(
                baseline_checkpoint_query * (allowed_query_regression_pct / 100.0),
                allowed_query_regression_ms,
            )
            checkpoint_query_gate = (
                "passed"
                if candidate_checkpoint_query
                <= baseline_checkpoint_query + checkpoint_query_allowed_ms
                else "failed"
            )

        point_status = (
            "accepted"
            if write_gate == "passed"
            and hot_query_gate == "passed"
            and checkpoint_query_gate != "failed"
            else "rejected"
        )
        points.append(
            {
                "records": records,
                "status": point_status,
                "baseline_recent_insert_p95_ms": round(baseline_insert, 3),
                "candidate_recent_insert_p95_ms": round(candidate_insert, 3),
                "write_improvement_pct": round(write_improvement_pct, 3),
                "write_gate": write_gate,
                "baseline_hot_query_p95_ms": round(baseline_query, 3),
                "candidate_hot_query_p95_ms": round(candidate_query, 3),
                "hot_query_regression_ms": round(hot_query_regression_ms, 3),
                "hot_query_regression_pct": round(hot_query_regression_pct, 3),
                "hot_query_allowed_regression_ms": round(hot_query_allowed_ms, 3),
                "hot_query_gate": hot_query_gate,
                "baseline_checkpoint_query_p95_ms": _round_optional(
                    baseline_checkpoint_query
                ),
                "candidate_checkpoint_query_p95_ms": _round_optional(
                    candidate_checkpoint_query
                ),
                "checkpoint_query_regression_ms": _round_optional(
                    checkpoint_query_regression_ms
                ),
                "checkpoint_query_regression_pct": _round_optional(
                    checkpoint_query_regression_pct
                ),
                "checkpoint_query_allowed_regression_ms": _round_optional(
                    checkpoint_query_allowed_ms
                ),
                "checkpoint_query_gate": checkpoint_query_gate,
            }
        )

    status = "accepted" if all(point["status"] == "accepted" for point in points) else "rejected"
    return {
        "comparison": "tracedb-inprocess-scaling-guard",
        "status": status,
        "baseline_label": baseline_label,
        "candidate_label": candidate_label,
        "baseline_reports": len(baseline_reports),
        "candidate_reports": len(candidate_reports),
        "min_repeats": min_repeats,
        "required_write_improvement_pct": required_write_improvement_pct,
        "allowed_query_regression_pct": allowed_query_regression_pct,
        "allowed_query_regression_ms": allowed_query_regression_ms,
        "control_status": "same_machine_parent_baseline_required",
        "missing_baseline_records": [],
        "missing_candidate_records": [],
        "failures": [
            f"{point['records']}: write gate {point['write_gate']}, hot query gate {point['hot_query_gate']}, checkpoint query gate {point['checkpoint_query_gate']}"
            for point in points
            if point["status"] != "accepted"
        ],
        "points": points,
    }


def render_inprocess_scaling_comparison_markdown(comparison: dict[str, Any]) -> str:
    lines = [
        "# TraceDB In-Process Scaling Guard",
        "",
        f"- Status: `{comparison['status']}`",
        f"- Baseline: `{comparison['baseline_label']}`",
        f"- Candidate: `{comparison['candidate_label']}`",
        f"- Baseline reports: `{comparison.get('baseline_reports', 0)}`",
        f"- Candidate reports: `{comparison.get('candidate_reports', 0)}`",
        f"- Minimum repeats per side/record: `{comparison.get('min_repeats', 1)}`",
        f"- Required write improvement: `{comparison['required_write_improvement_pct']}%`",
        f"- Allowed hot/checkpoint query regression: `max({comparison['allowed_query_regression_pct']}%, {comparison['allowed_query_regression_ms']} ms)`",
        "",
        "| records | status | write improvement % | write gate | hot query baseline -> candidate | hot query gate | checkpoint query baseline -> candidate | checkpoint query gate |",
        "| ---: | --- | ---: | --- | ---: | --- | ---: | --- |",
    ]
    for point in comparison["points"]:
        checkpoint_pair = "n/a"
        if point["baseline_checkpoint_query_p95_ms"] is not None:
            checkpoint_pair = (
                f"{point['baseline_checkpoint_query_p95_ms']} -> "
                f"{point['candidate_checkpoint_query_p95_ms']}"
            )
        lines.append(
            "| {records} | {status} | {write_improvement_pct} | {write_gate} | {baseline_hot_query_p95_ms} -> {candidate_hot_query_p95_ms} | {hot_query_gate} | {checkpoint_pair} | {checkpoint_query_gate} |".format(
                checkpoint_pair=checkpoint_pair,
                **point,
            )
        )
    lines.extend(
        [
            "",
            "## Failures",
            "",
        ]
    )
    failures = comparison.get("failures", [])
    if failures:
        lines.extend(f"- {failure}" for failure in failures)
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "A candidate is accepted only when recent write p95 improves enough and hot/checkpoint query p95 stay inside the read-regression guard. This is internal development evidence unless an external control is attached separately.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_inprocess_scaling_compare(args) -> int:
    baseline_reports = [_read_json(Path(path)) for path in args.baseline_json]
    candidate_reports = [_read_json(Path(path)) for path in args.candidate_json]
    comparison = compare_inprocess_scaling_reports(
        baseline_reports,
        candidate_reports,
        baseline_label=args.baseline_label,
        candidate_label=args.candidate_label,
        min_repeats=args.min_repeats,
        required_write_improvement_pct=args.required_write_improvement_pct,
        allowed_query_regression_pct=args.allowed_query_regression_pct,
        allowed_query_regression_ms=args.allowed_query_regression_ms,
    )
    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(comparison, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(render_inprocess_scaling_comparison_markdown(comparison), encoding="utf-8")
    print(f"wrote {output_json}")
    print(f"wrote {output_md}")
    return 0 if comparison["status"] == "accepted" else 1


def directory_size(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _collect_inprocess_points(reports: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    points: dict[int, list[dict[str, Any]]] = {}
    for report in reports:
        if report.get("benchmark") != "tracedb-inprocess-scaling":
            raise ValueError("expected tracedb-inprocess-scaling report")
        for point in report.get("points", []):
            points.setdefault(int(point["records"]), []).append(point)
    return points


def _median_metric(points: list[dict[str, Any]], metric: str) -> float:
    values = [
        float(point[metric])
        for point in points
        if point.get(metric) is not None
    ]
    if not values:
        raise ValueError(f"metric {metric} is missing from comparison points")
    return float(median(values))


def _optional_median_metric(points: list[dict[str, Any]], metric: str) -> float | None:
    values = [
        float(point[metric])
        for point in points
        if point.get(metric) is not None
    ]
    return float(median(values)) if values else None


def _improvement_pct(baseline: float, candidate: float) -> float:
    if baseline <= 0.0:
        return 0.0
    return ((baseline - candidate) / baseline) * 100.0


def _regression_pct(baseline: float, candidate: float) -> float:
    if baseline <= 0.0:
        return 0.0
    return ((candidate - baseline) / baseline) * 100.0


def _round_optional(value: float | None) -> float | None:
    return round(value, 3) if value is not None else None


def run_subprocess(command: list[str]) -> tuple[int, str, str]:
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout, completed.stderr


def parse_record_targets(raw: str) -> list[int]:
    targets = [int(part.strip()) for part in raw.split(",") if part.strip()]
    if not targets:
        raise ValueError("at least one target is required")
    return sorted(set(targets))


def resolve_tracedb_cli(repo_root: Path, explicit: str) -> Path:
    if explicit:
        return Path(explicit)
    return repo_root / "target" / "debug" / "tracedb"


def run_tracedb_scaling(args) -> int:
    lab_root = Path.cwd()
    repo_root = lab_root.parent.parent
    tracedb_cli = resolve_tracedb_cli(repo_root, args.tracedb_cli)
    if not tracedb_cli.exists():
        raise SystemExit(f"TraceDB CLI not found at {tracedb_cli}; run cargo build -p tracedb-cli")
    data_dir = Path(args.data_dir) if args.data_dir else Path(tempfile.mkdtemp(prefix="tracedb-scaling-db-"))
    runner = TraceDbScalingRunner(
        repo_root=repo_root,
        tracedb_cli=tracedb_cli,
        data_dir=data_dir,
        output_json=Path(args.output_json),
        output_md=Path(args.output_md),
        record_targets=parse_record_targets(args.records),
        inspect_repetitions=args.inspect_repetitions,
        query_repetitions=args.query_repetitions,
        checkpoint_at_points=args.checkpoint_at_points,
    )
    runner.run()
    print(f"wrote {args.output_json}")
    print(f"wrote {args.output_md}")
    return 0
