from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


TABLE = "chat_memory"
TENANT = "tenant-alpha"
SUBJECT_TO_DELETE = "user-erased"
FEATURE = "embedding"


CommandRunner = Callable[[list[str]], tuple[int, str, str]]


def fixture_records() -> list[dict[str, Any]]:
    rows = [
        (
            "alpha-memory-1",
            "tenant-alpha",
            "user-ada",
            "Ada prefers deterministic local memory demos with hybrid search.",
            [1.0, 0.0, 0.0, 0.0],
        ),
        (
            "alpha-memory-2",
            "tenant-alpha",
            "user-ada",
            "Ada asked TraceDB to remember CLI-first chat history.",
            [0.92, 0.08, 0.0, 0.0],
        ),
        (
            "alpha-pending",
            "tenant-alpha",
            "user-grace",
            "Grace has a lifecycle vector that will become pending.",
            [0.0, 1.0, 0.0, 0.0],
        ),
        (
            "alpha-failed",
            "tenant-alpha",
            "user-grace",
            "Grace has a lifecycle vector that will become failed.",
            [0.0, 0.92, 0.08, 0.0],
        ),
        (
            "alpha-erased-1",
            "tenant-alpha",
            "user-erased",
            "Erasure subject remembers a billing token and removal request.",
            [0.0, 0.0, 1.0, 0.0],
        ),
        (
            "alpha-erased-2",
            "tenant-alpha",
            "user-erased",
            "Erasure subject has a second chat memory to remove from live queries.",
            [0.0, 0.0, 0.95, 0.05],
        ),
        (
            "beta-memory-1",
            "tenant-beta",
            "user-ada",
            "Beta tenant has similar Ada text but must stay isolated.",
            [1.0, 0.0, 0.0, 0.0],
        ),
    ]
    return [
        {
            "table": TABLE,
            "id": record_id,
            "tenant_id": tenant_id,
            "fields": {
                "id": record_id,
                "tenant": tenant_id,
                "user_id": user_id,
                "body": body,
                "kind": "chat_memory",
                "embedding": vector,
            },
        }
        for record_id, tenant_id, user_id, body, vector in rows
    ]


def schema() -> dict[str, Any]:
    return {
        "name": TABLE,
        "primary_id_column": "id",
        "tenant_id_column": "tenant",
        "scalar_columns": ["user_id", "kind"],
        "text_indexed_columns": ["body"],
        "vector_columns": [
            {
                "name": FEATURE,
                "dimensions": 4,
                "source_columns": ["body"],
            }
        ],
    }


def query_payload(
    *,
    tenant_id: str,
    text: str,
    vector: list[float],
    freshness: str = "Strict",
    top_k: int = 5,
) -> dict[str, Any]:
    return {
        "table": TABLE,
        "tenant_id": tenant_id,
        "text": text,
        "vector": vector,
        "top_k": top_k,
        "freshness": freshness,
        "explain": True,
    }


@dataclass
class ChatDemoRunner:
    repo_root: Path
    tracedb_cli: Path
    data_dir: Path
    output_json: Path
    output_md: Path
    run_command: CommandRunner | None = None

    def run(self) -> dict[str, Any]:
        self.output_json.parent.mkdir(parents=True, exist_ok=True)
        self.output_md.parent.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="tracedb-chat-demo-payloads-") as temp_dir:
            payload_dir = Path(temp_dir)
            commands: list[dict[str, Any]] = []

            self._call(["init"], commands)
            schema_path = self._write_payload(payload_dir, "schema.json", schema())
            self._call(["schema", "apply", str(schema_path)], commands)

            records = fixture_records()
            for record in records:
                path = self._write_payload(payload_dir, f"{record['id']}.json", record)
                self._call(["put", str(path)], commands)

            baseline_query_path = self._write_payload(
                payload_dir,
                "query-baseline.json",
                query_payload(
                    tenant_id=TENANT,
                    text="deterministic local memory hybrid",
                    vector=[1.0, 0.0, 0.0, 0.0],
                    freshness="Strict",
                ),
            )
            baseline = self._call(["query", str(baseline_query_path)], commands)

            patch_path = self._write_payload(
                payload_dir,
                "patch-alpha-memory-1.json",
                {
                    "table": TABLE,
                    "tenant_id": TENANT,
                    "id": "alpha-memory-1",
                    "fields": {
                        "body": "Ada updated the memory after the embedding was generated."
                    },
                },
            )
            self._call(["patch", str(patch_path)], commands)
            dirty_query_path = self._write_payload(
                payload_dir,
                "query-dirty.json",
                query_payload(
                    tenant_id=TENANT,
                    text="updated memory embedding generated",
                    vector=[1.0, 0.0, 0.0, 0.0],
                    freshness="AllowDirty",
                ),
            )
            dirty = self._call(["query", str(dirty_query_path)], commands)

            pending_status = self._call(
                ["feature", "status", "set", TABLE, TENANT, "alpha-pending", FEATURE, "Pending"],
                commands,
            )
            failed_status = self._call(
                ["feature", "status", "set", TABLE, TENANT, "alpha-failed", FEATURE, "Failed"],
                commands,
            )
            lifecycle_query_path = self._write_payload(
                payload_dir,
                "query-lifecycle.json",
                query_payload(
                    tenant_id=TENANT,
                    text="lifecycle vector",
                    vector=[0.0, 1.0, 0.0, 0.0],
                    freshness="Strict",
                ),
            )
            lifecycle = self._call(["query", str(lifecycle_query_path)], commands)

            erasure_query_path = self._write_payload(
                payload_dir,
                "query-erasure-before.json",
                query_payload(
                    tenant_id=TENANT,
                    text="erasure subject billing token removal",
                    vector=[0.0, 0.0, 1.0, 0.0],
                    freshness="Strict",
                ),
            )
            erasure_before = self._call(["query", str(erasure_query_path)], commands)
            deleted_ids = [
                record["id"]
                for record in records
                if record["tenant_id"] == TENANT
                and record["fields"]["user_id"] == SUBJECT_TO_DELETE
            ]
            for record_id in deleted_ids:
                self._call(["delete", TABLE, TENANT, record_id], commands)
            erasure_after_query_path = self._write_payload(
                payload_dir,
                "query-erasure-after.json",
                query_payload(
                    tenant_id=TENANT,
                    text="erasure subject billing token removal",
                    vector=[0.0, 0.0, 1.0, 0.0],
                    freshness="Strict",
                ),
            )
            erasure_after = self._call(["query", str(erasure_after_query_path)], commands)
            beta_get = self._call(["get", TABLE, "tenant-beta", "beta-memory-1"], commands)

        baseline_ids = result_ids(baseline)
        dirty_counts = feature_counts(dirty)
        lifecycle_counts = feature_counts(lifecycle)
        erasure_before_ids = result_ids(erasure_before)
        erasure_after_ids = result_ids(erasure_after)
        beta_visible = beta_get.get("record") is not None
        report = {
            "demo": "local-chat-memory",
            "repo_root": str(self.repo_root),
            "data_dir": str(self.data_dir),
            "fixture": {
                "table": TABLE,
                "records": records,
                "record_count": len(records),
                "tenants": sorted({record["tenant_id"] for record in records}),
            },
            "summary": {
                "records_inserted": len(records),
                "baseline_result_ids": baseline_ids,
                "dirty_feature_counts": dirty_counts,
                "pending_failed_feature_counts": lifecycle_counts,
                "pending_status": pending_status,
                "failed_status": failed_status,
                "deleted_subject": {"tenant_id": TENANT, "user_id": SUBJECT_TO_DELETE},
                "deleted_subject_records": deleted_ids,
                "erasure_before_result_ids": erasure_before_ids,
                "erasure_after_result_ids": erasure_after_ids,
                "erased_subject_visible_after_delete": any(
                    record_id in erasure_after_ids for record_id in deleted_ids
                ),
                "beta_tenant_record_visible": beta_visible,
            },
            "commands": commands,
            "caveats": [
                "local logical demo only",
                "no cloud dependency",
                "no legal export/purge claim",
                "no LangSmith canonical storage",
            ],
        }
        failures = demo_invariant_failures(report)
        report["invariant_failures"] = failures
        self.output_json.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.output_md.write_text(render_markdown_report(report), encoding="utf-8")
        if failures:
            raise RuntimeError("chat demo invariant check failed: " + "; ".join(failures))
        return report

    def _write_payload(self, payload_dir: Path, name: str, value: dict[str, Any]) -> Path:
        path = payload_dir / name
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return path

    def _call(self, args: list[str], commands: list[dict[str, Any]]) -> dict[str, Any]:
        command = [str(self.tracedb_cli), "--data", str(self.data_dir), *args]
        runner = self.run_command or run_subprocess
        code, stdout, stderr = runner(command)
        entry = {
            "command": command,
            "exit_code": code,
            "stdout": stdout.strip(),
            "stderr": stderr.strip(),
        }
        commands.append(entry)
        if code != 0:
            raise RuntimeError(f"command failed: {' '.join(command)}\n{stderr}")
        if not stdout.strip():
            return {}
        return json.loads(stdout)


def result_ids(output: dict[str, Any]) -> list[str]:
    return [row.get("record_id", "") for row in output.get("results", [])]


def feature_counts(output: dict[str, Any]) -> dict[str, int]:
    explain = output.get("explain", {})
    return {
        "dirty": int(explain.get("dirty_feature_count", 0)),
        "pending": int(explain.get("pending_feature_count", 0)),
        "failed": int(explain.get("failed_feature_count", 0)),
        "missing": int(explain.get("missing_feature_count", 0)),
    }


def demo_invariant_failures(report: dict[str, Any]) -> list[str]:
    summary = report.get("summary", {})
    failures = []
    baseline_ids = summary.get("baseline_result_ids", [])
    if not baseline_ids:
        failures.append("baseline tenant-alpha hybrid query returned no results")
    if "beta-memory-1" in baseline_ids:
        failures.append("baseline tenant-alpha query returned beta-memory-1")
    if summary.get("pending_status", {}).get("status") != "Pending":
        failures.append("pending status command did not return status Pending")
    if summary.get("failed_status", {}).get("status") != "Failed":
        failures.append("failed status command did not return status Failed")
    lifecycle_counts = summary.get("pending_failed_feature_counts", {})
    if int(lifecycle_counts.get("pending", 0)) < 1:
        failures.append("lifecycle query explain did not show pending >= 1")
    if int(lifecycle_counts.get("failed", 0)) < 1:
        failures.append("lifecycle query explain did not show failed >= 1")
    dirty_counts = summary.get("dirty_feature_counts", {})
    if int(dirty_counts.get("dirty", 0)) < 1:
        failures.append("dirty query explain did not show dirty >= 1")
    deleted_ids = set(summary.get("deleted_subject_records", []))
    erasure_before_ids = set(summary.get("erasure_before_result_ids", []))
    erasure_after_ids = set(summary.get("erasure_after_result_ids", []))
    missing_before = sorted(deleted_ids - erasure_before_ids)
    if missing_before:
        failures.append(
            "erased subject records were not visible before delete: "
            + ", ".join(missing_before)
        )
    still_visible = sorted(deleted_ids & erasure_after_ids)
    if still_visible:
        failures.append(
            "erased subject records were still visible after delete: "
            + ", ".join(still_visible)
        )
    if not summary.get("beta_tenant_record_visible"):
        failures.append("beta exact get was not visible")
    return failures


def run_subprocess(command: list[str]) -> tuple[int, str, str]:
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout, completed.stderr


def render_markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# TraceDB Local Chat-Memory Demo",
        "",
        "## Summary",
        "",
    ]
    for key, value in report.get("summary", {}).items():
        lines.append(f"- `{key}`: `{json.dumps(value, sort_keys=True)}`")
    lines.extend(["", "## Commands", ""])
    for index, command in enumerate(report.get("commands", []), start=1):
        lines.append(f"### {index}. `{shell_join(command['command'])}`")
        lines.append("")
        lines.append(f"- exit: `{command['exit_code']}`")
        if command.get("stdout"):
            lines.append("")
            lines.append("```json")
            lines.append(command["stdout"])
            lines.append("```")
        if command.get("stderr"):
            lines.append("")
            lines.append("```text")
            lines.append(command["stderr"])
            lines.append("```")
        lines.append("")
    lines.extend(["## Caveats", ""])
    for caveat in report.get("caveats", []):
        lines.append(f"- {caveat}")
    if "invariant_failures" in report:
        lines.extend(["", "## Invariant Failures", ""])
        failures = report.get("invariant_failures") or []
        if failures:
            for failure in failures:
                lines.append(f"- {failure}")
        else:
            lines.append("- none")
    return "\n".join(lines).rstrip() + "\n"


def shell_join(command: list[str]) -> str:
    return " ".join(shlex_quote(part) for part in command)


def shlex_quote(value: str) -> str:
    if not value:
        return "''"
    safe = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_@%+=:,./-")
    if all(char in safe for char in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_cli(repo_root: Path) -> Path:
    env_cli = os.environ.get("TRACEDB_CLI")
    if env_cli:
        return Path(env_cli)
    return repo_root / "target" / "debug" / "tracedb"


def prepare_data_dir(path: Path | None) -> Path:
    if path is not None:
        return path
    return Path(tempfile.mkdtemp(prefix="tracedb-chat-demo-")) / "db"


def run_chat_demo(args: Any) -> int:
    repo_root = default_repo_root()
    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    data_dir = prepare_data_dir(Path(args.data_dir) if args.data_dir else None)
    cli = Path(args.tracedb_cli) if args.tracedb_cli else default_cli(repo_root)
    if not shutil.which(str(cli)) and not cli.exists():
        raise RuntimeError(
            f"TraceDB CLI not found at {cli}; run `cargo build -p tracedb-cli` or pass --tracedb-cli"
        )
    report = ChatDemoRunner(
        repo_root=repo_root,
        tracedb_cli=cli,
        data_dir=data_dir,
        output_json=output_json,
        output_md=output_md,
    ).run()
    print(f"wrote {output_json}")
    print(f"wrote {output_md}")
    print(f"data_dir {report['data_dir']}")
    return 0
