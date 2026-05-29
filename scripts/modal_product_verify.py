from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

try:
    import modal
except ImportError:  # pragma: no cover - local unit tests do not require Modal.
    modal = None


REPO_ROOT = Path(__file__).resolve().parents[1]
REMOTE_REPO = "/workspace/TraceDB"
RUST_MODAL_IMAGE = "rust:1.94-bookworm"
MODAL_APP_NAME = "tracedb-product-verify"
MODAL_MIN_EPHEMERAL_DISK_MB = 524_288
DEFAULT_CPU = 8.0
DEFAULT_MEMORY_MB = 32_768
DEFAULT_TIMEOUT_SECONDS = 7_200
RECEIPT_PATH = Path("target/tracedb/product-quickstart.json")

MODAL_IGNORE_PATTERNS = [
    ".DS_Store",
    ".env",
    ".env.*",
    ".git/**",
    ".modal.toml",
    ".obsidian/**",
    ".tracedb/**",
    "target/**",
    "node_modules/**",
    "clients/typescript/node_modules/**",
    "benchmarks/realworld/.cache/**",
    "benchmarks/realworld/.env.local",
    "benchmarks/realworld/.venv/**",
    "benchmarks/realworld/report-bundles/**",
    "benchmarks/realworld/reports/**",
    "benchmarks/realworld/run-data/**",
]

EXPECTED_REDUCED_STEPS = [
    "embedded_demo",
    "embedded_verify",
    "http_demo",
    "local_doctor",
    "rust_sdk_quickstart",
    "python_sdk_smoke",
]
SKIPPED_TYPESCRIPT_STEPS = [
    "typescript_check",
    "typescript_http_smoke",
    "typescript_gateway_smoke",
]
ONLY_COMMAND_ALIASES = {
    "agent_memory_flight_recorder": [
        "agent-memory-flight-recorder-build",
        "agent-memory-flight-recorder",
    ],
    "typescript_gateway_smoke": [
        "typescript-npm-ci",
        "typescript-npm-public-gateway-smoke",
    ],
}


def build_command_plan(mode: str, *, only: str = "") -> list[dict[str, Any]]:
    mode = mode.strip().lower()
    commands: list[dict[str, Any]] = [
        {
            "name": "cargo-fmt",
            "argv": ["cargo", "fmt", "--all", "--", "--check"],
        },
        {
            "name": "quickstart-receipt-test",
            "argv": [
                "cargo",
                "test",
                "-p",
                "tracedb-cli",
                "--test",
                "demo",
                "product_quickstart_skip_typescript_uses_default_report_file_and_marks_reduced_evidence",
                "--",
                "--nocapture",
            ],
        },
        {
            "name": "quickstart-doc-contract-test",
            "argv": [
                "cargo",
                "test",
                "-p",
                "tracedb-testkit",
                "--test",
                "usability_acceptance",
                "local_product_regression_runner_declares_current_product_gate",
                "--",
                "--nocapture",
            ],
        },
        {
            "name": "platform-contract-doc-test",
            "argv": [
                "cargo",
                "test",
                "-p",
                "tracedb-testkit",
                "--test",
                "usability_acceptance",
                "platform_contract_v0_declares_sdk_conformance_harness",
                "--",
                "--exact",
                "--nocapture",
            ],
        },
        {
            "name": "platform-conformance-quick",
            "argv": [
                "python3",
                "scripts/platform_conformance.py",
                "--surface",
                "http_direct",
                "--surface",
                "rust_sdk",
                "--summary-json",
                "/tmp/tracedb-platform-conformance.json",
            ],
        },
        {
            "name": "agent-memory-flight-recorder-build",
            "argv": ["cargo", "build", "-p", "tracedb-cli"],
        },
        {
            "name": "agent-memory-flight-recorder",
            "argv": [
                "python3",
                "-m",
                "runner",
                "chat-demo",
                "--output-json",
                "/tmp/tracedb-agent-memory-flight-recorder.json",
                "--output-md",
                "/tmp/tracedb-agent-memory-flight-recorder.md",
            ],
            "cwd": "benchmarks/realworld",
            "receipt_json": "/tmp/tracedb-agent-memory-flight-recorder.json",
        },
        {
            "name": "product-quickstart-skip-typescript",
            "argv": [
                "cargo",
                "run",
                "-q",
                "-p",
                "tracedb-cli",
                "--",
                "product-quickstart",
                "--skip-typescript",
            ],
            "capture_stdout": True,
        },
    ]
    if mode == "quickstart":
        return select_only_commands(commands, only)
    if mode == "workspace":
        return select_only_commands(commands + [
            {
                "name": "traceql-sqlish-conformance",
                "argv": [
                    "python3",
                    "scripts/platform_conformance.py",
                    "--surface",
                    "traceql_sqlish",
                    "--summary-json",
                    "/tmp/tracedb-traceql-sqlish-conformance.json",
                ],
            },
            {
                "name": "api-parity-conformance",
                "argv": [
                    "cargo",
                    "run",
                    "-q",
                    "-p",
                    "tracedb-cli",
                    "--",
                    "api-parity",
                    "--report-file",
                    "/tmp/tracedb-api-parity.json",
                ],
            },
            {
                "name": "graphql-native-conformance",
                "argv": [
                    "python3",
                    "scripts/platform_conformance.py",
                    "--surface",
                    "graphql",
                    "--summary-json",
                    "/tmp/tracedb-graphql-conformance.json",
                ],
            },
            {
                "name": "typescript-npm-ci",
                "argv": ["npm", "ci"],
                "cwd": "clients/typescript",
            },
            {
                "name": "typescript-npm-check",
                "argv": ["npm", "run", "check"],
                "cwd": "clients/typescript",
            },
            {
                "name": "typescript-npm-public-http-smoke",
                "argv": ["npm", "run", "public-http-smoke"],
                "cwd": "clients/typescript",
            },
            {
                "name": "typescript-sdk-conformance",
                "argv": [
                    "python3",
                    "scripts/platform_conformance.py",
                    "--surface",
                    "typescript_sdk",
                    "--summary-json",
                    "/tmp/tracedb-typescript-sdk-conformance.json",
                ],
            },
            {
                "name": "typescript-npm-public-gateway-smoke",
                "argv": ["npm", "run", "gateway-smoke"],
                "cwd": "clients/typescript",
            },
            {
                "name": "python-sdk-unit-tests",
                "argv": ["python3", "-m", "unittest", "discover", "-s", "clients/python/tests"],
            },
            {
                "name": "python-sdk-install-smoke",
                "argv": ["python3", "clients/python/install_smoke.py"],
            },
            {
                "name": "python-platform-conformance-tests",
                "argv": ["python3", "-m", "unittest", "benchmarks.realworld.tests.test_platform_conformance"],
            },
            {
                "name": "python-sdk-conformance",
                "argv": [
                    "python3",
                    "scripts/platform_conformance.py",
                    "--surface",
                    "python_sdk",
                    "--summary-json",
                    "/tmp/tracedb-python-sdk-conformance.json",
                ],
            },
            {
                "name": "tracedb-cli-demo-tests",
                "argv": [
                    "cargo",
                    "test",
                    "-p",
                    "tracedb-cli",
                    "--test",
                    "demo",
                    "--",
                    "--nocapture",
                ],
            },
            {
                "name": "tracedb-testkit-usability-tests",
                "argv": [
                    "cargo",
                    "test",
                    "-p",
                    "tracedb-testkit",
                    "--test",
                    "usability_acceptance",
                    "--",
                    "--nocapture",
                ],
            },
            {
                "name": "rust-sdk-routing-tests",
                "argv": [
                    "cargo",
                    "test",
                    "-p",
                    "tracedb-sdk",
                    "--test",
                    "http_client",
                    "managed_client_defaults_branch_id_from_database_id_into_json_posts",
                    "--",
                    "--exact",
                    "--nocapture",
                ],
            },
            {
                "name": "query-field-rust-tests",
                "argv": [
                    "cargo",
                    "test",
                    "-p",
                    "tracedb-query",
                    "hybrid_query_",
                    "--",
                    "--nocapture",
                ],
            },
            {
                "name": "schema-validation-core-tests",
                "argv": [
                    "cargo",
                    "test",
                    "-p",
                    "tracedb-core",
                    "schema_validation_",
                    "--",
                    "--nocapture",
                ],
            },
            {
                "name": "schema-validation-acceptance-tests",
                "argv": [
                    "cargo",
                    "test",
                    "-p",
                    "tracedb-testkit",
                    "--test",
                    "acceptance",
                    "schema_validation_",
                    "--",
                    "--nocapture",
                ],
            },
            {
                "name": "workspace-all-targets",
                "argv": ["cargo", "test", "--workspace", "--all-targets"],
            },
        ], only)
    raise ValueError("mode must be quickstart or workspace")


def select_only_commands(commands: list[dict[str, Any]], only: str = "") -> list[dict[str, Any]]:
    only = only.strip()
    if not only:
        return commands
    requested = ONLY_COMMAND_ALIASES.get(only, [only])
    selected = [
        command
        for command in commands
        if command["name"] in requested
    ]
    found = {command["name"] for command in selected}
    missing = [name for name in requested if name not in found]
    if missing:
        raise ValueError(f"unknown --only command {only}: missing {', '.join(missing)}")
    return selected


def _tail(text: str, max_chars: int = 12_000) -> str:
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _command_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("CI", "1")
    env.setdefault("CARGO_TERM_COLOR", "never")
    env.setdefault("CARGO_INCREMENTAL", "0")
    env.setdefault("RUST_BACKTRACE", "1")
    return env


def run_command(command: dict[str, Any], cwd: Path) -> dict[str, Any]:
    command_cwd = cwd / command.get("cwd", "")
    started = time.monotonic()
    process = subprocess.run(
        command["argv"],
        cwd=command_cwd,
        env=_command_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    duration_s = round(time.monotonic() - started, 3)
    result = {
        "name": command["name"],
        "argv": command["argv"],
        "cwd": str(command_cwd),
        "ok": process.returncode == 0,
        "returncode": process.returncode,
        "duration_s": duration_s,
        "stdout_tail": _tail(process.stdout),
        "stderr_tail": _tail(process.stderr),
    }
    if command.get("capture_stdout"):
        result["stdout"] = process.stdout
    return result


def validate_reduced_quickstart_receipt(
    stdout_text: str,
    report_summary: dict[str, Any],
) -> dict[str, Any]:
    stdout_summary = json.loads(stdout_text)
    assert stdout_summary == report_summary, "stdout summary differs from report file"
    assert stdout_summary["ok"] is True, "product quickstart did not pass"
    assert stdout_summary["mode"] == "local-product-regression"
    assert stdout_summary["scope"] == "local_only"
    assert stdout_summary["typescript_enabled"] is False
    assert stdout_summary["claims"] == {
        "sql_module": "not_implemented",
        "managed_cloud": "not_checked",
        "benchmark": "not_checked",
    }
    assert stdout_summary["human_summary"]["status"] == "passed"
    assert stdout_summary["human_summary"]["steps_passed"] == len(EXPECTED_REDUCED_STEPS)
    assert stdout_summary["human_summary"]["steps_total"] == len(EXPECTED_REDUCED_STEPS)
    steps = stdout_summary["steps"]
    assert sorted(steps) == sorted(EXPECTED_REDUCED_STEPS), (
        f"expected reduced quickstart steps {EXPECTED_REDUCED_STEPS}, got {sorted(steps)}"
    )
    for step in EXPECTED_REDUCED_STEPS:
        assert steps[step]["ok"] is True, f"{step} did not pass"
    for step in SKIPPED_TYPESCRIPT_STEPS:
        assert step not in steps, f"{step} should be skipped in reduced quickstart mode"
    return {
        "ok": True,
        "mode": stdout_summary["mode"],
        "scope": stdout_summary["scope"],
        "report_file": stdout_summary["report_file"],
        "typescript_enabled": stdout_summary["typescript_enabled"],
        "steps_passed": stdout_summary["human_summary"]["steps_passed"],
        "steps_total": stdout_summary["human_summary"]["steps_total"],
        "skipped_typescript_steps": len(SKIPPED_TYPESCRIPT_STEPS),
        "claims": stdout_summary["claims"],
    }


def validate_agent_memory_flight_recorder_receipt(report: dict[str, Any]) -> dict[str, Any]:
    assert report.get("demo") == "local-chat-memory", "unexpected demo report kind"
    assert report.get("invariant_failures") == [], "chat demo invariants failed"
    receipt = report.get("flight_recorder_receipt")
    assert isinstance(receipt, dict), "missing flight_recorder_receipt"
    assert receipt.get("receipt_kind") == "agent_memory_flight_recorder"
    assert receipt.get("substrate") == "TraceDB"
    assert receipt.get("scope") == "local_product_demo"
    assert receipt.get("product_identity") == "AI-native transactional candidate-stream database"

    records = receipt.get("records", {})
    assert records.get("table") == "chat_memory"
    assert records.get("tenant") == "tenant-alpha"
    assert records.get("record_count") == 7
    assert "alpha-memory-1" in records.get("record_ids", [])

    retrieval = receipt.get("retrieval", {})
    assert retrieval.get("query_text") == "deterministic local memory hybrid"
    result_ids = retrieval.get("result_ids", [])
    assert "alpha-memory-1" in result_ids, "expected alpha-memory-1 in retrieval results"
    assert "beta-memory-1" not in result_ids, "Flight Recorder receipt leaked beta tenant result"

    provenance = receipt.get("provenance", {})
    assert provenance.get("deleted_subject_visible_after_delete") is False

    replay = receipt.get("replay", {})
    assert replay.get("commands_recorded", 0) >= 1
    assert replay.get("command_exit_failures") == []

    runtime = receipt.get("tracefield_runtime", {})
    assert runtime.get("status") == "not_implemented", "TraceField runtime must remain unimplemented"
    tensors = receipt.get("tensor_artifacts", {})
    assert tensors.get("status") == "future_module_layer"
    non_guarantees = receipt.get("non_guarantees", [])
    assert "no TraceField runtime behavior" in non_guarantees
    assert "no tensor artifact support" in non_guarantees

    return {
        "ok": True,
        "receipt_kind": receipt["receipt_kind"],
        "substrate": receipt["substrate"],
        "scope": receipt["scope"],
        "record_count": records["record_count"],
        "result_ids": result_ids,
        "commands_recorded": replay["commands_recorded"],
        "tracefield_runtime_status": runtime["status"],
        "tensor_artifacts_status": tensors["status"],
        "non_guarantees": non_guarantees,
    }


def _read_json_artifact(path_text: str, repo_root: Path) -> dict[str, Any]:
    path = Path(path_text)
    if not path.is_absolute():
        path = repo_root / path
    return json.loads(path.read_text())


def run_verification(
    mode: str,
    *,
    repo_root: Path,
    source_metadata: dict[str, Any] | None = None,
    only: str = "",
) -> dict[str, Any]:
    started = time.monotonic()
    commands = build_command_plan(mode, only=only)
    results: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "ok": False,
        "mode": mode,
        "only": only,
        "runner": "modal-product-verify",
        "repo_root": str(repo_root),
        "source": source_metadata or {},
        "commands": results,
    }

    try:
        for command in commands:
            result = run_command(command, repo_root)
            results.append(result)
            if not result["ok"]:
                summary["failed_command"] = result["name"]
                return _finish_summary(summary, started)
            if result["name"] == "agent-memory-flight-recorder":
                report = _read_json_artifact(command["receipt_json"], repo_root)
                summary["flight_recorder_receipt_check"] = (
                    validate_agent_memory_flight_recorder_receipt(report)
                )
            if result["name"] == "product-quickstart-skip-typescript":
                receipt_file = repo_root / RECEIPT_PATH
                report_summary = json.loads(receipt_file.read_text())
                summary["receipt_check"] = validate_reduced_quickstart_receipt(
                    result.get("stdout", ""),
                    report_summary,
                )
                result.pop("stdout", None)
        summary["ok"] = True
        return _finish_summary(summary, started)
    except Exception as error:  # pragma: no cover - exercised by remote failures.
        summary["error"] = f"{type(error).__name__}: {error}"
        return _finish_summary(summary, started)


def _finish_summary(summary: dict[str, Any], started: float) -> dict[str, Any]:
    summary["duration_s"] = round(time.monotonic() - started, 3)
    return summary


def _git_output(*args: str) -> str:
    process = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode != 0:
        raise RuntimeError(process.stderr.strip() or process.stdout.strip())
    return process.stdout.strip()


def source_git_metadata() -> dict[str, Any]:
    try:
        status = _git_output("status", "--short")
        return {
            "commit": _git_output("rev-parse", "HEAD"),
            "branch": _git_output("branch", "--show-current"),
            "dirty": bool(status),
            "status_short": status,
            "note": "local checkout uploaded with .git excluded",
        }
    except Exception as error:
        return {
            "error": f"{type(error).__name__}: {error}",
            "note": "local checkout uploaded with .git excluded",
        }


def write_summary_json(summary: dict[str, Any], output_path: str) -> None:
    if not output_path:
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def run_local(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run TraceDB product verification locally or describe the Modal ladder."
    )
    parser.add_argument("--mode", choices=["quickstart", "workspace"], default="quickstart")
    parser.add_argument("--only", default="", help="run one named command or supported alias")
    parser.add_argument("--summary-json", default="")
    parser.add_argument("--local", action="store_true", help="run the ladder on this machine")
    args = parser.parse_args(argv)

    if args.local:
        summary = run_verification(
            args.mode,
            repo_root=REPO_ROOT,
            source_metadata=source_git_metadata(),
            only=args.only,
        )
        write_summary_json(summary, args.summary_json)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["ok"] else 1

    plan = {
        "mode": args.mode,
        "only": args.only,
        "runner": "modal-product-verify",
        "modal_command": f"modal run scripts/modal_product_verify.py --mode {args.mode}"
        + (f" --only {args.only}" if args.only else ""),
        "commands": build_command_plan(args.mode, only=args.only),
        "upload_ignore": MODAL_IGNORE_PATTERNS,
    }
    write_summary_json(plan, args.summary_json)
    print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


if modal is not None:
    image = (
        modal.Image.from_registry(RUST_MODAL_IMAGE, add_python="3.12")
        .apt_install("build-essential", "ca-certificates", "curl", "pkg-config", "libssl-dev")
        .run_commands("rustup component add rustfmt")
        .run_commands(
            "curl -fsSL https://deb.nodesource.com/setup_24.x | bash - && "
            "apt-get install -y nodejs && "
            "node --version && npm --version"
        )
        .add_local_dir(
            str(REPO_ROOT),
            remote_path=REMOTE_REPO,
            ignore=MODAL_IGNORE_PATTERNS,
        )
    )
    app = modal.App(MODAL_APP_NAME)

    @app.function(
        image=image,
        cpu=DEFAULT_CPU,
        memory=DEFAULT_MEMORY_MB,
        timeout=DEFAULT_TIMEOUT_SECONDS,
        ephemeral_disk=MODAL_MIN_EPHEMERAL_DISK_MB,
    )
    def run_product_verify_remote(
        mode: str = "quickstart",
        only: str = "",
        source_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return run_verification(
            mode,
            repo_root=Path(REMOTE_REPO),
            source_metadata=source_metadata,
            only=only,
        )

    @app.local_entrypoint()
    def main(mode: str = "quickstart", summary_json: str = "", only: str = "") -> None:
        result = run_product_verify_remote.remote(
            mode=mode,
            only=only,
            source_metadata=source_git_metadata(),
        )
        write_summary_json(result, summary_json)
        print(json.dumps(result, indent=2, sort_keys=True))

else:
    app = None


if __name__ == "__main__":
    raise SystemExit(run_local())
