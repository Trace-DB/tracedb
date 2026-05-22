from __future__ import annotations

import argparse
import json
import os
import sys
from copy import copy
from pathlib import Path
from typing import Any

from .adapters import all_adapters
from .chat_demo import run_chat_demo
from .datasets import load_dataset
from .experiment import ExperimentRecorder, new_run_id, redact, service_environment
from .openrouter import (
    OpenRouterClient,
    OpenRouterError,
    config_from_args,
    maybe_apply_openrouter_embeddings,
)
from .report import build_report, write_json, write_markdown
from .scaling import run_inprocess_scaling_compare, run_tracedb_scaling
from .suite import build_suite_report, selected_scenarios, write_suite_json, write_suite_markdown
from .suite_spec import (
    build_suite_gate,
    default_suite_spec,
    load_suite_spec,
    select_suite_baseline_json,
    write_suite_gate_json,
)
from .types import RunConfig

try:
    from railway_bench import (
        build_railway_artifact_manifest,
        build_railway_backup_receipt,
        build_railway_backup_verdict,
        build_railway_manifest,
        build_railway_operation_receipt,
        build_railway_operation_plan,
        build_railway_operator_runbook,
        build_railway_persistence_verdict,
        build_railway_runbook_verification,
        railway_operator_runbook_markdown,
        railway_runbook_verification_markdown,
        load_railway_config,
        run_railway_endpoint_health,
        run_railway_snapshot_restore_check,
        run_railway_stateful_smoke,
        validate_railway_backup_receipt,
        validate_railway_operation_receipt,
    )
except ImportError:  # pragma: no cover - package import path used by unit discovery.
    from ..railway_bench import (
        build_railway_artifact_manifest,
        build_railway_backup_receipt,
        build_railway_backup_verdict,
        build_railway_manifest,
        build_railway_operation_receipt,
        build_railway_operation_plan,
        build_railway_operator_runbook,
        build_railway_persistence_verdict,
        build_railway_runbook_verification,
        railway_operator_runbook_markdown,
        railway_runbook_verification_markdown,
        load_railway_config,
        run_railway_endpoint_health,
        run_railway_snapshot_restore_check,
        run_railway_stateful_smoke,
        validate_railway_backup_receipt,
        validate_railway_operation_receipt,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m runner")
    subcommands = parser.add_subparsers(dest="command", required=True)

    run = subcommands.add_parser("run", help="run a real-world benchmark profile")
    add_benchmark_args(run)

    loop = subcommands.add_parser("loop", help="loop a benchmark and stop on falsification")
    add_benchmark_args(loop)
    loop.add_argument("--iterations", type=int, default=20)
    loop.add_argument("--stop-on-failure", action="store_true")

    doctor = subcommands.add_parser("doctor", help="inspect benchmark dependencies")
    doctor_subcommands = doctor.add_subparsers(dest="doctor_target", required=True)
    openrouter = doctor_subcommands.add_parser("openrouter", help="inspect OpenRouter config")
    add_openrouter_args(openrouter)

    suite = subcommands.add_parser("suite", help="run a scenario suite and aggregate reports")
    add_benchmark_args(suite)
    suite.set_defaults(records=None)
    suite.add_argument("--scenarios", default="all")
    suite.add_argument("--suite-spec", default="")
    suite.add_argument(
        "--suite-baseline-json",
        default="",
        help="Existing suite.json to compare for same-suite performance regressions.",
    )
    suite.add_argument(
        "--suite-baseline-dir",
        default="",
        help="Reports tree to scan for the latest compatible prior suite.json when no explicit baseline is provided.",
    )
    suite.add_argument(
        "--regression-tolerance-pct",
        type=float,
        default=15.0,
        help="Allowed relative TraceDB metric regression before recording a gate regression.",
    )
    suite.add_argument(
        "--regression-tolerance-absolute",
        type=float,
        default=0.0,
        help="Allowed absolute TraceDB metric regression before recording a gate regression.",
    )
    suite.add_argument(
        "--railway-config-from-env",
        action="store_true",
        help="Write railway-manifest.json from Railway env vars and feed it into suite-gate.json.",
    )
    suite.add_argument(
        "--railway-manifest-json",
        default="",
        help="Existing Railway manifest JSON to feed into suite-gate.json.",
    )
    suite.add_argument(
        "--railway-health-check",
        action="store_true",
        help="Probe the configured TraceDB Railway endpoint and record readiness in railway-manifest.json.",
    )
    suite.add_argument(
        "--railway-health-timeout-seconds",
        type=float,
        default=5.0,
        help="Per-request timeout for --railway-health-check.",
    )
    suite.add_argument(
        "--railway-stateful-smoke",
        action="store_true",
        help="Write and read a TraceDB marker record against the configured Railway endpoint.",
    )
    suite.add_argument(
        "--railway-stateful-smoke-timeout-seconds",
        type=float,
        default=5.0,
        help="Per-request timeout for --railway-stateful-smoke.",
    )
    suite.add_argument(
        "--railway-stateful-marker-id",
        default="",
        help="Specific Railway stateful marker id to write/read or read after a restart.",
    )
    suite.add_argument(
        "--railway-stateful-read-only",
        action="store_true",
        help="When used with --railway-stateful-smoke, read an existing marker without schema apply or put.",
    )
    suite.add_argument(
        "--railway-snapshot-restore-check",
        action="store_true",
        help="POST Railway TraceDB admin snapshot/restore routes using an explicit server-side scratch root.",
    )
    suite.add_argument(
        "--railway-snapshot-restore-timeout-seconds",
        type=float,
        default=60.0,
        help="Per-request timeout for --railway-snapshot-restore-check.",
    )
    suite.add_argument(
        "--railway-snapshot-root",
        default="",
        help="Override TRACEDB_RAILWAY_SNAPSHOT_ROOT for the server-side snapshot/restore scratch path.",
    )
    suite.add_argument(
        "--railway-verify-restored-marker",
        action="store_true",
        help="Ask /v1/admin/restore to read the stateful smoke marker from the restored target.",
    )
    suite.add_argument(
        "--railway-restart-redeploy-plan",
        action="store_true",
        help="Record a non-mutating Railway restart/redeploy readiness plan in railway-manifest.json.",
    )
    suite.add_argument(
        "--railway-persistence-pre-manifest-json",
        default="",
        help="Pre-operation railway-manifest.json with marker write/read evidence.",
    )
    suite.add_argument(
        "--railway-operation-receipt-json",
        default="",
        help="Operator-provided restart/redeploy receipt JSON for persistence verdict evaluation.",
    )
    suite.add_argument(
        "--railway-backup-receipt-json",
        default="",
        help="Operator-provided Railway backup/restore-validation receipt JSON for backup gates.",
    )
    suite.add_argument(
        "--railway-runbook-verification-json",
        default="",
        help="Existing railway-runbook-verify JSON artifact to feed into suite-gate.json.",
    )
    suite.add_argument(
        "--railway-require-runbook-verification",
        action="store_true",
        help="Block before child scenario execution unless runbook verification status is complete.",
    )
    suite.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Write suite/gate/Railway evidence artifacts without running child benchmark "
            "scenarios. Intended for cheap scheduled-lane receipt validation before heavy work."
        ),
    )

    railway_receipt = subcommands.add_parser(
        "railway-receipt",
        help="write a non-mutating operator receipt for a manual Railway restart/redeploy",
    )
    railway_receipt.add_argument("--operation", required=True, choices=["restart", "redeploy"])
    railway_receipt.add_argument(
        "--status",
        default="passed",
        choices=[
            "passed",
            "completed",
            "succeeded",
            "success",
            "ok",
            "blocked",
            "cancelled",
            "canceled",
            "error",
            "failed",
            "failure",
            "timeout",
            "timed_out",
        ],
    )
    railway_receipt.add_argument("--suite-id", default="")
    railway_receipt.add_argument("--output-json", default="reports/railway-operation-receipt.json")
    railway_receipt.add_argument(
        "--confirm-executed",
        action="store_true",
        help="Set executed=true and confirmed=true after the operator has manually run the operation.",
    )
    railway_receipt.add_argument("--operator", default="")
    railway_receipt.add_argument("--command", dest="operation_command", default="")
    railway_receipt.add_argument("--deployment-id", default="")
    railway_receipt.add_argument("--note", action="append", default=[])

    railway_backup_receipt = subcommands.add_parser(
        "railway-backup-receipt",
        help="write a non-mutating operator receipt for Railway backup/restore validation",
    )
    railway_backup_receipt.add_argument(
        "--status",
        default="passed",
        choices=[
            "passed",
            "completed",
            "succeeded",
            "success",
            "ok",
            "blocked",
            "cancelled",
            "canceled",
            "error",
            "failed",
            "failure",
            "timeout",
            "timed_out",
        ],
    )
    railway_backup_receipt.add_argument("--suite-id", default="")
    railway_backup_receipt.add_argument("--backup-id", required=True)
    railway_backup_receipt.add_argument("--output-json", default="reports/railway-backup-receipt.json")
    railway_backup_receipt.add_argument(
        "--confirm-created",
        action="store_true",
        help="Set backup_created=true and confirmed=true after checking the Railway backup.",
    )
    railway_backup_receipt.add_argument(
        "--restore-validated",
        action="store_true",
        help="Set restore_validated=true after validating a restored copy or restore drill.",
    )
    railway_backup_receipt.add_argument("--restore-validation-method", default="")
    railway_backup_receipt.add_argument("--operator", default="")
    railway_backup_receipt.add_argument("--note", action="append", default=[])

    railway_runbook = subcommands.add_parser(
        "railway-runbook",
        help="write a non-mutating Railway operator runbook for a suite spec",
    )
    railway_runbook.add_argument("--suite-spec", required=True)
    railway_runbook.add_argument("--suite-id", default="")
    railway_runbook.add_argument("--run-id", default="")
    railway_runbook.add_argument("--reports-dir", default="reports")
    railway_runbook.add_argument("--target", default="tracedb")
    railway_runbook.add_argument("--surface", default="sdk")
    railway_runbook.add_argument("--scenarios", default="sdk_cli_surface")
    railway_runbook.add_argument("--backup-receipt-json", default="")
    railway_runbook.add_argument("--operation-receipt-json", default="")
    railway_runbook.add_argument("--pre-manifest-json", default="")
    railway_runbook.add_argument("--marker-id", default="")
    railway_runbook.add_argument("--operation", choices=["restart", "redeploy"], default="restart")
    railway_runbook.add_argument("--runbook-verification-json", default="")
    railway_runbook.add_argument("--runbook-verification-md", default="")
    railway_runbook.add_argument("--suite-baseline-dir", default="")
    railway_runbook.add_argument("--output-json", default="reports/railway-runbook.json")
    railway_runbook.add_argument("--output-md", default="reports/railway-runbook.md")

    railway_runbook_verify = subcommands.add_parser(
        "railway-runbook-verify",
        help="verify existing Railway runbook evidence artifacts without mutating Railway",
    )
    railway_runbook_verify.add_argument("--runbook-json", required=True)
    railway_runbook_verify.add_argument(
        "--output-json",
        default="reports/railway-runbook-verification.json",
    )
    railway_runbook_verify.add_argument(
        "--output-md",
        default="reports/railway-runbook-verification.md",
    )
    railway_runbook_verify.add_argument(
        "--max-age-seconds",
        type=float,
        default=0.0,
        help="Mark artifacts older than this many seconds stale; 0 disables age checks.",
    )

    chat_demo = subcommands.add_parser("chat-demo", help="run the local chat-memory demo")
    chat_demo.add_argument("--data-dir", default="")
    chat_demo.add_argument("--tracedb-cli", default="")
    chat_demo.add_argument("--output-json", default="reports/chat-demo/latest.json")
    chat_demo.add_argument("--output-md", default="reports/chat-demo/latest.md")

    scaling = subcommands.add_parser("tracedb-scaling", help="measure local TraceDB CLI open/recovery scaling")
    scaling.add_argument("--data-dir", default="")
    scaling.add_argument("--tracedb-cli", default="")
    scaling.add_argument("--records", default="128,512,1024")
    scaling.add_argument("--inspect-repetitions", type=int, default=5)
    scaling.add_argument("--query-repetitions", type=int, default=3)
    scaling.add_argument("--checkpoint-at-points", action="store_true")
    scaling.add_argument("--output-json", default="reports/scaling/latest.json")
    scaling.add_argument("--output-md", default="reports/scaling/latest.md")

    scaling_compare = subcommands.add_parser(
        "tracedb-scaling-compare",
        help="compare TraceDB in-process scaling reports against a parent baseline",
    )
    scaling_compare.add_argument("--baseline-json", nargs="+", required=True)
    scaling_compare.add_argument("--candidate-json", nargs="+", required=True)
    scaling_compare.add_argument("--baseline-label", default="baseline")
    scaling_compare.add_argument("--candidate-label", default="candidate")
    scaling_compare.add_argument("--min-repeats", type=int, default=2)
    scaling_compare.add_argument("--required-write-improvement-pct", type=float, default=25.0)
    scaling_compare.add_argument("--allowed-query-regression-pct", type=float, default=10.0)
    scaling_compare.add_argument("--allowed-query-regression-ms", type=float, default=5.0)
    scaling_compare.add_argument("--output-json", default="reports/scaling/comparison.json")
    scaling_compare.add_argument("--output-md", default="reports/scaling/comparison.md")

    args = parser.parse_args(argv)
    if args.command == "run":
        return run_benchmark(args)
    if args.command == "loop":
        return run_loop(args)
    if args.command == "doctor" and args.doctor_target == "openrouter":
        return doctor_openrouter(args)
    if args.command == "suite":
        return run_suite(args)
    if args.command == "railway-receipt":
        return run_railway_receipt(args)
    if args.command == "railway-backup-receipt":
        return run_railway_backup_receipt(args)
    if args.command == "railway-runbook":
        return run_railway_runbook(args)
    if args.command == "railway-runbook-verify":
        return run_railway_runbook_verify(args)
    if args.command == "chat-demo":
        return run_chat_demo(args)
    if args.command == "tracedb-scaling":
        return run_tracedb_scaling(args)
    if args.command == "tracedb-scaling-compare":
        return run_inprocess_scaling_compare(args)
    parser.error(f"unknown command {args.command}")
    return 2


def add_benchmark_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", default="smoke", choices=["smoke", "local", "ci"])
    parser.add_argument(
        "--dataset",
        default="generated",
        choices=[
            "generated",
            "generated_hybrid",
            "embedded_movies",
            "beir_scifact",
            "scifact",
            "codesearchnet",
            "code_search_net",
            "codesearchnet_body",
            "codesearchnet_codeaware",
        ],
    )
    parser.add_argument("--records", type=int, default=1000)
    parser.add_argument("--target", default="all")
    parser.add_argument("--surface", default="sdk,cli,http,curl")
    parser.add_argument("--output-json", default="reports/latest.json")
    parser.add_argument("--output-md", default="reports/latest.md")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--require-services", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-id", default="")
    parser.add_argument(
        "--tracedb-ingest-mode",
        default="per_record",
        choices=["per_record", "batch"],
        help="TraceDB HTTP ingest lane: per_record keeps one durable write per record; batch uses one atomic batch transaction.",
    )
    add_openrouter_args(parser)


def add_openrouter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--openrouter-mode", default="auto", choices=["auto", "off", "required"])
    parser.add_argument(
        "--openrouter-cap",
        default="moderate",
        choices=["conservative", "moderate", "aggressive"],
    )
    parser.add_argument("--embed-model", default=None)
    parser.add_argument("--compare-embed-models", default=None)
    parser.add_argument("--judge-model", default=None)
    parser.add_argument("--rerank-model", default=None)
    parser.add_argument(
        "--embedding-dimensions",
        default=None,
        help="Cap provider embeddings to this many dimensions; use 0/native/auto for provider-native size.",
    )


def run_benchmark(args: argparse.Namespace) -> int:
    try:
        report, exit_code = execute_benchmark(args)
    except OpenRouterError as error:
        print(str(error), file=sys.stderr)
        return 1
    if args.require_services and report["summary"]["unavailable_count"]:
        print("one or more required services were unavailable", file=sys.stderr)
        return 1
    return exit_code


def run_railway_receipt(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    config = load_railway_config()
    receipt = build_railway_operation_receipt(
        config,
        suite_id=args.suite_id,
        operation=args.operation,
        status=args.status,
        executed=args.confirm_executed,
        confirmed=args.confirm_executed,
        command=args.operation_command,
        operator=args.operator,
        deployment_id=args.deployment_id,
        notes=args.note,
    )
    validation = validate_railway_operation_receipt(
        receipt,
        expected_service_id=str(config.get("tracedb_service_id", "")),
    )
    output_path = _resolve_path(lab_root, args.output_json)
    write_json(receipt, output_path)
    print(f"wrote {output_path}")
    if not validation["ok"]:
        for error in validation["errors"]:
            print(error, file=sys.stderr)
        for missing in validation["missing"]:
            print(f"missing receipt field: {missing}", file=sys.stderr)
        return 1
    return 0


def run_railway_backup_receipt(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    config = load_railway_config()
    receipt = build_railway_backup_receipt(
        config,
        suite_id=args.suite_id,
        status=args.status,
        backup_id=args.backup_id,
        confirmed=args.confirm_created,
        backup_created=args.confirm_created,
        restore_validated=args.restore_validated,
        restore_validation_method=args.restore_validation_method,
        operator=args.operator,
        notes=args.note,
    )
    validation = validate_railway_backup_receipt(
        receipt,
        expected_service_id=str(config.get("tracedb_service_id", "")),
    )
    output_path = _resolve_path(lab_root, args.output_json)
    write_json(receipt, output_path)
    print(f"wrote {output_path}")
    if not validation["ok"]:
        for error in validation["errors"]:
            print(error, file=sys.stderr)
        for missing in validation["missing"]:
            print(f"missing backup receipt field: {missing}", file=sys.stderr)
        return 1
    return 0


def run_railway_runbook(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    suite_spec_path = _resolve_suite_spec_path(lab_root, args.suite_spec)
    suite_spec = load_suite_spec(suite_spec_path)
    suite_id = args.suite_id or args.run_id or f"{suite_spec.id}-railway-runbook"
    config = load_railway_config()
    runbook = build_railway_operator_runbook(
        config,
        suite_id=suite_id,
        suite_spec_id=suite_spec.id,
        suite_spec_path=args.suite_spec,
        reports_dir=args.reports_dir,
        railway=suite_spec.railway,
        target=args.target,
        surface=args.surface,
        scenarios=args.scenarios,
        backup_receipt_json=args.backup_receipt_json,
        operation_receipt_json=args.operation_receipt_json,
        pre_manifest_json=args.pre_manifest_json,
        marker_id=args.marker_id,
        operation=args.operation,
        runbook_json=args.output_json,
        runbook_verification_json=args.runbook_verification_json,
        runbook_verification_md=args.runbook_verification_md,
        suite_baseline_dir=args.suite_baseline_dir,
    )
    output_json = _resolve_path(lab_root, args.output_json)
    output_md = _resolve_path(lab_root, args.output_md)
    write_json(runbook, output_json)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(railway_operator_runbook_markdown(runbook), encoding="utf-8")
    print(f"wrote {output_json}")
    print(f"wrote {output_md}")
    return 0 if runbook["status"] == "ready" else 1


def run_railway_runbook_verify(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    runbook_path = _resolve_path(lab_root, args.runbook_json)
    payload = json.loads(runbook_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("--runbook-json must contain a JSON object")
    max_age_seconds = args.max_age_seconds if args.max_age_seconds > 0 else None
    verification = build_railway_runbook_verification(
        payload,
        root=lab_root,
        max_age_seconds=max_age_seconds,
    )
    output_json = _resolve_path(lab_root, args.output_json)
    output_md = _resolve_path(lab_root, args.output_md)
    write_json(verification, output_json)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(railway_runbook_verification_markdown(verification), encoding="utf-8")
    print(f"wrote {output_json}")
    print(f"wrote {output_md}")
    return 0 if verification["status"] == "complete" else 1


def execute_benchmark(
    args: argparse.Namespace,
    *,
    seed: int | None = None,
    run_id: str | None = None,
) -> tuple[dict[str, Any], int]:
    lab_root = Path.cwd()
    repo_root = lab_root.parent.parent
    reports_dir = _resolve_path(lab_root, args.reports_dir)
    selected_run_id = run_id or args.run_id or new_run_id("bench")
    recorder = ExperimentRecorder(selected_run_id, reports_dir)

    targets = parse_csv(args.target)
    surfaces = parse_csv(args.surface)
    selected_seed = args.seed if seed is None else seed
    openrouter_config = config_from_args(args, lab_root)
    dataset = load_dataset(args.dataset, args.records, selected_seed)
    dataset, openrouter_stats = maybe_apply_openrouter_embeddings(
        dataset, openrouter_config, lab_root, recorder
    )
    config = RunConfig(
        profile=args.profile,
        target=targets,
        surfaces=surfaces,
        require_services=args.require_services,
        repo_root=str(repo_root),
        openrouter_mode=openrouter_config.mode,
        openrouter_cap=openrouter_config.cap_name,
        run_id=selected_run_id,
        reports_dir=str(reports_dir),
        observer=recorder,
        tracedb_ingest_mode=args.tracedb_ingest_mode,
    )
    manifest = build_manifest(
        args=args,
        config=config,
        dataset=dataset,
        openrouter_config=openrouter_config.public_summary(),
        openrouter_stats=openrouter_stats,
        seed=selected_seed,
        repo_root=repo_root,
    )
    recorder.write_manifest(manifest)
    recorder.observe(
        "benchmark.start",
        {
            "targets": targets,
            "surfaces": surfaces,
            "record_count": len(dataset.records),
            "query_count": len(dataset.queries),
        },
    )

    selected = [adapter for adapter in all_adapters() if "all" in targets or adapter.name in targets]
    if not selected:
        raise SystemExit(f"no adapters selected for target={args.target}")

    baselines = []
    for adapter in selected:
        recorder.observe("adapter.start", {"name": adapter.name})
        try:
            result = adapter.run(dataset, config)
        except Exception as error:
            if args.require_services:
                raise
            result = adapter.unavailable(f"{adapter.name} failed: {error}", dataset)
        baselines.append(result)
        recorder.observe(
            "adapter.complete",
            {
                "name": adapter.name,
                "available": result["available"],
                "metrics": result["metrics"],
                "notes": result["notes"],
            },
        )

    report = build_report(dataset, config, baselines, openrouter_stats)
    failures = collect_failures(report)
    recorder.write_failures(failures)
    write_json(report, recorder.run_dir / "summary.json")
    write_markdown(report, recorder.run_dir / "report.md")
    write_json(report, _resolve_path(lab_root, args.output_json))
    write_markdown(report, _resolve_path(lab_root, args.output_md))
    recorder.observe(
        "benchmark.complete",
        {
            "summary": report["summary"],
            "failure_count": len(failures),
            "artifact_dir": str(recorder.run_dir),
        },
    )
    print(f"wrote {recorder.run_dir / 'summary.json'}")
    print(f"wrote {recorder.run_dir / 'report.md'}")
    print(f"wrote {args.output_json}")
    print(f"wrote {args.output_md}")
    return report, 0


def run_loop(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    reports_dir = _resolve_path(lab_root, args.reports_dir)
    base_run_id = args.run_id or new_run_id("loop")
    recorder = ExperimentRecorder(base_run_id, reports_dir)
    injection = os.environ.get("BENCH_INJECT_FAILURE_ITERATION")
    recorder.write_manifest(
        {
            "run_id": base_run_id,
            "kind": "loop",
            "profile": args.profile,
            "dataset": args.dataset,
            "records": args.records,
            "iterations": args.iterations,
            "seed_start": args.seed,
            "openrouter_mode": args.openrouter_mode,
            "openrouter_cap": args.openrouter_cap,
            "service_environment": service_environment(),
        }
    )
    loop_summary: list[dict[str, Any]] = []
    for iteration in range(1, args.iterations + 1):
        seed = args.seed + iteration - 1
        if injection and int(injection) == iteration:
            reason = f"injected failure at iteration {iteration}"
            recorder.write_failure_case(iteration, seed, reason)
            (recorder.run_dir / "summary.json").write_text(
                json.dumps(
                    {"run_id": base_run_id, "status": "failed", "iterations": loop_summary},
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            if args.stop_on_failure:
                return 1
            continue

        child_args = copy(args)
        child_args.run_id = f"{base_run_id}-iteration-{iteration}"
        try:
            report, exit_code = execute_benchmark(child_args, seed=seed, run_id=child_args.run_id)
        except OpenRouterError as error:
            recorder.write_failure_case(iteration, seed, str(error))
            if args.stop_on_failure:
                return 1
            continue
        summary = {
            "iteration": iteration,
            "seed": seed,
            "run_id": child_args.run_id,
            "exit_code": exit_code,
            "summary": report["summary"],
        }
        loop_summary.append(summary)
        failed = exit_code != 0 or report["summary"]["failure_count"] > 0
        if failed:
            recorder.write_failure_case(iteration, seed, f"benchmark failure in {child_args.run_id}")
            if args.stop_on_failure:
                return 1

    (recorder.run_dir / "summary.json").write_text(
        json.dumps(
            {"run_id": base_run_id, "status": "passed", "iterations": loop_summary},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    recorder.write_failures([])
    print(f"wrote {recorder.run_dir / 'summary.json'}")
    return 0


def run_suite(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    reports_dir = _resolve_path(lab_root, args.reports_dir)
    suite_id = args.run_id or new_run_id("suite")
    suite_dir = reports_dir / suite_id
    suite_dir.mkdir(parents=True, exist_ok=True)
    explicit_suite_spec = (
        load_suite_spec(_resolve_suite_spec_path(lab_root, args.suite_spec))
        if args.suite_spec
        else None
    )
    if args.records is None:
        args.records = explicit_suite_spec.default_records if explicit_suite_spec else 1000
    scenarios_value = args.scenarios
    if explicit_suite_spec is not None and scenarios_value == "all":
        scenarios_value = ",".join(explicit_suite_spec.scenarios)
    specs = selected_scenarios(scenarios_value)
    gate_spec = explicit_suite_spec or default_suite_spec(
        scenarios=[spec.scenario_id for spec in specs],
        surfaces=parse_csv(args.surface),
        controls=parse_csv(args.target),
        records=args.records,
    )
    runbook_verification = _load_or_write_railway_runbook_verification(args, lab_root, suite_dir)
    pre_execution_blocked = _railway_runbook_verification_blocks_execution(
        args,
        runbook_verification,
    )
    child_reports = []
    exit_code = 0
    if not args.preflight_only and not pre_execution_blocked:
        for spec in specs:
            child_args = copy(args)
            child_args.run_id = f"{suite_id}-{spec.scenario_id}"
            child_args.target = spec.target if args.target == "all" else args.target
            child_args.surface = spec.surface if args.surface == "sdk,cli,http,curl" else args.surface
            child_args.output_json = str(suite_dir / f"{spec.scenario_id}.json")
            child_args.output_md = str(suite_dir / f"{spec.scenario_id}.md")
            try:
                report, child_exit = execute_benchmark(child_args, run_id=child_args.run_id)
            except OpenRouterError as error:
                print(str(error), file=sys.stderr)
                return 1
            exit_code = max(exit_code, child_exit)
            if args.require_services and report["summary"]["unavailable_count"]:
                exit_code = 1
            child_reports.append(
                {
                    "spec": spec,
                    "report": report,
                    "artifact_dir": str(reports_dir / child_args.run_id),
                }
            )

    suite_report = build_suite_report(
        suite_id=suite_id,
        profile=args.profile,
        dataset=args.dataset,
        records=args.records,
        reports=child_reports,
    )
    suite_report["suite_spec"] = gate_spec.id
    if args.preflight_only:
        suite_report["preflight"] = {
            "status": "metadata_only",
            "scenario_execution": "skipped",
            "selected_scenarios": [spec.scenario_id for spec in specs],
        }
    elif pre_execution_blocked:
        suite_report["preflight"] = {
            "status": "blocked",
            "scenario_execution": "skipped",
            "selected_scenarios": [spec.scenario_id for spec in specs],
            "blocking_reason": "railway_runbook_verification",
        }
    return _write_suite_outputs(
        args=args,
        lab_root=lab_root,
        suite_dir=suite_dir,
        suite_id=suite_id,
        suite_report=suite_report,
        gate_spec=gate_spec,
        exit_code=exit_code,
        railway_runbook_verification=runbook_verification,
    )


def _write_suite_outputs(
    *,
    args: argparse.Namespace,
    lab_root: Path,
    suite_dir: Path,
    suite_id: str,
    suite_report: dict[str, Any],
    gate_spec: Any,
    exit_code: int,
    railway_runbook_verification: dict[str, Any] | None = None,
) -> int:
    railway_manifest = _load_or_write_railway_manifest(args, lab_root, suite_dir, suite_id)
    artifact_paths = {
        "suite_json": "suite.json",
        "suite_md": "suite.md",
        "suite_gate_json": "suite-gate.json",
    }
    if railway_manifest is not None:
        artifact_paths["railway_manifest_json"] = "railway-manifest.json"
        artifact_paths["railway_artifacts_json"] = "railway-artifacts.json"
    if railway_runbook_verification is not None:
        artifact_paths["railway_runbook_verification_json"] = "railway-runbook-verification.json"
    regression_baseline, baseline_selection = _load_suite_baseline(
        args,
        lab_root,
        suite_id=suite_id,
        suite_spec_id=str(gate_spec.id),
        dataset=str(suite_report.get("dataset", "")),
        records=int(suite_report.get("records", 0)),
    )
    if regression_baseline is not None:
        artifact_paths["suite_baseline_json"] = str(baseline_selection["path"])
        artifact_paths["suite_baseline_source"] = str(baseline_selection["source"])
        if baseline_selection.get("suite_id"):
            artifact_paths["suite_baseline_suite_id"] = str(baseline_selection["suite_id"])
    elif getattr(args, "suite_baseline_dir", ""):
        artifact_paths["suite_baseline_dir"] = args.suite_baseline_dir
        artifact_paths["suite_baseline_source"] = "auto_latest_not_found"
    write_suite_json(suite_report, suite_dir / "suite.json")
    write_suite_markdown(suite_report, suite_dir / "suite.md")
    suite_gate = build_suite_gate(
        suite_report,
        gate_spec,
        artifact_paths=artifact_paths,
        railway_manifest=railway_manifest,
        railway_runbook_verification=railway_runbook_verification,
        railway_runbook_verification_required=args.railway_require_runbook_verification,
        regression_baseline=regression_baseline,
        regression_tolerance_pct=args.regression_tolerance_pct,
        regression_tolerance_absolute=args.regression_tolerance_absolute,
    )
    write_suite_gate_json(suite_gate, suite_dir / "suite-gate.json")
    if railway_manifest is not None:
        railway_artifacts = build_railway_artifact_manifest(
            suite_dir,
            suite_id=suite_id,
            artifact_paths=suite_gate["artifact_paths"],
            railway_manifest=railway_manifest,
            suite_gate=suite_gate,
        )
        write_json(railway_artifacts, suite_dir / "railway-artifacts.json")
    if suite_gate["blocking_failures"]:
        exit_code = 1
    print(f"wrote {suite_dir / 'suite.json'}")
    print(f"wrote {suite_dir / 'suite.md'}")
    print(f"wrote {suite_dir / 'suite-gate.json'}")
    return exit_code


def doctor_openrouter(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    try:
        config = config_from_args(args, lab_root)
        if config.mode == "off":
            print(
                json.dumps(
                    {
                        "status": "disabled",
                        "openrouter_mode": "off",
                        "reason": "disabled by --openrouter-mode off",
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if not config.api_key:
            payload = {
                "status": "missing_key",
                "openrouter_mode": config.mode,
                "reason": "OPENROUTER_API_KEY is not configured",
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
            if config.mode == "required":
                print(payload["reason"], file=sys.stderr)
                return 1
            return 0
        client = OpenRouterClient(config, lab_root / ".cache" / "openrouter")
        key_info = client.key_info()
        models = client.list_embedding_models()
        payload = {
            "status": "ok",
            "openrouter_mode": config.mode,
            "base_url": config.base_url,
            "embed_model": config.embed_model,
            "compare_embed_models": config.compare_embed_models,
            "judge_model": config.judge_model,
            "rerank_model": config.rerank_model,
            "embedding_dimensions": config.embedding_dimensions,
            "key": redact(key_info),
            "embedding_model_count": len(models),
            "stats": client.stats,
        }
        print(json.dumps(redact(payload), indent=2, sort_keys=True))
        return 0
    except OpenRouterError as error:
        print(str(error), file=sys.stderr)
        return 1


def build_manifest(
    *,
    args: argparse.Namespace,
    config: RunConfig,
    dataset: Any,
    openrouter_config: dict[str, Any],
    openrouter_stats: dict[str, Any],
    seed: int,
    repo_root: Path,
) -> dict[str, Any]:
    return {
        "run_id": config.run_id,
        "hypothesis": "TraceDB can satisfy real-world database and AI retrieval workloads with observable, falsifiable behavior.",
        "profile": config.profile,
        "seed": seed,
        "dataset": {
            "kind": dataset.kind,
            "source": dataset.source,
            "digest": dataset.digest,
            "record_count": len(dataset.records),
            "query_count": len(dataset.queries),
            "embedding_model": dataset.embedding_model,
            "embedding_dimensions": dataset.embedding_dimensions,
            "embedding_source": dataset.embedding_source,
            "relevance_label_mode": dataset.relevance_label_mode,
            "relevance_label_scope": dataset.relevance_label_scope,
        },
        "targets": config.target,
        "surfaces": config.surfaces,
        "tracedb_ingest_mode": config.tracedb_ingest_mode,
        "openrouter": {
            **openrouter_config,
            "stats": openrouter_stats,
        },
        "adapter_versions": {
            "runner": "realworld-v1",
            "tracedb": "workspace-debug",
        },
        "service_environment": service_environment(),
        "repo_root": str(repo_root),
        "output_json": args.output_json,
        "output_md": args.output_md,
    }


def collect_failures(report: dict[str, Any]) -> list[str]:
    failures = []
    for baseline in report["baselines"]:
        metrics = baseline.get("metrics", {})
        if not baseline.get("available", False):
            failures.append(f"{baseline['name']} unavailable: {'; '.join(baseline['notes'])}")
        if int(metrics.get("failure_count", 0)) > 0:
            failures.append(f"{baseline['name']} reported failure_count={metrics['failure_count']}")
    return failures


def parse_csv(value: str) -> list[str]:
    out = [item.strip() for item in value.split(",") if item.strip()]
    return out or ["all"]


def _resolve_path(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _resolve_suite_spec_path(root: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    lab_candidate = root / path
    if lab_candidate.exists():
        return lab_candidate
    repo_candidate = root.parent.parent / path
    return repo_candidate if repo_candidate.exists() else lab_candidate


def _load_or_write_railway_manifest(
    args: argparse.Namespace,
    lab_root: Path,
    suite_dir: Path,
    suite_id: str,
) -> dict[str, Any] | None:
    if args.railway_config_from_env:
        config = load_railway_config()
        if args.railway_snapshot_root:
            config["tracedb_snapshot_root"] = args.railway_snapshot_root
        endpoint_health = (
            run_railway_endpoint_health(
                config,
                timeout_seconds=args.railway_health_timeout_seconds,
                bearer_token=os.environ.get("TRACEDB_HTTP_BEARER_TOKEN") or None,
            )
            if args.railway_health_check
            else None
        )
        stateful_smoke = (
            run_railway_stateful_smoke(
                config,
                timeout_seconds=args.railway_stateful_smoke_timeout_seconds,
                bearer_token=os.environ.get("TRACEDB_HTTP_BEARER_TOKEN") or None,
                run_id=suite_id,
                marker_id=args.railway_stateful_marker_id or None,
                write_marker=not args.railway_stateful_read_only,
            )
            if args.railway_stateful_smoke
            else None
        )
        snapshot_marker_id = None
        if isinstance(stateful_smoke, dict):
            snapshot_marker = stateful_smoke.get("marker")
            if isinstance(snapshot_marker, dict):
                snapshot_marker_id = str(snapshot_marker.get("id") or "") or None
        snapshot_restore = (
            run_railway_snapshot_restore_check(
                config,
                timeout_seconds=args.railway_snapshot_restore_timeout_seconds,
                bearer_token=os.environ.get("TRACEDB_HTTP_BEARER_TOKEN") or None,
                run_id=suite_id,
                marker_id=snapshot_marker_id or args.railway_stateful_marker_id or None,
                snapshot_root=args.railway_snapshot_root or None,
                verify_restored_marker=args.railway_verify_restored_marker,
            )
            if args.railway_snapshot_restore_check
            else None
        )
        operation_plan = (
            build_railway_operation_plan(config, suite_id=suite_id)
            if args.railway_restart_redeploy_plan
            else None
        )
        manifest = build_railway_manifest(
            config,
            suite_id=suite_id,
            endpoint_health=endpoint_health,
            stateful_smoke=stateful_smoke,
            snapshot_restore=snapshot_restore,
            operation_plan=operation_plan,
        )
        if args.railway_persistence_pre_manifest_json or args.railway_operation_receipt_json:
            pre_manifest = _load_json_if_configured(
                lab_root, args.railway_persistence_pre_manifest_json
            )
            operation_receipt = _load_json_if_configured(
                lab_root, args.railway_operation_receipt_json
            )
            manifest["persistence_verdict"] = build_railway_persistence_verdict(
                pre_manifest,
                manifest,
                operation_receipt,
            )
        if args.railway_backup_receipt_json:
            backup_receipt = _load_json_if_configured(lab_root, args.railway_backup_receipt_json)
            manifest["backup_verdict"] = build_railway_backup_verdict(
                manifest,
                backup_receipt,
            )
        _write_railway_manifest(manifest, suite_dir / "railway-manifest.json")
        return manifest
    if args.railway_manifest_json:
        manifest_path = _resolve_path(lab_root, args.railway_manifest_json)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        _write_railway_manifest(manifest, suite_dir / "railway-manifest.json")
        return manifest
    return None


def _write_railway_manifest(manifest: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_or_write_railway_runbook_verification(
    args: argparse.Namespace,
    lab_root: Path,
    suite_dir: Path,
) -> dict[str, Any] | None:
    if not args.railway_runbook_verification_json:
        return None
    path = _resolve_path(lab_root, args.railway_runbook_verification_json)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("--railway-runbook-verification-json must contain a JSON object")
    write_json(payload, suite_dir / "railway-runbook-verification.json")
    return payload


def _load_suite_baseline(
    args: argparse.Namespace,
    lab_root: Path,
    *,
    suite_id: str,
    suite_spec_id: str,
    dataset: str,
    records: int,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not args.suite_baseline_json and not getattr(args, "suite_baseline_dir", ""):
        return None, None
    if args.suite_baseline_json:
        path = _resolve_path(lab_root, args.suite_baseline_json)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("--suite-baseline-json must contain a JSON object")
        return payload, {
            "source": "explicit",
            "path": str(path),
            "suite_id": str(payload.get("suite_id", "")),
            "suite_spec": str(payload.get("suite_spec", "")),
            "dataset": str(payload.get("dataset", "")),
            "records": payload.get("records"),
        }
    baseline_dir = _resolve_path(lab_root, args.suite_baseline_dir)
    selection = select_suite_baseline_json(
        baseline_dir,
        suite_id=suite_id,
        suite_spec_id=suite_spec_id,
        dataset=dataset,
        records=records,
    )
    if selection is None:
        return None, None
    path = Path(selection["path"])
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("--suite-baseline-dir selected a non-object suite.json")
    return payload, selection


def _railway_runbook_verification_blocks_execution(
    args: argparse.Namespace,
    verification: dict[str, Any] | None,
) -> bool:
    status = str((verification or {}).get("status") or "not_checked")
    if status not in {"not_checked", "complete"}:
        return True
    return bool(args.railway_require_runbook_verification and status != "complete")


def _load_json_if_configured(root: Path, value: str) -> dict[str, Any]:
    if not value:
        return {}
    path = _resolve_path(root, value)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}
