from __future__ import annotations

import argparse
import json
import os
import subprocess
import tarfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import modal
except ImportError:  # pragma: no cover - local tests should not require Modal.
    modal = None


LAB_ROOT = Path(__file__).resolve().parent
REPO_ROOT = LAB_ROOT.parent.parent
REMOTE_REPO = "/workspace/TraceDB"
DEFAULT_REPORTS_DIR = "/tmp/tracedb-modal-reports"
DEFAULT_BUNDLE_DIR = "/tmp/tracedb-modal-bundles"
DEFAULT_MAX_RECORDS = 1000
MODAL_MIN_EPHEMERAL_DISK_MB = 524_288
MODAL_IGNORE_PATTERNS = [
    "target/**",
    ".git/**",
    ".env",
    ".env.local",
    "benchmarks/realworld/.env.local",
    "benchmarks/realworld/.cache/**",
    "benchmarks/realworld/.venv/**",
    "benchmarks/realworld/run-data/**",
    "benchmarks/realworld/reports/**",
    "benchmarks/realworld/report-bundles/**",
]


@dataclass(frozen=True)
class ModalSmokeConfig:
    run_id: str = "modal-smoke"
    profile: str = "smoke"
    dataset: str = "generated"
    records: int = 128
    target: str = "tracedb"
    surface: str = "sdk"
    scenarios: str = "sdk_cli_surface"
    openrouter_mode: str = "off"
    openrouter_cap: str = "moderate"
    embedding_dimensions: str | None = None
    reports_dir: str = DEFAULT_REPORTS_DIR
    bundle_dir: str = DEFAULT_BUNDLE_DIR
    min_free_mb: int = 20_000
    cpu: float = 4.0
    memory_mb: int = 16_384
    timeout_seconds: int = 3_600
    ephemeral_disk_mb: int = MODAL_MIN_EPHEMERAL_DISK_MB
    gpu_requested: bool = False
    allow_gpu: bool = False
    allow_large: bool = False
    allow_external_controls: bool = False
    allow_provider: bool = False


def validate_config(config: ModalSmokeConfig) -> None:
    if config.records > DEFAULT_MAX_RECORDS and not config.allow_large:
        raise ValueError(
            f"records={config.records} exceeds safe default {DEFAULT_MAX_RECORDS}; set allow_large explicitly"
        )
    if config.gpu_requested and not config.allow_gpu:
        raise ValueError("GPU use requires allow_gpu=True")
    if config.openrouter_mode == "required" and not config.allow_provider:
        raise ValueError("OpenRouter required mode needs allow_provider=True")
    if config.target == "all" and not config.allow_external_controls:
        raise ValueError("target=all needs allow_external_controls=True")
    if config.min_free_mb < 1_000:
        raise ValueError("min_free_mb is too low for reproducible report artifact runs")


def build_suite_command(config: ModalSmokeConfig) -> list[str]:
    command = [
        "python3",
        "-m",
        "runner",
        "suite",
        "--profile",
        config.profile,
        "--dataset",
        config.dataset,
        "--records",
        str(config.records),
        "--target",
        config.target,
        "--surface",
        config.surface,
        "--openrouter-mode",
        config.openrouter_mode,
        "--openrouter-cap",
        config.openrouter_cap,
        "--run-id",
        config.run_id,
        "--reports-dir",
        config.reports_dir,
        "--scenarios",
        config.scenarios,
    ]
    if config.embedding_dimensions is not None:
        command.extend(["--embedding-dimensions", str(config.embedding_dimensions)])
    return command


def build_manifest(
    config: ModalSmokeConfig,
    suite_command: list[str],
    *,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    return {
        "kind": "tracedb_modal_smoke",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_id": config.run_id,
        "repo_root": str(repo_root),
        "suite_command": suite_command,
        "modal_resource_class": {
            "cpu": config.cpu,
            "memory_mb": config.memory_mb,
            "timeout_seconds": config.timeout_seconds,
            "ephemeral_disk_mb": config.ephemeral_disk_mb,
            "gpu_requested": config.gpu_requested,
        },
        "config": asdict(config),
    }


def bundle_report_artifacts(
    *,
    run_id: str,
    reports_dir: Path,
    bundle_dir: Path,
    manifest: dict[str, Any],
) -> Path:
    run_dir = reports_dir / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"report run directory not found: {run_dir}")
    bundle_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle = bundle_dir / f"{run_id}.tar.gz"
    with tarfile.open(bundle, "w:gz") as archive:
        archive.add(run_dir, arcname=run_id)
    return bundle


def extract_control_summary(bundle_path: Path, run_id: str) -> dict[str, Any]:
    suite_member = f"{run_id}/suite.json"
    with tarfile.open(bundle_path, "r:gz") as archive:
        extracted = archive.extractfile(suite_member)
        if extracted is None:
            raise FileNotFoundError(f"{suite_member} not found in {bundle_path}")
        suite = json.loads(extracted.read().decode("utf-8"))
    ledger = suite.get("control_ledger", {})
    return {
        "run_id": suite.get("suite_id", run_id),
        "control_status": suite.get("control_status", "unknown"),
        "failure_count": suite.get("summary", {}).get("failure_count", 0),
        "available_external_controls": [
            control.get("name")
            for control in ledger.get("available_external_controls", [])
        ],
        "unavailable_external_controls": [
            control.get("name")
            for control in ledger.get("unavailable_external_controls", [])
        ],
        "number_to_beat": suite.get("number_to_beat", {}),
        "suite_json": suite_member,
        "bundle_path": str(bundle_path),
    }


def run_suite_and_bundle(config: ModalSmokeConfig, *, lab_root: Path = LAB_ROOT) -> dict[str, Any]:
    validate_config(config)
    _ensure_free_space(Path(config.bundle_dir), config.min_free_mb)
    reports_dir = Path(config.reports_dir)
    bundle_dir = Path(config.bundle_dir)
    command = build_suite_command(config)
    env = os.environ.copy()
    env["BENCH_DISABLE_ENV_FILE"] = "1"
    completed = subprocess.run(
        command,
        cwd=lab_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    manifest = build_manifest(config, command, repo_root=lab_root.parent.parent)
    manifest["process"] = {
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
    }
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr or completed.stdout)
    bundle = bundle_report_artifacts(
        run_id=config.run_id,
        reports_dir=reports_dir,
        bundle_dir=bundle_dir,
        manifest=manifest,
    )
    summary = extract_control_summary(bundle, config.run_id)
    summary["manifest"] = manifest
    return summary


def _ensure_free_space(path: Path, min_free_mb: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    usage = os.statvfs(path)
    free_mb = (usage.f_bavail * usage.f_frsize) // (1024 * 1024)
    if free_mb < min_free_mb:
        raise RuntimeError(f"free disk {free_mb} MiB is below required {min_free_mb} MiB")


def _parse_args(argv: list[str] | None = None) -> ModalSmokeConfig:
    parser = argparse.ArgumentParser(description="Run a cost-guarded TraceDB Modal smoke benchmark")
    parser.add_argument("--run-id", default="modal-smoke")
    parser.add_argument("--records", type=int, default=128)
    parser.add_argument("--dataset", default="generated")
    parser.add_argument("--target", default="tracedb")
    parser.add_argument("--surface", default="sdk")
    parser.add_argument("--scenarios", default="sdk_cli_surface")
    parser.add_argument("--openrouter-mode", default="off", choices=["auto", "off", "required"])
    parser.add_argument("--openrouter-cap", default="moderate")
    parser.add_argument("--min-free-mb", type=int, default=20_000)
    parser.add_argument("--allow-large", action="store_true")
    parser.add_argument("--allow-external-controls", action="store_true")
    parser.add_argument("--allow-provider", action="store_true")
    args = parser.parse_args(argv)
    return ModalSmokeConfig(
        run_id=args.run_id,
        records=args.records,
        dataset=args.dataset,
        target=args.target,
        surface=args.surface,
        scenarios=args.scenarios,
        openrouter_mode=args.openrouter_mode,
        openrouter_cap=args.openrouter_cap,
        min_free_mb=args.min_free_mb,
        allow_large=args.allow_large,
        allow_external_controls=args.allow_external_controls,
        allow_provider=args.allow_provider,
    )


def run_local(argv: list[str] | None = None) -> int:
    config = _parse_args(argv)
    summary = run_suite_and_bundle(config)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if modal is not None:
    image = (
        modal.Image.debian_slim(python_version="3.12")
        .apt_install("build-essential", "ca-certificates", "curl", "pkg-config", "libssl-dev")
        .pip_install_from_requirements(str(LAB_ROOT / "requirements.txt"))
        .add_local_dir(str(REPO_ROOT), remote_path=REMOTE_REPO, ignore=MODAL_IGNORE_PATTERNS)
    )
    app = modal.App("tracedb-realworld-smoke")

    @app.function(
        image=image,
        cpu=ModalSmokeConfig.cpu,
        memory=ModalSmokeConfig.memory_mb,
        timeout=ModalSmokeConfig.timeout_seconds,
        ephemeral_disk=ModalSmokeConfig.ephemeral_disk_mb,
    )
    def run_smoke(**kwargs: Any) -> dict[str, Any]:
        config = ModalSmokeConfig(**kwargs)
        return run_suite_and_bundle(config, lab_root=Path(REMOTE_REPO) / "benchmarks" / "realworld")

    @app.local_entrypoint()
    def main(
        run_id: str = "modal-smoke",
        records: int = 128,
        dataset: str = "generated",
        target: str = "tracedb",
        surface: str = "sdk",
        scenarios: str = "sdk_cli_surface",
        openrouter_mode: str = "off",
    ) -> None:
        result = run_smoke.remote(
            run_id=run_id,
            records=records,
            dataset=dataset,
            target=target,
            surface=surface,
            scenarios=scenarios,
            openrouter_mode=openrouter_mode,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
else:
    app = None


if __name__ == "__main__":
    raise SystemExit(run_local())
