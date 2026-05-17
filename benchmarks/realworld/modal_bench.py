from __future__ import annotations

import argparse
import glob
import json
import os
import shlex
import shutil
import subprocess
import tarfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

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
POSTGRES_DSN_ENV = "BENCH_POSTGRES_DSN"
EXTERNAL_CONTROL_TARGETS = {
    "postgres",
    "pgvector",
    "mongodb",
    "qdrant",
    "opensearch",
    "milvus",
    "mysql",
}
SENSITIVE_ENV_KEYS = {
    POSTGRES_DSN_ENV,
    "BENCH_PGVECTOR_DSN",
    "BENCH_MONGO_URI",
    "OPENROUTER_API_KEY",
}
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
    require_services: bool = False
    postgres_control: bool = False
    postgres_port: int = 25_432


def validate_config(config: ModalSmokeConfig) -> None:
    if config.records > DEFAULT_MAX_RECORDS and not config.allow_large:
        raise ValueError(
            f"records={config.records} exceeds safe default {DEFAULT_MAX_RECORDS}; set allow_large explicitly"
        )
    if config.gpu_requested and not config.allow_gpu:
        raise ValueError("GPU use requires allow_gpu=True")
    if config.openrouter_mode == "required" and not config.allow_provider:
        raise ValueError("OpenRouter required mode needs allow_provider=True")
    targets = requested_targets(config.target)
    if "all" in targets and not config.allow_external_controls:
        raise ValueError("target=all needs allow_external_controls=True")
    if external_targets(targets) and not config.allow_external_controls:
        raise ValueError("external controls need allow_external_controls=True")
    if config.postgres_control and not config.allow_external_controls:
        raise ValueError("postgres_control needs allow_external_controls=True")
    if config.postgres_control and not target_needs_postgres(config):
        raise ValueError("postgres_control needs target including postgres or all")
    if config.min_free_mb < 1_000:
        raise ValueError("min_free_mb is too low for reproducible report artifact runs")


def requested_targets(target: str) -> set[str]:
    return {part.strip() for part in target.split(",") if part.strip()}


def external_targets(targets: set[str]) -> set[str]:
    if "all" in targets:
        return set(EXTERNAL_CONTROL_TARGETS)
    return targets & EXTERNAL_CONTROL_TARGETS


def target_needs_postgres(config: ModalSmokeConfig) -> bool:
    targets = requested_targets(config.target)
    return "all" in targets or "postgres" in targets


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
    if config.require_services:
        command.append("--require-services")
    return command


def build_runner_env(
    config: ModalSmokeConfig,
    *,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    env = dict(os.environ if base_env is None else base_env)
    env["BENCH_DISABLE_ENV_FILE"] = "1"
    if config.postgres_control:
        env[POSTGRES_DSN_ENV] = postgres_control_dsn(config)
    elif config.require_services and target_needs_postgres(config) and not env.get(POSTGRES_DSN_ENV):
        raise ValueError(f"{POSTGRES_DSN_ENV} is required for required PostgreSQL control runs")
    return env


def postgres_control_dsn(config: ModalSmokeConfig) -> str:
    return f"postgresql://tracedb:tracedb@127.0.0.1:{config.postgres_port}/tracedb_bench"


def redacted_env(env: Mapping[str, str]) -> dict[str, str]:
    redacted = {}
    for key, value in env.items():
        if key in SENSITIVE_ENV_KEYS:
            redacted[key] = "[redacted]"
        elif key.startswith("BENCH_") or key.startswith("OPENROUTER_"):
            redacted[key] = value
    return redacted


def build_manifest(
    config: ModalSmokeConfig,
    suite_command: list[str],
    *,
    repo_root: Path = REPO_ROOT,
    runner_env: Mapping[str, str] | None = None,
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
        "runner_env": redacted_env(runner_env or {}),
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
    env = build_runner_env(config)
    postgres_service: PostgresControl | None = None
    try:
        if config.postgres_control:
            postgres_service = start_postgres_control(config)
        completed = subprocess.run(
            command,
            cwd=lab_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    finally:
        if postgres_service is not None:
            stop_postgres_control(postgres_service)
    manifest = build_manifest(config, command, repo_root=lab_root.parent.parent, runner_env=env)
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


@dataclass(frozen=True)
class PostgresControl:
    data_dir: Path
    log_path: Path
    port: int


def start_postgres_control(config: ModalSmokeConfig) -> PostgresControl:
    data_dir = Path("/tmp") / f"tracedb-postgres-{config.run_id}"
    log_path = Path("/tmp") / f"tracedb-postgres-{config.run_id}.log"
    bin_dir = postgres_bin_dir()
    initdb = bin_dir / "initdb"
    pg_ctl = bin_dir / "pg_ctl"
    createdb = shutil.which("createdb")
    if createdb is None:
        raise RuntimeError("createdb not found; install postgresql-client in the Modal image")
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)
    if _running_as_root():
        subprocess.run(
            ["chown", "-R", "postgres:postgres", str(data_dir)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    _run_maybe_as_postgres(
        [
            str(initdb),
            "-D",
            str(data_dir),
            "-U",
            "tracedb",
            "--auth=trust",
            "--no-instructions",
        ]
    )
    _run_maybe_as_postgres(
        [
            str(pg_ctl),
            "-D",
            str(data_dir),
            "-l",
            str(log_path),
            "-o",
            f"-h 127.0.0.1 -p {config.postgres_port}",
            "-w",
            "-t",
            "30",
            "start",
        ]
    )
    subprocess.run(
        [
            createdb,
            "-h",
            "127.0.0.1",
            "-p",
            str(config.postgres_port),
            "-U",
            "tracedb",
            "tracedb_bench",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    return PostgresControl(data_dir=data_dir, log_path=log_path, port=config.postgres_port)


def stop_postgres_control(service: PostgresControl) -> None:
    bin_dir = postgres_bin_dir()
    pg_ctl = bin_dir / "pg_ctl"
    _run_maybe_as_postgres(
        [str(pg_ctl), "-D", str(service.data_dir), "-m", "fast", "-w", "stop"],
        check=False,
    )


def postgres_bin_dir() -> Path:
    candidates = sorted(glob.glob("/usr/lib/postgresql/*/bin/initdb"))
    if candidates:
        return Path(candidates[-1]).parent
    initdb = shutil.which("initdb")
    if initdb is not None:
        return Path(initdb).parent
    raise RuntimeError("initdb not found; install postgresql in the Modal image")


def _run_maybe_as_postgres(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    if _running_as_root():
        shell_command = " ".join(shlex.quote(part) for part in command)
        command = ["su", "postgres", "-c", shell_command]
    return subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=check,
    )


def _running_as_root() -> bool:
    return hasattr(os, "geteuid") and os.geteuid() == 0


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
    parser.add_argument("--require-services", action="store_true")
    parser.add_argument("--postgres-control", action="store_true")
    parser.add_argument("--postgres-port", type=int, default=25_432)
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
        require_services=args.require_services,
        postgres_control=args.postgres_control,
        postgres_port=args.postgres_port,
    )


def run_local(argv: list[str] | None = None) -> int:
    config = _parse_args(argv)
    summary = run_suite_and_bundle(config)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if modal is not None:
    BASE_APT_PACKAGES = (
        "build-essential",
        "ca-certificates",
        "curl",
        "pkg-config",
        "libssl-dev",
    )

    def modal_image(*extra_packages: str) -> modal.Image:
        return (
            modal.Image.debian_slim(python_version="3.12")
            .apt_install(*BASE_APT_PACKAGES, *extra_packages)
            .pip_install_from_requirements(str(LAB_ROOT / "requirements.txt"))
            .add_local_dir(str(REPO_ROOT), remote_path=REMOTE_REPO, ignore=MODAL_IGNORE_PATTERNS)
        )

    image = modal_image()
    postgres_image = modal_image("postgresql", "postgresql-client")
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

    @app.function(
        image=postgres_image,
        cpu=ModalSmokeConfig.cpu,
        memory=ModalSmokeConfig.memory_mb,
        timeout=ModalSmokeConfig.timeout_seconds,
        ephemeral_disk=ModalSmokeConfig.ephemeral_disk_mb,
    )
    def run_smoke_with_postgres(**kwargs: Any) -> dict[str, Any]:
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
        allow_external_controls: bool = False,
        require_services: bool = False,
        postgres_control: bool = False,
        allow_large: bool = False,
        allow_provider: bool = False,
    ) -> None:
        remote_function = run_smoke_with_postgres if postgres_control else run_smoke
        result = remote_function.remote(
            run_id=run_id,
            records=records,
            dataset=dataset,
            target=target,
            surface=surface,
            scenarios=scenarios,
            openrouter_mode=openrouter_mode,
            allow_external_controls=allow_external_controls,
            require_services=require_services,
            postgres_control=postgres_control,
            allow_large=allow_large,
            allow_provider=allow_provider,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
else:
    app = None


if __name__ == "__main__":
    raise SystemExit(run_local())
