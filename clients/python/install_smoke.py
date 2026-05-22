from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import venv
from pathlib import Path


CLIENT_ROOT = Path(__file__).resolve().parent
MIN_PYTHON = (3, 11)
MAX_PREFERRED_PYTHON = (3, 14)


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _run(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout: int = 120,
) -> subprocess.CompletedProcess[str]:
    process = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    if process.returncode != 0:
        raise RuntimeError(
            "command failed: "
            + " ".join(argv)
            + f"\nstdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    return process


def _python_version(python: str) -> tuple[int, int] | None:
    try:
        result = _run(
            [
                python,
                "-c",
                "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')",
            ],
            cwd=CLIENT_ROOT,
            timeout=10,
        )
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        return None
    major, minor = result.stdout.strip().split(".", 1)
    return int(major), int(minor)


def _candidate_interpreters() -> list[str]:
    candidates: list[str] = []
    for executable in [sys.executable, "python3.12", "python3.11", "python3"]:
        resolved = shutil.which(executable) if executable != sys.executable else executable
        if resolved is not None and resolved not in candidates:
            candidates.append(resolved)
    return candidates


def _venv_interpreter() -> str:
    fallback = sys.executable
    for python in _candidate_interpreters():
        version = _python_version(python)
        if version is None or version < MIN_PYTHON:
            continue
        if version < MAX_PREFERRED_PYTHON:
            return python
        fallback = python
    return fallback


def _create_venv(venv_dir: Path) -> None:
    python = _venv_interpreter()
    version = _python_version(python)
    if Path(python).resolve() == Path(sys.executable).resolve() and version is not None and version < MAX_PREFERRED_PYTHON:
        venv.EnvBuilder(with_pip=True).create(venv_dir)
        return
    _run([python, "-m", "venv", str(venv_dir)], cwd=venv_dir.parent, timeout=120)


def _write_consumer(consumer: Path) -> None:
    consumer.write_text(
        textwrap.dedent(
            """
            from tracedb import TraceDB, TraceDBRequestError

            db = TraceDB.from_env(
                env={
                    "TRACEDB_URL": "http://127.0.0.1:8090/",
                    "TRACEDB_TOKEN": "dev-token",
                    "TRACEDB_DATABASE_ID": "db_local",
                    "TRACEDB_BRANCH_ID": "db_local:main",
                    "TRACEDB_TIMEOUT_MS": "2500",
                    "TRACEDB_SAFE_RETRIES": "2",
                }
            )
            assert db.url == "http://127.0.0.1:8090"
            assert db.token == "dev-token"
            assert db.database_id == "db_local"
            assert db.branch_id == "db_local:main"
            assert db.timeout == 2.5
            assert db.safe_retries == 2

            table = db.table("docs").tenant("tenant-a").limit(20)
            assert table.name == "docs"
            assert table.tenant_id == "tenant-a"
            assert table.scan_limit == 20

            try:
                TraceDB.from_env(env={})
            except TraceDBRequestError as error:
                assert "TRACEDB_URL" in str(error)
            else:
                raise AssertionError("TraceDB.from_env should reject missing TRACEDB_URL")

            print("python sdk install smoke ok")
            """
        ).strip()
        + "\n"
    )


def _run_venv_smoke(temp_dir: Path, consumer: Path, package_dir: Path) -> str:
    venv_dir = temp_dir / ".venv"
    _create_venv(venv_dir)
    python = _venv_python(venv_dir)

    _run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-deps",
            str(package_dir),
        ],
        cwd=temp_dir,
    )

    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    result = _run([str(python), str(consumer)], cwd=consumer.parent, env=env)
    return result.stdout


def _run_target_smoke(temp_dir: Path, consumer: Path, package_dir: Path) -> str:
    target_dir = temp_dir / "target-site"
    _run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-deps",
            "--target",
            str(target_dir),
            str(package_dir),
        ],
        cwd=temp_dir,
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = str(target_dir)
    result = _run([sys.executable, str(consumer)], cwd=consumer.parent, env=env)
    return result.stdout


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="tracedb-python-install-") as temp:
        temp_dir = Path(temp)
        consumer_dir = temp_dir / "consumer"
        package_dir = temp_dir / "package"
        consumer_dir.mkdir()
        shutil.copytree(
            CLIENT_ROOT,
            package_dir,
            ignore=shutil.ignore_patterns("build", "*.egg-info", "__pycache__"),
        )

        consumer = consumer_dir / "consumer.py"
        _write_consumer(consumer)

        try:
            stdout = _run_venv_smoke(temp_dir, consumer, package_dir)
        except (RuntimeError, subprocess.SubprocessError, OSError):
            stdout = _run_target_smoke(temp_dir, consumer, package_dir)
        if "python sdk install smoke ok" not in stdout:
            raise RuntimeError(f"consumer did not emit install smoke sentinel:\n{stdout}")

    print("python sdk install smoke ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
