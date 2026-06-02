#!/usr/bin/env python3
"""Validate that all tracedb-protocol.lock files agree on contract revision.

Reads every ``tracedb-protocol.lock`` file under a core repo or multi-repo
workspace, verifies that they all reference the same contract revision and
contract version, and optionally checks that the revision matches the latest in
the local protocol repo.

Lock files are TOML (matching the existing ``tracedb-protocol.lock`` format)::

    repo = "https://github.com/Trace-DB/tracedb-protocol"
    revision = "4aac3d6d2fe2fda3bc31c87416a43c19b785b35b"
    contract = "platform-contract-v0"

Usage:
    # From tracedb/ in a multi-repo workspace, scans the workspace by default.
    python3 scripts/validate_protocol_locks.py

    # Scan only the core repo.
    python3 scripts/validate_protocol_locks.py --repo-root /path/to/Trace-DB/tracedb

    # Scan all sibling repos in the organization workspace.
    python3 tracedb/scripts/validate_protocol_locks.py --repo-root /path/to/Trace-DB

    # Override protocol HEAD discovery.
    python3 scripts/validate_protocol_locks.py --protocol-repo /path/to/tracedb-protocol
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

CORE_REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCAN_ROOT = (
    CORE_REPO_ROOT.parent
    if (CORE_REPO_ROOT.parent / "tracedb-protocol").is_dir()
    else CORE_REPO_ROOT
)
LOCK_GLOB = "**/tracedb-protocol.lock"

TOML_KV = re.compile(r'^(\w+)\s*=\s*"([^"]*)"')


def find_lock_files(repo_root: Path) -> list[Path]:
    """Return all tracedb-protocol.lock files under the repo."""
    return sorted(p for p in repo_root.glob(LOCK_GLOB) if p.is_file())


def parse_toml_lock(path: Path) -> dict[str, str]:
    """Parse a minimal TOML lock file (key = "value" lines only)."""
    data: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = TOML_KV.match(line)
        if m:
            data[m.group(1)] = m.group(2)
    return data


def validate_locks(
    repo_root: Path,
    *,
    protocol_repo: Path | None = None,
    skip_latest_check: bool = False,
) -> int:
    """Validate all lock files. Returns 0 on success, 1 on drift or error."""
    lock_files = find_lock_files(repo_root)

    if not lock_files:
        print("WARN: no tracedb-protocol.lock files found")
        print("HINT: if SDKs have not been initialised yet, this is expected")
        return 0

    print(f"Found {len(lock_files)} lock file(s):")
    revisions: dict[str, list[Path]] = {}
    contracts: dict[str, list[Path]] = {}

    for lf in lock_files:
        data = parse_toml_lock(lf)
        rev = data.get("revision", "")
        contract = data.get("contract", "")
        if not rev:
            print(
                f"FAIL: {lf.relative_to(repo_root)} missing 'revision' key",
                file=sys.stderr,
            )
            return 1
        if not contract:
            print(
                f"FAIL: {lf.relative_to(repo_root)} missing 'contract' key",
                file=sys.stderr,
            )
            return 1
        revisions.setdefault(rev, []).append(lf)
        contracts.setdefault(contract, []).append(lf)
        print(f"  {lf.relative_to(repo_root)}: contract={contract!r} revision={rev!r}")

    # Check that all revisions agree.
    if len(revisions) > 1:
        print("\nFAIL: lock files reference different revisions:", file=sys.stderr)
        for rev, paths in revisions.items():
            print(f"  {rev!r}:", file=sys.stderr)
            for p in paths:
                print(f"    {p.relative_to(repo_root)}", file=sys.stderr)
        return 1

    # Check that all contracts agree.
    if len(contracts) > 1:
        print(
            "\nFAIL: lock files reference different contract versions:", file=sys.stderr
        )
        for contract, paths in contracts.items():
            print(f"  {contract!r}:", file=sys.stderr)
            for p in paths:
                print(f"    {p.relative_to(repo_root)}", file=sys.stderr)
        return 1

    agreed_rev = next(iter(revisions))
    agreed_contract = next(iter(contracts))

    if not skip_latest_check:
        resolved_protocol_repo = protocol_repo or discover_protocol_repo(repo_root)
        if resolved_protocol_repo is None:
            print(
                "WARN: tracedb-protocol repository not found; skipped latest-revision check"
            )
        else:
            latest_rev = git_head_revision(resolved_protocol_repo)
            if latest_rev is None:
                print(
                    f"WARN: could not read git HEAD for protocol repo {resolved_protocol_repo}; skipped latest-revision check"
                )
            elif latest_rev != agreed_rev:
                print(
                    "\nFAIL: lock revision does not match tracedb-protocol HEAD:",
                    file=sys.stderr,
                )
                print(f"  lock revision:     {agreed_rev}", file=sys.stderr)
                print(f"  protocol repo HEAD: {latest_rev}", file=sys.stderr)
                print(
                    f"  protocol repo:      {resolved_protocol_repo}", file=sys.stderr
                )
                return 1
            else:
                print(
                    f"Latest-revision check: {resolved_protocol_repo} HEAD matches lock"
                )

    print(
        f"\nOK: all lock files agree on contract={agreed_contract!r} "
        f"revision={agreed_rev!r}"
    )
    return 0


def discover_protocol_repo(repo_root: Path) -> Path | None:
    """Find a sibling or child tracedb-protocol checkout when available."""
    candidates = [repo_root / "tracedb-protocol", repo_root.parent / "tracedb-protocol"]
    for candidate in candidates:
        if candidate.is_dir() and (candidate / ".git").exists():
            return candidate
    return None


def git_head_revision(repo: Path) -> str | None:
    """Return git HEAD for repo, or None when unavailable."""
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    revision = completed.stdout.strip()
    return revision or None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate tracedb-protocol.lock files are in sync across a single "
            "repo or the TraceDB multi-repo workspace."
        ),
    )
    parser.add_argument(
        "--repo-root",
        default=str(DEFAULT_SCAN_ROOT),
        help=(
            "Path to scan for tracedb-protocol.lock files. Use the core "
            "tracedb repo root for a single-repo check, or the Trace-DB "
            f"workspace root to compare sibling repos (default: {DEFAULT_SCAN_ROOT})."
        ),
    )
    parser.add_argument(
        "--protocol-repo",
        default="",
        help=(
            "Path to a local tracedb-protocol checkout for HEAD revision "
            "validation. By default, the script looks under --repo-root and "
            "next to --repo-root."
        ),
    )
    parser.add_argument(
        "--skip-latest-check",
        action="store_true",
        help="Only check lock-file agreement; do not compare with tracedb-protocol HEAD.",
    )
    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    if not repo_root.is_dir():
        print(f"FAIL: --repo-root is not a directory: {repo_root}", file=sys.stderr)
        return 1

    protocol_repo = (
        Path(args.protocol_repo).expanduser().resolve() if args.protocol_repo else None
    )
    if protocol_repo is not None and not protocol_repo.is_dir():
        print(
            f"FAIL: --protocol-repo is not a directory: {protocol_repo}",
            file=sys.stderr,
        )
        return 1

    return validate_locks(
        repo_root,
        protocol_repo=protocol_repo,
        skip_latest_check=args.skip_latest_check,
    )


if __name__ == "__main__":
    raise SystemExit(main())
