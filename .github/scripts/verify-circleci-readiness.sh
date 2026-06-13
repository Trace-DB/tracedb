#!/usr/bin/env bash
set -euo pipefail

required="${REQUIRED_CIRCLECI_CHECK:-circleci/${GITHUB_REPOSITORY##*/}}"
commit_sha="${RELEASE_COMMIT_SHA:-}"
if [[ -z "$commit_sha" ]]; then
  if [[ "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
    commit_sha="$(git rev-list -n 1 "${GITHUB_REF_NAME:-}" 2>/dev/null || true)"
  fi
  commit_sha="${commit_sha:-${GITHUB_SHA:?}}"
fi

export RELEASE_COMMIT_SHA="$commit_sha"
export REQUIRED_CIRCLECI_CHECK="$required"
export REQUIRED_CIRCLECI_CHECKS="${REQUIRED_CIRCLECI_CHECKS:-$required}"
: "${GITHUB_REPOSITORY:?}"
: "${GITHUB_TOKEN:?}"

python3 <<'PY'
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

repo = os.environ["GITHUB_REPOSITORY"]
sha = os.environ["RELEASE_COMMIT_SHA"]
required = os.environ["REQUIRED_CIRCLECI_CHECK"]
token = os.environ["GITHUB_TOKEN"]

headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "tracedb-release-readiness",
}

def get(path):
    request = urllib.request.Request(f"https://api.github.com/repos/{repo}{path}", headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)

required_names = [
    check.strip().lower()
    for check in os.environ.get("REQUIRED_CIRCLECI_CHECKS", required).replace("\n", ",").split(",")
    if check.strip()
]
max_attempts = int(os.environ.get("CIRCLECI_READINESS_ATTEMPTS", "120"))
sleep_seconds = int(os.environ.get("CIRCLECI_READINESS_SLEEP_SECONDS", "30"))
require_fresh = os.environ.get("CIRCLECI_READINESS_REQUIRE_FRESH", "").lower() in {"1", "true", "yes"}
fresh_grace_seconds = int(os.environ.get("CIRCLECI_READINESS_FRESH_GRACE_SECONDS", "600"))
fresh_cutoff = datetime.now(timezone.utc) - timedelta(seconds=fresh_grace_seconds)


def load_candidates():
    candidates = []
    statuses = []
    check_runs = []
    for page in get_pages(f"/commits/{sha}/statuses?per_page=100"):
        statuses.extend(page)
    for page in get_pages(f"/commits/{sha}/check-runs?per_page=100&filter=all"):
        check_runs.extend(page.get("check_runs", []))
    for status in statuses:
        candidates.append({
            "name": status.get("context") or "",
            "state": status.get("state") or "",
            "url": status.get("target_url") or "",
            "provider": "",
            "updated_at": status.get("updated_at") or status.get("created_at") or "",
            "id": status.get("id") or 0,
        })
    for check in check_runs:
        app = check.get("app") or {}
        completed = check.get("status") == "completed"
        candidates.append({
            "name": check.get("name") or "",
            "state": check.get("conclusion") if completed else check.get("status"),
            "url": check.get("html_url") or "",
            "provider": app.get("slug") or "",
            "updated_at": check.get("completed_at") or check.get("started_at") or check.get("created_at") or "",
            "id": check.get("id") or 0,
        })
    return candidates

def get_pages(path):
    url = f"https://api.github.com/repos/{repo}{path}"
    while url:
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            data = json.load(response)
            link = response.headers.get("Link", "")
        yield data
        url = ""
        for part in link.split(","):
            section = part.strip()
            if 'rel="next"' not in section:
                continue
            url = section.split(";", 1)[0].strip()[1:-1]
            break

def is_named_match(candidate):
    name = candidate["name"].lower()
    return any(name == check or name.endswith(check) for check in required_names)

def is_circleci(candidate):
    haystack = " ".join([
        candidate["name"].lower(),
        candidate["url"].lower(),
        candidate["provider"].lower(),
    ])
    return "circleci" in haystack

def parse_time(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

def is_fresh(candidate):
    if not require_fresh:
        return True
    updated_at = parse_time(candidate["updated_at"])
    return updated_at is not None and updated_at >= fresh_cutoff

def find_matches(candidates):
    matches = [
        candidate
        for candidate in candidates
        if is_circleci(candidate) and is_named_match(candidate) and is_fresh(candidate)
    ]
    latest = {}
    for candidate in matches:
        key = (candidate["name"].lower(), candidate["provider"].lower())
        previous = latest.get(key)
        if previous is None or (candidate["updated_at"], candidate["id"]) >= (previous["updated_at"], previous["id"]):
            latest[key] = candidate
    return list(latest.values())


pending_states = {"pending", "queued", "in_progress", "waiting", "requested"}
for attempt in range(1, max_attempts + 1):
    candidates = load_candidates()
    matches = find_matches(candidates)
    if not matches:
        if attempt < max_attempts:
            time.sleep(sleep_seconds)
            continue
        print(f"No CircleCI readiness check matched {required_names!r} for {sha}.", file=sys.stderr)
        if require_fresh:
            print(f"Only checks updated at or after {fresh_cutoff.isoformat()} are eligible.", file=sys.stderr)
        for candidate in candidates:
            print(f"- {candidate['name']}: {candidate['state']} {candidate['url']}", file=sys.stderr)
        sys.exit(1)

    successes = [candidate for candidate in matches if candidate["state"] == "success"]
    if successes:
        for candidate in successes:
            print(f"CircleCI readiness accepted: {candidate['name']} {candidate['state']} {candidate['url']}")
        break

    pending = [candidate for candidate in matches if candidate["state"] in pending_states]
    if pending:
        if attempt < max_attempts:
            time.sleep(sleep_seconds)
            continue
        print(f"Timed out waiting for CircleCI readiness check for {sha}.", file=sys.stderr)
        for candidate in matches:
            print(f"- {candidate['name']}: {candidate['state']} {candidate['url']}", file=sys.stderr)
        sys.exit(1)

    print(f"CircleCI readiness check is not green for {sha}.", file=sys.stderr)
    for candidate in matches:
        print(f"- {candidate['name']}: {candidate['state']} {candidate['url']}", file=sys.stderr)
    sys.exit(1)
PY
