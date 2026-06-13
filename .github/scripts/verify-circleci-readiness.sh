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
: "${GITHUB_REPOSITORY:?}"
: "${GITHUB_TOKEN:?}"

python3 <<'PY'
import json
import os
import sys
import urllib.request

repo = os.environ["GITHUB_REPOSITORY"]
sha = os.environ["RELEASE_COMMIT_SHA"]
required = os.environ["REQUIRED_CIRCLECI_CHECK"]
token = os.environ["GITHUB_TOKEN"]
slug = repo.rsplit("/", 1)[-1].lower()

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

statuses = get(f"/commits/{sha}/status").get("statuses", [])
check_runs = get(f"/commits/{sha}/check-runs?per_page=100").get("check_runs", [])

candidates = []
for status in statuses:
    candidates.append({
        "name": status.get("context") or "",
        "state": status.get("state") or "",
        "url": status.get("target_url") or "",
        "provider": "",
    })
for check in check_runs:
    app = check.get("app") or {}
    completed = check.get("status") == "completed"
    candidates.append({
        "name": check.get("name") or "",
        "state": check.get("conclusion") if completed else check.get("status"),
        "url": check.get("html_url") or "",
        "provider": app.get("slug") or "",
    })

required_lower = required.lower()

def is_named_match(candidate):
    name = candidate["name"].lower()
    return name == required_lower or name.endswith(required_lower) or name == slug

def is_circleci(candidate):
    haystack = " ".join([
        candidate["name"].lower(),
        candidate["url"].lower(),
        candidate["provider"].lower(),
    ])
    return "circleci" in haystack

matches = [candidate for candidate in candidates if is_named_match(candidate)]
if not matches:
    matches = [candidate for candidate in candidates if is_circleci(candidate) and slug in " ".join([
        candidate["name"].lower(),
        candidate["url"].lower(),
        candidate["provider"].lower(),
    ])]
if not matches:
    matches = [candidate for candidate in candidates if is_circleci(candidate)]

if not matches:
    print(f"No CircleCI readiness check matched {required!r} for {sha}.", file=sys.stderr)
    for candidate in candidates:
        print(f"- {candidate['name']}: {candidate['state']} {candidate['url']}", file=sys.stderr)
    sys.exit(1)

passing = {"success", "neutral", "skipped"}
failed = [candidate for candidate in matches if candidate["state"] not in passing]
if failed:
    print(f"CircleCI readiness check is not green for {sha}.", file=sys.stderr)
    for candidate in matches:
        print(f"- {candidate['name']}: {candidate['state']} {candidate['url']}", file=sys.stderr)
    sys.exit(1)

for candidate in matches:
    print(f"CircleCI readiness accepted: {candidate['name']} {candidate['state']} {candidate['url']}")
PY
