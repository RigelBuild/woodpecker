#!/usr/bin/env bash
# Usage: check-release-source.sh <sha> <repo>
# Fails unless <sha> is on origin/rigel-release, not on the main mirror, and CI (push) is green.
# A commit status cannot name its branch, so a SHA main shares could carry main's CI result.
set -euo pipefail
sha="$1" repo="$2"
branch=rigel-release

if ! git merge-base --is-ancestor "$sha" "origin/$branch" 2>/dev/null; then
  echo "::error::$sha is not on origin/$branch" >&2
  exit 1
fi
if ! git rev-parse --verify --quiet origin/main >/dev/null; then
  echo "::error::origin/main is missing; cannot rule out a mirror commit" >&2
  exit 1
fi
if git merge-base --is-ancestor "$sha" origin/main; then
  echo "::error::$sha is on the main mirror; release from a rigel-release-only commit" >&2
  exit 1
fi
state="$(gh api "repos/$repo/commits/$sha/status" --jq '[.statuses[] | select(.context == "CI (push)") | .state][0] // "missing"')"
if [ "$state" != "success" ]; then
  echo "::error::CI (push) on $sha is '$state', not success" >&2
  exit 1
fi
