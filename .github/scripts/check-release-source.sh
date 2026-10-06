#!/usr/bin/env bash
# Usage: check-release-source.sh <sha> <repo>
# Fails unless <sha> is on origin/rigel-release and its CI (push) status is success:
# releases come only from the gated branch, never from the resettable main mirror.
set -euo pipefail
sha="$1" repo="$2"
branch=rigel-release

if ! git merge-base --is-ancestor "$sha" "origin/$branch" 2>/dev/null; then
  echo "::error::$sha is not on origin/$branch" >&2
  exit 1
fi
state="$(gh api "repos/$repo/commits/$sha/status" --jq '[.statuses[] | select(.context == "CI (push)") | .state][0] // "missing"')"
if [ "$state" != "success" ]; then
  echo "::error::CI (push) on $sha is '$state', not success" >&2
  exit 1
fi
