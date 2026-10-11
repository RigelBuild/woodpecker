#!/usr/bin/env bash
# Usage: check-release-source.sh <ref>
# Fails unless the tag or branch commit under release is reachable from rigel-release.
set -euo pipefail
ref="$1"
branch=rigel-release
commit="$(git rev-parse --verify "${ref}^{commit}")"

if ! git merge-base --is-ancestor "$commit" "origin/$branch" 2>/dev/null; then
  echo "::error::$commit from $ref is not reachable from origin/$branch" >&2
  exit 1
fi
echo "$commit is reachable from origin/$branch"
