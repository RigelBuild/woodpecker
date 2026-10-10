#!/usr/bin/env bash
# Apply the two release tags to each immutable build digest and record manifest pins.
set -euo pipefail

: "${VERSION:?VERSION is required}"
: "${COMMIT_SHA:?COMMIT_SHA is required}"
: "${AGENT_DIGEST:?AGENT_DIGEST is required}"
: "${SERVER_DIGEST:?SERVER_DIGEST is required}"

finalize_image() {
  local image="$1" digest="$2" tag reference resolved
  for tag in "$VERSION" "sha-${COMMIT_SHA}"; do
    docker buildx imagetools create --tag "${image}:${tag}" "${image}@${digest}"
    reference="${image}:${tag}"
    resolved="$(docker buildx imagetools inspect "$reference" --format '{{.Manifest.Digest}}')"
    if [ "$resolved" != "$digest" ]; then
      echo "::error::${reference} resolves to ${resolved}, expected ${digest}" >&2
      return 1
    fi
    printf '%s@%s\n' "$reference" "$resolved" >> "$GITHUB_STEP_SUMMARY"
  done
}

finalize_image ghcr.io/rigelbuild/woodpecker-agent "$AGENT_DIGEST"
finalize_image ghcr.io/rigelbuild/woodpecker-server "$SERVER_DIGEST"
