#!/usr/bin/env bash
# Usage: resolve-image-version.sh <event> <ref> <dispatch-version> <flake.nix>
# Prints the tag-derived version only when it has the required shape and matches flake.nix.
set -euo pipefail
event="$1" ref="$2" dispatch_version="$3" flake="$4"

mapfile -t versions < <(sed -n 's/^ *version = "\([^"]*\)";$/\1/p' "$flake")
if [ "${#versions[@]}" -ne 1 ]; then
  echo "::error::expected exactly one flake.nix version literal, found ${#versions[@]}" >&2
  exit 1
fi
flake_version="${versions[0]}"
case "$event" in
  push) version="${ref#refs/tags/v}" ;;
  workflow_dispatch) version="$dispatch_version" ;;
  *)
    echo "::error::unsupported event '$event'" >&2
    exit 1
    ;;
esac

if [[ ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-rigel\.[1-9][0-9]*$ ]]; then
  echo "::error::version '$version' does not match the required X.Y.Z-rigel.N shape" >&2
  exit 1
fi
if [ "$version" != "$flake_version" ]; then
  echo "::error::version '$version' does not match flake.nix version '$flake_version'" >&2
  exit 1
fi
echo "$version"
