#!/usr/bin/env bash
# Usage: resolve-image-version.sh <event> <ref-name> <dispatch-version> <flake.nix>
# Prints the image version; fails unless it equals the flake.nix version literal.
set -euo pipefail
event="$1" ref_name="$2" dispatch_version="$3" flake="$4"

if [ "$event" = "push" ]; then
  version="${ref_name#v}"
else
  version="$dispatch_version"
fi
flake_version="$(sed -n 's/^ *version = "\(.*\)";$/\1/p' "$flake" | head -n1)"

if [ -z "$version" ] || [ "$version" != "$flake_version" ]; then
  echo "::error::version '$version' does not match flake.nix version '$flake_version'" >&2
  exit 1
fi
echo "$version"
