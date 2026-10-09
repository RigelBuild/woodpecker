#!/usr/bin/env bash
# Tests for resolve-image-version.sh: tag and dispatch inputs, and the flake guard.
set -u
script="$(dirname "$0")/resolve-image-version.sh"
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
printf '      in {\n        version = "3.17.0-rigel.3";\n' >"$dir/flake.nix"
fails=0

expect() {
  local name="$1" want_rc="$2" want_out="$3"
  shift 3
  local out rc
  out="$("$script" "$@" 2>/dev/null)"
  rc=$?
  if [ "$rc" != "$want_rc" ] || [ "$out" != "$want_out" ]; then
    echo "FAIL $name: rc=$rc out='$out' (want rc=$want_rc out='$want_out')"
    fails=$((fails + 1))
  else
    echo "ok   $name"
  fi
}

expect "tag push strips v" 0 3.17.0-rigel.3 push v3.17.0-rigel.3 "" "$dir/flake.nix"
expect "dispatch uses input" 0 3.17.0-rigel.3 workflow_dispatch main 3.17.0-rigel.3 "$dir/flake.nix"
expect "dispatch ignores ref" 0 3.17.0-rigel.3 workflow_dispatch v9.9.9-rigel.9 3.17.0-rigel.3 "$dir/flake.nix"
expect "tag mismatch fails" 1 "" push v3.17.0-rigel.4 "" "$dir/flake.nix"
expect "dispatch mismatch fails" 1 "" workflow_dispatch main 3.16.0-rigel.1 "$dir/flake.nix"
expect "empty dispatch input fails" 1 "" workflow_dispatch main "" "$dir/flake.nix"
expect "missing flake version fails" 1 "" push v3.17.0-rigel.3 "" /dev/null

[ "$fails" -eq 0 ]
