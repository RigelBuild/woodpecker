#!/usr/bin/env bash
# Usage: check-release-hashes.sh <flake.nix> <out-dir>
# Downloads checksums.txt for the flake's releaseVersion and fails unless every
# releaseAssets hash matches the sha256 that release published for that tarball.
set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${1:?flake.nix path required}" "${2:?out dir required}"
flake="$1" out="$2"

# Exactly one releaseVersion literal, scraped the way release-tag.sh scrapes version.
release_version() {
  local versions=()
  mapfile -t versions < <(sed -n 's/^ *releaseVersion = "\([^"]*\)";$/\1/p' "$flake")
  if [ "${#versions[@]}" -ne 1 ]; then
    echo "::error::expected exactly one flake.nix releaseVersion literal, found ${#versions[@]}" >&2
    return 1
  fi
  printf '%s\n' "${versions[0]}"
}

version="$(release_version)"
checks="$out/checksums.txt"
mkdir -p "$out"
gh release download "v${version}" --repo "$GITHUB_REPOSITORY" --pattern checksums.txt --dir "$out" --clobber

mapfile -t platforms < <(sed -n 's/^ *platform = "\([^"]*\)";$/\1/p' "$flake")
mapfile -t agents < <(sed -n 's/^ *agentHash = "sha256-\([^"]*\)";$/\1/p' "$flake")
mapfile -t clis < <(sed -n 's/^ *cliHash = "sha256-\([^"]*\)";$/\1/p' "$flake")
if [ "${#platforms[@]}" -eq 0 ] || [ "${#agents[@]}" -ne "${#platforms[@]}" ] || [ "${#clis[@]}" -ne "${#platforms[@]}" ]; then
  echo "::error::releaseAssets has ${#platforms[@]} platforms, ${#agents[@]} agent hashes and ${#clis[@]} cli hashes" >&2
  exit 1
fi

status=0
for i in "${!platforms[@]}"; do
  for cmd in agent cli; do
    case "$cmd" in
      agent) sri="${agents[i]}" ;;
      cli) sri="${clis[i]}" ;;
    esac
    name="woodpecker-${cmd}_${platforms[i]}.tar.gz"
    expected="$(printf '%s' "$sri" | base64 -d | od -An -v -tx1 | tr -d ' \n')"
    actual="$(grep -F "  ./${name}" "$checks" | cut -d ' ' -f 1 || true)"
    if [ "$expected" != "$actual" ] || [ -z "$actual" ]; then
      echo "::error::flake hash for ${name} is ${expected}, checksums.txt for v${version} has ${actual:-no entry}" >&2
      status=1
    fi
  done
done
exit "$status"
