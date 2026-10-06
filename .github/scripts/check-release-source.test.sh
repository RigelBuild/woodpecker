#!/usr/bin/env bash
# Tests for check-release-source.sh against a scratch repo and a stubbed gh.
set -u
script="$(cd "$(dirname "$0")" && pwd)/check-release-source.sh"
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
fails=0

mkdir -p "$dir/bin" "$dir/repo"
# The stub answers the status query with the state named in $CI_STATE (empty = no status).
cat >"$dir/bin/gh" <<'EOF'
#!/usr/bin/env bash
[ -n "${CI_STATE:-}" ] && echo "$CI_STATE" || echo missing
EOF
chmod +x "$dir/bin/gh"
export PATH="$dir/bin:$PATH"

cd "$dir/repo" || exit 1
git init -q -b main
git -c user.name=t -c user.email=t@t commit -q --allow-empty -m base
release="$(git rev-parse HEAD)"
git update-ref refs/remotes/origin/rigel-release "$release"
git -c user.name=t -c user.email=t@t commit -q --allow-empty -m mirror-only
mirror="$(git rev-parse HEAD)"

expect() {
  local name="$1" want_rc="$2" sha="$3" ci="$4" rc
  CI_STATE="$ci" "$script" "$sha" RigelBuild/woodpecker >/dev/null 2>&1
  rc=$?
  if [ "$rc" != "$want_rc" ]; then
    echo "FAIL $name: rc=$rc (want $want_rc)"
    fails=$((fails + 1))
  else
    echo "ok   $name"
  fi
}

expect "release commit with green CI passes" 0 "$release" success
expect "mirror-only commit fails" 1 "$mirror" success
expect "release commit with failed CI fails" 1 "$release" failure
expect "release commit with pending CI fails" 1 "$release" pending
expect "release commit without CI status fails" 1 "$release" ""

git update-ref -d refs/remotes/origin/rigel-release
expect "missing rigel-release branch fails" 1 "$release" success

[ "$fails" -eq 0 ]
