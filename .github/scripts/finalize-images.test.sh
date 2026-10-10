#!/usr/bin/env bash
# Test digest-to-tag finalization using a deterministic docker stub.
set -u
script="$(cd "$(dirname "$0")" && pwd)/finalize-images.sh"
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
mkdir -p "$dir/bin"
cat >"$dir/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$DOCKER_LOG"
if [ "$1" = buildx ] && [ "$2" = imagetools ] && [ "$3" = inspect ]; then
  case "$4" in
    *woodpecker-agent*) printf '%s\n' "$AGENT_DIGEST" ;;
    *woodpecker-server*) printf '%s\n' "$SERVER_DIGEST" ;;
    *) exit 2 ;;
  esac
fi
EOF
chmod +x "$dir/bin/docker"
export PATH="$dir/bin:$PATH"
export DOCKER_LOG="$dir/docker.log"
export GITHUB_STEP_SUMMARY="$dir/summary"
export VERSION=3.17.0-rigel.9
export COMMIT_SHA=0123456789abcdef0123456789abcdef01234567
export AGENT_DIGEST=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export SERVER_DIGEST=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
: >"$DOCKER_LOG"
: >"$GITHUB_STEP_SUMMARY"

"$script"
if [ "$?" -ne 0 ]; then echo 'FAIL finalize succeeds for matching manifest digests'; exit 1; fi
[ "$(wc -l <"$DOCKER_LOG")" -eq 8 ] || { echo 'FAIL both tags are created and inspected per image'; exit 1; }
[ "$(wc -l <"$GITHUB_STEP_SUMMARY")" -eq 4 ] || { echo 'FAIL summary records every tag pin'; exit 1; }
grep -q 'woodpecker-agent:3.17.0-rigel.9@sha256:aaaa' "$GITHUB_STEP_SUMMARY" || { echo 'FAIL agent version digest summary'; exit 1; }
grep -q 'woodpecker-agent:sha-0123456789abcdef0123456789abcdef01234567@sha256:aaaa' "$GITHUB_STEP_SUMMARY" || { echo 'FAIL agent commit digest summary'; exit 1; }
grep -q 'woodpecker-server:3.17.0-rigel.9@sha256:bbbb' "$GITHUB_STEP_SUMMARY" || { echo 'FAIL server version digest summary'; exit 1; }
grep -q 'woodpecker-server:sha-0123456789abcdef0123456789abcdef01234567@sha256:bbbb' "$GITHUB_STEP_SUMMARY" || { echo 'FAIL server commit digest summary'; exit 1; }
echo 'ok   both immutable digests receive version and commit tags'
echo 'ok   all four tag@digest pins appear in the summary'
