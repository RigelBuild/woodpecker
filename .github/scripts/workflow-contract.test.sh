#!/usr/bin/env bash
# Static acceptance checks for publisher hygiene and release-tag requirement 1.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
publisher="$root/.github/workflows/publish-images.yaml"
release="$root/.github/workflows/release-tag.yaml"

fail() {
  echo "FAIL $1" >&2
  exit 1
}
pass() { echo "ok   $1"; }

[ -f "$publisher" ] || fail 'publish-images.yaml exists'
[ -f "$release" ] || fail 'release-tag.yaml exists'
[ ! -e "$root/.github/workflows/publish-agent-image.yml" ] || fail 'old agent publisher removed'
[ ! -e "$root/.github/workflows/publish-server-image.yaml" ] || fail 'old server publisher removed'
if grep -Eq '^[[:space:]]*on:' "$publisher" "$release"; then fail 'workflow on key is quoted'; fi
[ "$(yq -r '.on.push.tags[0]' "$publisher")" = 'v*.*.*-rigel.*' ] || fail 'publisher tag trigger'
[ "$(yq -r '.on.workflow_dispatch.inputs.version.required' "$publisher")" = true ] || fail 'publisher dispatch version required'
[ "$(yq -r '.concurrency.group' "$publisher")" = '${{ github.workflow }}-${{ github.ref }}' ] || fail 'publisher concurrency group'
[ "$(yq -r '.concurrency["cancel-in-progress"]' "$publisher")" = false ] || fail 'publisher concurrency does not cancel'
[ "$(yq -r '.jobs.resolve.steps | map(select(.name == "Require release ancestry")) | length' "$publisher")" = 1 ] || fail 'publisher ancestry guard exists'
[ "$(yq -r '.on | keys | length' "$publisher")" = 2 ] || fail 'publisher has only push and workflow_dispatch triggers'
[ "$(yq -r '.jobs.resolve.steps | map(select(.name == "Resolve and validate version")) | .[0].env.REF' "$publisher")" = '${{ github.ref }}' ] || fail 'publisher passes full event ref'
for job in agent-publish server-publish; do
  [ "$(yq -r ".jobs.${job}.permissions.packages" "$publisher")" = write ] || fail "$job alone publishes with package permission"
done
[ "$(yq -r '.jobs.finalize.permissions.contents' "$publisher")" = write ] || fail 'finalize can create GitHub Release'
[ "$(yq -r '.jobs.agent-rehearsal.permissions.packages // "read"' "$publisher")" = read ] || fail 'agent rehearsal cannot write packages'
[ "$(yq -r '.jobs.server-rehearsal.permissions.packages // "read"' "$publisher")" = read ] || fail 'server rehearsal cannot write packages'
[ "$(yq -r '.jobs.agent-rehearsal.steps | map(select(.uses | test("docker/login-action")) ) | length' "$publisher")" = 0 ] || fail 'agent rehearsal has no registry login'
[ "$(yq -r '.jobs.server-rehearsal.steps | map(select(.uses | test("docker/login-action")) ) | length' "$publisher")" = 0 ] || fail 'server rehearsal has no registry login'
for job in agent-publish agent-rehearsal server-publish server-rehearsal; do
  platforms="$(yq -r ".jobs.${job}.steps | map(select(.uses | test(\"docker/build-push-action\"))) | .[0].with.platforms" "$publisher")"
  [ "$platforms" = 'linux/amd64,linux/arm64' ] || fail "$job builds both required platforms"
done
[ "$(yq -r '.jobs.agent-publish.steps | map(select(.uses | test("docker/build-push-action"))) | .[0].with.file' "$publisher")" = docker/Dockerfile.agent.multiarch ] || fail 'agent tag build uses upstream Dockerfile'
[ "$(yq -r '.jobs.agent-rehearsal.steps | map(select(.uses | test("docker/build-push-action"))) | .[0].with.file' "$publisher")" = docker/Dockerfile.agent.multiarch ] || fail 'agent rehearsal uses upstream Dockerfile'
[ "$(yq -r '.jobs.server-publish.steps | map(select(.uses | test("docker/build-push-action"))) | .[0].with.file' "$publisher")" = docker/Dockerfile.server.multiarch.rootless ] || fail 'server tag build uses upstream Dockerfile'
[ "$(yq -r '.jobs.server-rehearsal.steps | map(select(.uses | test("docker/build-push-action"))) | .[0].with.file' "$publisher")" = docker/Dockerfile.server.multiarch.rootless ] || fail 'server rehearsal uses upstream Dockerfile'
[ "$(yq -r '.jobs.agent-rehearsal.steps | map(select(.uses | test("docker/build-push-action"))) | .[0].with.push' "$publisher")" = false ] || fail 'agent rehearsal does not push'
[ "$(yq -r '.jobs.server-rehearsal.steps | map(select(.uses | test("docker/build-push-action"))) | .[0].with.push' "$publisher")" = false ] || fail 'server rehearsal does not push'
[ "$(yq -r '.jobs.agent-rehearsal.if' "$publisher")" = "github.event_name == 'workflow_dispatch'" ] || fail 'agent rehearsal is dispatch-only'
[ "$(yq -r '.jobs.server-rehearsal.if' "$publisher")" = "github.event_name == 'workflow_dispatch'" ] || fail 'server rehearsal is dispatch-only'
[ "$(yq -r '.jobs.finalize.if' "$publisher")" = "github.event_name == 'push'" ] || fail 'finalize is tag-push-only'
[ "$(yq -r '.jobs.finalize.needs | sort | join(",")' "$publisher")" = 'agent-publish,resolve,server-publish' ] || fail 'finalize needs both image digests and resolve'
for job in agent-publish server-publish; do
  [ "$(yq -r ".jobs.${job}.steps | map(select(.uses | test(\"docker/build-push-action\"))) | .[0].with.push" "$publisher")" = true ] || fail "$job pushes its manifest"
  [ "$(yq -r ".jobs.${job}.steps | map(select(.uses | test(\"docker/build-push-action\"))) | .[0].with.outputs" "$publisher" | grep -c 'push-by-digest=true')" = 1 ] || fail "$job pushes by digest"
done
pass 'publisher build, rehearsal, and finalize contracts'
while IFS= read -r line; do
  if [[ ! "$line" =~ uses:[[:space:]]+[^[:space:]#]+@[0-9a-f]{40}[[:space:]]+#\ v[^[:space:]]+[[:space:]]*$ ]]; then
    fail "third-party action is not SHA-pinned with a version trailer: $line"
  fi
done < <(grep -h 'uses:' "$publisher" "$release")
if grep -Eq 'docker/setup-qemu-action|docker/setup-qemu' "$publisher"; then fail 'publisher omits QEMU'; fi
pass 'publisher hygiene and rehearsal permissions'

[ "$(yq -r '.on.push.branches[0]' "$release")" = rigel-release ] || fail 'release mint triggers on rigel-release push'
[ "$(yq -r '.on | has("workflow_dispatch")' "$release")" = true ] || fail 'release mint supports workflow dispatch'
[ "$(yq -r '.jobs.mint.steps | map(select(.name == "Checkout")) | .[0].with.fetch-depth' "$release")" = 0 ] || fail 'mint checkout fetches full history and tags'
[ "$(yq -r '.jobs.check.steps | map(select(.name == "Validate a changed release version")) | length' "$release")" = 1 ] || fail 'PR check validates changed flake versions'
 [ "$(yq -r '.jobs.check.permissions.contents' "$release")" = read ] || fail 'release check has contents read only'
[ "$(yq -r '.permissions.contents' "$release")" = read ] || fail 'release GITHUB_TOKEN has contents read only'
[ "$(yq -r '.jobs.mint.steps | map(select(.uses | test("actions/create-github-app-token"))) | length' "$release")" = 1 ] || fail 'mint takes App token on every run'
[ "$(yq -r '.jobs.mint.steps | map(select(.name == "Require release-branch dispatch")) | length' "$release")" = 1 ] || fail 'mint rejects arbitrary dispatch refs'
[ "$(yq -r '.jobs.mint.steps | map(select(.name == "Create scoped release App token")) | .[0].with.client-id' "$release")" = '${{ vars.AUTOMATION_APP_CLIENT_ID }}' ] || fail 'mint uses T3 App client ID'
[ "$(yq -r '.jobs.mint.steps | map(select(.name == "Create scoped release App token")) | .[0].with.private-key' "$release")" = '${{ secrets.AUTOMATION_APP_PRIVATE_KEY }}' ] || fail 'mint uses T3 App private key'
[ "$(yq -r '.jobs.mint.steps | map(select(.name == "Create scoped release App token")) | .[0].with.permission-contents' "$release")" = write ] || fail 'mint token is contents write'
[ "$(yq -r '.jobs.mint.steps | map(select(.name == "Create scoped release App token")) | .[0].with.repositories' "$release")" = woodpecker ] || fail 'mint token is scoped to woodpecker'
[ "$(yq -r '.jobs.mint.concurrency // "none"' "$release")" = none ] || fail 'mint has no concurrency group'
pass 'release-tag Requirement 1 workflow shape'
