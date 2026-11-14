#!/usr/bin/env bash
# Usage: build-release-binaries.sh <version> <out-dir>
# Builds agent and CLI archives for the flake's four systems and writes checksums.
set -euo pipefail

: "${1:?version required}" "${2:?out dir required}"
version="$1" out="$2"
module=go.woodpecker-ci.org/woodpecker/v3
ldflags="-s -w -X ${module}/version.Version=${version}"
targets=(linux/amd64 linux/arm64 darwin/amd64 darwin/arm64)

mkdir -p "$out"
for target in "${targets[@]}"; do
  goos="${target%/*}"
  goarch="${target#*/}"
  for cmd in agent cli; do
    stage="$(mktemp -d)"
    CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
      go build -trimpath -ldflags "$ldflags" \
      -o "$stage/woodpecker-$cmd" "${module}/cmd/$cmd"
    tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
      -cf - -C "$stage" "woodpecker-$cmd" | gzip -n > "$out/woodpecker-${cmd}_${goos}_${goarch}.tar.gz"
    rm -rf "$stage"
  done
done
(cd "$out" && sha256sum ./*.tar.gz > checksums.txt)
