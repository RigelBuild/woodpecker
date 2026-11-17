#!/usr/bin/env bash
# Usage: build-release-binaries.sh <version> <out-dir>
# Builds the agent and CLI for the flake's three platforms and writes a checksum file.
set -euo pipefail

: "${1:?version required}" "${2:?out dir required}"
version="$1" out="$2"
module=go.woodpecker-ci.org/woodpecker/v3
ldflags="-s -w -X ${module}/version.Version=${version}"
targets=(linux/amd64 linux/arm64 darwin/arm64)

mkdir -p "$out"
for target in "${targets[@]}"; do
  goos="${target%/*}"
  goarch="${target#*/}"
  for cmd in agent cli; do
    stage="$(mktemp -d)"
    CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
      go build -trimpath -ldflags "$ldflags" -tags grpcnotrace \
      -o "$stage/woodpecker-$cmd" "${module}/cmd/$cmd"
    tar -czf "$out/woodpecker-${cmd}_${goos}_${goarch}.tar.gz" -C "$stage" "woodpecker-$cmd"
    rm -rf "$stage"
  done
done
(cd "$out" && sha256sum ./*.tar.gz > checksums.txt)
