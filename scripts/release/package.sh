#!/usr/bin/env bash
# Build release binaries for one target and package them into dist/ (REQ-0041 AC1).
#
#   scripts/release/package.sh x86_64-unknown-linux-gnu
#
# Produces dist/dscore-<version>-<target>.tar.gz and its .sha256.
set -euo pipefail

target="${1:?usage: package.sh <rust-target-triple>}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"

version="$(cargo metadata --format-version 1 --no-deps --locked \
  | python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="dscore-server"))')"
bins=(dscore-server dscore-importer dscore-harness)

cargo build --release --locked --target "$target" "${bins[@]/#/--bin=}"

name="dscore-$version-$target"
stage="target/dist/$name"
rm -rf "$stage" && mkdir -p "$stage" dist
for b in "${bins[@]}"; do
  cp "target/$target/release/$b" "$stage/"
done
cp README.md "$stage/"
tar -C target/dist -czf "dist/$name.tar.gz" "$name"
(cd dist && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
echo "dist/$name.tar.gz"
