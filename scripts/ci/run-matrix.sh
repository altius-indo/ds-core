#!/usr/bin/env bash
# Run a test suite on this host for one of the supported architectures (REQ-0041 AC2).
#
#   scripts/ci/run-matrix.sh --arch amd64,arm64 --suite release
#
# CI runs this once per architecture on a native runner (see .github/workflows/ci.yml, job
# `suite`); STORY-0028 E2 passes when every listed architecture's job passes. The script fails
# if the host is not one of the requested architectures, so a misconfigured runner cannot pass
# for an architecture it is not.
#
# Suites:
#   release  everything a release must pass: workspace tests in release mode and both policy
#            gates. The Jepsen-style suite joins it when dscore-harness can drive a cluster
#            (TASK-0005).
#   quick    workspace tests only.
set -euo pipefail

arches="" suite="release"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --arch) arches="$2"; shift 2 ;;
    --suite) suite="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n "$arches" ]] || { echo "--arch is required (e.g. amd64,arm64)" >&2; exit 2; }

case "$(uname -m)" in
  x86_64|amd64) host=amd64 ;;
  aarch64|arm64) host=arm64 ;;
  *) echo "unsupported host architecture: $(uname -m)" >&2; exit 1 ;;
esac
[[ "$(uname -s)" == "Linux" ]] || echo "warning: host is $(uname -s); REQ-0041 targets Linux" >&2

if [[ ",$arches," != *",$host,"* ]]; then
  echo "host architecture $host is not in --arch $arches" >&2
  exit 1
fi

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
echo "== suite $suite on linux/$host"

case "$suite" in
  quick)
    cargo test --workspace --locked
    ;;
  release)
    cargo test --workspace --locked --release
    scripts/ci/rust-policy.sh --forbid-langs c,cpp,go --require-safety-comments --report-unsafe-count \
      --negative-fixture tests/fixtures/rust-policy
    scripts/ci/ffi-allowlist.sh deny.native.toml --link-audit rustc-link-lib,readelf \
      --negative-fixture tests/fixtures/unlisted-native --require owner,purpose,licence,version,reviewed
    ;;
  *) echo "unknown suite: $suite" >&2; exit 2 ;;
esac
echo "== suite $suite on linux/$host: ok"
