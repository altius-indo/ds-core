#!/usr/bin/env bash
# Rust-only and unsafe policy gate (REQ-0048). See rust_policy.py.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
exec python3 "$root/scripts/ci/rust_policy.py" --root "$root" "$@"
