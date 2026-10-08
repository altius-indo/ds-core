#!/usr/bin/env bash
# Native-library allow-list gate (REQ-0049). See ffi_allowlist.py.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
exec python3 "$root/scripts/ci/ffi_allowlist.py" --root "$root" "$@"
