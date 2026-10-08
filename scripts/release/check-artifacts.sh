#!/usr/bin/env bash
# Verify release artifacts exist and are built for the right architecture (REQ-0041 AC1).
#
#   scripts/release/check-artifacts.sh --targets x86_64-unknown-linux-gnu,aarch64-unknown-linux-gnu [--dir dist]
#
# For each target: exactly one dist/dscore-*-<target>.tar.gz, a matching .sha256, all three
# binaries inside, each an ELF executable whose machine type matches the target.
set -euo pipefail

targets="" dir="dist"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --targets) targets="$2"; shift 2 ;;
    --dir) dir="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n "$targets" ]] || { echo "--targets is required" >&2; exit 2; }
command -v readelf >/dev/null || { echo "readelf is required" >&2; exit 2; }

machine_for() {
  case "$1" in
    x86_64-*) echo "Advanced Micro Devices X86-64" ;;
    aarch64-*) echo "AArch64" ;;
    *) echo "" ;;
  esac
}

fail=0
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

IFS=, read -ra list <<< "$targets"
for t in "${list[@]}"; do
  want="$(machine_for "$t")"
  [[ -n "$want" ]] || { echo "error: no machine type known for target $t"; fail=1; continue; }
  shopt -s nullglob
  tarballs=("$dir"/dscore-*-"$t".tar.gz)
  shopt -u nullglob
  if [[ ${#tarballs[@]} -ne 1 ]]; then
    echo "error: expected one dscore-*-$t.tar.gz in $dir, found ${#tarballs[@]}"
    fail=1
    continue
  fi
  tarball="${tarballs[0]}"
  if ! (cd "$(dirname "$tarball")" && sha256sum --quiet -c "$(basename "$tarball").sha256"); then
    echo "error: checksum mismatch or missing for $tarball"
    fail=1
  fi
  mkdir -p "$work/$t"
  tar -C "$work/$t" -xzf "$tarball"
  for b in dscore-server dscore-importer dscore-harness; do
    bin="$(find "$work/$t" -type f -name "$b" | head -1)"
    if [[ -z "$bin" ]]; then
      echo "error: $tarball has no $b"
      fail=1
      continue
    fi
    got="$(readelf -h "$bin" | sed -n 's/^ *Machine: *//p')"
    kind="$(readelf -h "$bin" | sed -n 's/^ *Type: *//p')"
    if [[ "$got" != "$want" || "$kind" != DYN* && "$kind" != EXEC* ]]; then
      echo "error: $t/$b is '$kind' for '$got', expected an executable for '$want'"
      fail=1
    else
      echo "ok: $t/$b ($got)"
    fi
  done
done

if [[ $fail -ne 0 ]]; then
  echo "check-artifacts: FAIL"
  exit 1
fi
echo "check-artifacts: ok"
