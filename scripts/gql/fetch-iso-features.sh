#!/usr/bin/env bash
# Download the ISO/IEC 39075:2024 optional-feature list into spec/.
# ISO publishes it free, but its licence does not clearly allow redistribution,
# so it is fetched at check time instead of being committed.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
dest="$root/spec/ISO_IEC_39075(en)-features.xml"
url="https://standards.iso.org/iso-iec/39075/ed-1/en/ISO_IEC_39075(en)-features.xml"

mkdir -p "$root/spec"
if [[ ! -s "$dest" ]]; then
  curl -sSfL --retry 3 -o "$dest" "$url"
fi
echo "$dest"
