#!/usr/bin/env python3
"""Check docs/gql-conformance.toml against ISO/IEC 39075's feature list (STORY-0010 E1).

usage: check-matrix.py MATRIX --against FEATURES_XML [--require-status]

Fails unless the matrix lists exactly the standard's optional features, once each, with the
standard's names, and (with --require-status) every entry has status supported, partial or
unsupported (REQ-0003 AC1) and a non-empty reason. Get the XML with
scripts/gql/fetch-iso-features.sh.
"""
import argparse
import sys
import tomllib
import xml.etree.ElementTree as ET

STATUSES = {"supported", "partial", "unsupported"}


def iso_features(path):
    root = ET.parse(path).getroot()
    out = {}
    for f in root.findall("feature"):
        desc = f.find("description")
        out[f.findtext("code")] = " ".join("".join(desc.itertext()).split())
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("matrix")
    ap.add_argument("--against", required=True)
    ap.add_argument("--require-status", action="store_true")
    a = ap.parse_args()

    with open(a.matrix, "rb") as fh:
        rows = tomllib.load(fh).get("feature", [])
    iso = iso_features(a.against)
    errors = []
    ids = [r.get("id") for r in rows]
    for dup in sorted({i for i in ids if ids.count(i) > 1}):
        errors.append(f"{dup}: listed more than once")
    for missing in sorted(set(iso) - set(ids)):
        errors.append(f"{missing}: in ISO/IEC 39075 but missing from the matrix")
    for extra in sorted(set(ids) - set(iso)):
        errors.append(f"{extra}: not an ISO/IEC 39075 feature")
    for r in rows:
        fid = r.get("id")
        if fid in iso and r.get("name") != iso[fid]:
            errors.append(f"{fid}: name {r.get('name')!r} differs from the standard's {iso[fid]!r}")
        if a.require_status:
            if r.get("status") not in STATUSES:
                errors.append(f"{fid}: status {r.get('status')!r} is not one of {sorted(STATUSES)}")
            if not str(r.get("reason", "")).strip():
                errors.append(f"{fid}: no reason given for its status")
    for e in errors:
        print(f"error: {e}")
    counts = {s: sum(1 for r in rows if r.get("status") == s) for s in sorted(STATUSES)}
    print(f"gql matrix: {len(rows)} features ({', '.join(f'{k} {v}' for k, v in counts.items())})")
    print("check-matrix: FAIL" if errors else "check-matrix: ok")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
