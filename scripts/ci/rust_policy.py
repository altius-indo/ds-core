#!/usr/bin/env python3
"""Rust-only and unsafe policy gate (REQ-0048, STORY-0036 E1).

- Fails if first-party C, C++ or Go source exists (REQ-0048 AC1).
- Fails if an `unsafe` block or `unsafe impl` lacks a `// SAFETY:` comment directly above it
  or on the same line (REQ-0048 AC2).
- Reports the count of unsafe blocks per crate and writes it to target/policy/unsafe-count.json.
- With --negative-fixture DIR, also runs the checks on every subdirectory of DIR and fails
  unless each one is rejected, proving the gate still catches seeded violations.
"""
import argparse
import json
import os
import re
import sys

LANG_EXT = {
    "c": (".c", ".h"),
    "cpp": (".cc", ".cpp", ".cxx", ".c++", ".hh", ".hpp", ".hxx", ".h++"),
    "go": (".go",),
}
SKIP_DIRS = {".git", "target", "node_modules"}
DEFAULT_EXCLUDE = ("tests/fixtures",)

UNSAFE_RE = re.compile(r"\bunsafe\s*(\{|impl\b)")
SAFETY_RE = re.compile(r"//\s*SAFETY:")


def walk(root, exclude):
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        dirnames[:] = [
            d for d in dirnames
            if d not in SKIP_DIRS and os.path.normpath(os.path.join(rel, d)) not in exclude
        ]
        for f in filenames:
            yield os.path.join(dirpath, f)


def strip_strings(line):
    """Blank out string and char literals so `unsafe {` inside them is ignored."""
    return re.sub(r'"(?:\\.|[^"\\])*"', '""', line)


def crate_of(path, root):
    d = os.path.dirname(path)
    while True:
        if os.path.isfile(os.path.join(d, "Cargo.toml")):
            return os.path.relpath(d, root) or "."
        if os.path.abspath(d) == os.path.abspath(root) or d == os.path.dirname(d):
            return "."
        d = os.path.dirname(d)


def check(root, forbid, require_safety, exclude):
    errors, counts = [], {}
    exts = tuple(e for lang in forbid for e in LANG_EXT[lang])
    for path in walk(root, exclude):
        rel = os.path.relpath(path, root)
        if exts and path.endswith(exts):
            errors.append(f"{rel}: first-party non-Rust source is forbidden (REQ-0048 AC1)")
            continue
        if not path.endswith(".rs"):
            continue
        with open(path, encoding="utf-8", errors="replace") as fh:
            lines = fh.read().splitlines()
        in_block_comment = False
        for i, raw in enumerate(lines):
            line = raw
            if in_block_comment:
                if "*/" not in line:
                    continue
                line = line.split("*/", 1)[1]
                in_block_comment = False
            if "/*" in line and "*/" not in line.split("/*", 1)[1]:
                in_block_comment = True
                line = line.split("/*", 1)[0]
            code = strip_strings(line.split("//", 1)[0])
            if not UNSAFE_RE.search(code):
                continue
            crate = crate_of(path, root)
            counts[crate] = counts.get(crate, 0) + 1
            if not require_safety:
                continue
            documented = bool(SAFETY_RE.search(raw))
            j = i - 1
            while not documented and j >= 0 and lines[j].strip().startswith(("//", "#[")):
                documented = bool(SAFETY_RE.search(lines[j]))
                j -= 1
            if not documented:
                errors.append(f"{rel}:{i + 1}: unsafe without a `// SAFETY:` comment (REQ-0048 AC2)")
    return errors, counts


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=os.getcwd())
    ap.add_argument("--forbid-langs", default="c,cpp,go")
    ap.add_argument("--require-safety-comments", action="store_true")
    ap.add_argument("--report-unsafe-count", action="store_true")
    ap.add_argument("--negative-fixture", help="dir whose subdirs must each fail the policy")
    args = ap.parse_args()

    forbid = [l for l in args.forbid_langs.split(",") if l]
    unknown = [l for l in forbid if l not in LANG_EXT]
    if unknown:
        ap.error(f"unknown language(s): {', '.join(unknown)}")
    root = os.path.abspath(args.root)
    exclude = {os.path.normpath(e) for e in DEFAULT_EXCLUDE}

    errors, counts = check(root, forbid, args.require_safety_comments, exclude)
    for e in errors:
        print(f"error: {e}")

    if args.report_unsafe_count:
        total = sum(counts.values())
        print(f"unsafe blocks: {total}")
        for crate, n in sorted(counts.items()):
            print(f"  {crate}: {n}")
        out = os.path.join(root, "target", "policy")
        os.makedirs(out, exist_ok=True)
        with open(os.path.join(out, "unsafe-count.json"), "w") as fh:
            json.dump({"total": total, "per_crate": counts}, fh, indent=2, sort_keys=True)

    failed = bool(errors)
    if args.negative_fixture:
        fixture_root = os.path.join(root, args.negative_fixture)
        cases = sorted(d for d in os.listdir(fixture_root) if os.path.isdir(os.path.join(fixture_root, d)))
        if not cases:
            print(f"error: no negative fixtures under {args.negative_fixture}")
            failed = True
        for case in cases:
            case_errors, _ = check(os.path.join(fixture_root, case), forbid, args.require_safety_comments, set())
            if case_errors:
                print(f"negative fixture {case}: rejected as expected ({len(case_errors)} violation(s))")
            else:
                print(f"error: negative fixture {case} passed the policy; the gate is broken")
                failed = True

    print("rust-policy: FAIL" if failed else "rust-policy: ok")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
