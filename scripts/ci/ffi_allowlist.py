#!/usr/bin/env python3
"""Native-library allow-list gate (REQ-0049, STORY-0036 E2).

usage: ffi_allowlist.py ALLOWLIST [--link-audit rustc-link-lib,readelf]
                        [--negative-fixture DIR] [--require owner,purpose,...]

Checks, in order:
  1. Every allow-list entry has the required fields and an exact `=` version pin (AC2).
  2. Every resolved crate with a Cargo `links` key is on the list, at the pinned version (AC1).
  3. rustc-link-lib: every library a build script asks rustc to link is covered by an entry's
     `link_names` or by [system].
  4. readelf: every shared library a built executable needs is covered likewise
     (readelf -d on Linux, otool -L on macOS; skipped with a warning if neither exists).
With --negative-fixture, the same checks run on that Cargo package and must FAIL.
"""
import argparse
import datetime
import fnmatch
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tomllib


def load_allowlist(path, required):
    with open(path, "rb") as fh:
        doc = tomllib.load(fh)
    errors = []
    libs = doc.get("library", [])
    for i, lib in enumerate(libs):
        label = lib.get("name") or f"library[{i}]"
        for field in required:
            if not str(lib.get(field, "")).strip():
                errors.append(f"allow-list {label}: missing `{field}` (REQ-0049 AC2)")
        for field in ("crate", "links"):
            if not lib.get(field):
                errors.append(f"allow-list {label}: missing `{field}`")
        version = str(lib.get("version", ""))
        if version and not re.fullmatch(r"=\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.+-]+)?", version):
            errors.append(f"allow-list {label}: version `{version}` is not an exact `=x.y.z` pin (REQ-0049 AC1)")
        reviewed = lib.get("reviewed")
        if reviewed:
            try:
                day = datetime.date.fromisoformat(str(reviewed))
                if (datetime.date.today() - day).days > 366:
                    print(f"warning: allow-list {label}: last reviewed {day}, over a year ago")
            except ValueError:
                errors.append(f"allow-list {label}: reviewed `{reviewed}` is not an ISO date")
    system = doc.get("system", {})
    return libs, system.get("link_names", []), system.get("shared", []), errors


def matches(name, patterns):
    return any(fnmatch.fnmatchcase(name, p) for p in patterns)


def link_name(spec):
    """'static:+whole-archive=foo:bar' -> 'foo'."""
    name = spec.split("=", 1)[1] if "=" in spec else spec
    return name.split(":", 1)[0]


# Platforms DS-CORE ships for (REQ-0041), checked alongside the host.
RELEASE_TARGETS = ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu")


def host_target():
    out = subprocess.run(["rustc", "-vV"], check=True, capture_output=True, text=True).stdout
    return next(l.split(": ", 1)[1] for l in out.splitlines() if l.startswith("host: "))


def built_packages(manifest):
    """(name, version) of every package compiled for the host or a release target.

    `cargo metadata` lists optional dependencies whose features are never enabled (e.g.
    rustls-webpki's `ring`) and dependencies for other platforms (e.g. wasm-bindgen), so the
    set comes from `cargo tree`, which applies real feature resolution per target.
    """
    built = set()
    for platform in sorted({host_target(), *RELEASE_TARGETS}):
        out = subprocess.run(
            ["cargo", "tree", "--locked", "--workspace", "-e", "normal,build,dev", "--target", platform,
             "--prefix", "none", "--format", "{p}", "--manifest-path", manifest],
            check=True, capture_output=True, text=True,
        ).stdout
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1].startswith("v"):
                built.add((parts[0], parts[1][1:]))
    return built


def check_links_keys(manifest, libs):
    meta = json.loads(subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked", "--manifest-path", manifest],
        check=True, capture_output=True, text=True,
    ).stdout)
    built = built_packages(manifest)
    errors = []
    for pkg in meta["packages"]:
        if not pkg.get("links") or (pkg["name"], pkg["version"]) not in built:
            continue
        entry = next((l for l in libs if l.get("links") == pkg["links"] and l.get("crate") == pkg["name"]), None)
        if entry is None:
            errors.append(f"{pkg['name']} {pkg['version']} links native `{pkg['links']}`, which is not on the allow-list")
        elif str(entry.get("version", "")).lstrip("=") != pkg["version"]:
            errors.append(f"{pkg['name']} is {pkg['version']}, allow-list pins {entry.get('version')}")
    return errors


def build_messages(manifest, target_dir, targets):
    env = dict(os.environ, CARGO_TARGET_DIR=target_dir)
    proc = subprocess.run(
        ["cargo", "build", "--locked", *targets, "--message-format=json", "--manifest-path", manifest],
        capture_output=True, text=True, env=env,
    )
    msgs = [json.loads(l) for l in proc.stdout.splitlines() if l.startswith("{")]
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"error: cargo build failed for {manifest}")
    return msgs


def audit_rustc_link_lib(msgs, allowed):
    errors = []
    for m in msgs:
        if m.get("reason") != "build-script-executed":
            continue
        for spec in m.get("linked_libs", []):
            name = link_name(spec)
            if not matches(name, allowed):
                errors.append(f"{m['package_id']} build script links `{spec}`, not on the allow-list")
    return errors


def needed_libs(path):
    if shutil.which("readelf"):
        out = subprocess.run(["readelf", "-d", path], capture_output=True, text=True).stdout
        return re.findall(r"\(NEEDED\)\s+Shared library: \[([^\]]+)\]", out)
    if platform.system() == "Darwin" and shutil.which("otool"):
        out = subprocess.run(["otool", "-L", path], capture_output=True, text=True).stdout
        return [l.strip().split(" (", 1)[0] for l in out.splitlines()[1:] if l.strip()]
    return None


def audit_readelf(msgs, allowed_names, shared):
    errors, warned = [], False
    patterns = list(shared)
    for n in allowed_names:
        patterns += [f"lib{n}.so*", f"*/lib{n}.dylib", f"*/lib{n}.*.dylib"]
    for m in msgs:
        exe = m.get("executable") if m.get("reason") == "compiler-artifact" else None
        if not exe:
            continue
        libs = needed_libs(exe)
        if libs is None:
            if not warned:
                print("warning: neither readelf nor otool found; skipping shared-library audit")
                warned = True
            continue
        for lib in libs:
            if not matches(lib, patterns) and not matches(os.path.basename(lib), patterns):
                errors.append(f"{os.path.basename(exe)} needs shared library `{lib}`, not on the allow-list")
    return errors


def run_checks(manifest, libs, sys_names, sys_shared, audits, target_dir, targets):
    allowed = sys_names + [n for l in libs for n in l.get("link_names", [])]
    errors = check_links_keys(manifest, libs)
    if audits:
        msgs = build_messages(manifest, target_dir, targets)
        if "rustc-link-lib" in audits:
            errors += audit_rustc_link_lib(msgs, allowed)
        if "readelf" in audits:
            errors += audit_readelf(msgs, allowed, sys_shared)
    return errors


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("allowlist")
    ap.add_argument("--root", default=os.getcwd())
    ap.add_argument("--link-audit", default="rustc-link-lib,readelf")
    ap.add_argument("--negative-fixture")
    ap.add_argument("--require", default="owner,purpose,licence,version,reviewed")
    args = ap.parse_args()

    root = os.path.abspath(args.root)
    audits = {a for a in args.link_audit.split(",") if a}
    bad = audits - {"rustc-link-lib", "readelf"}
    if bad:
        ap.error(f"unknown --link-audit value(s): {', '.join(sorted(bad))}")
    required = [f for f in args.require.split(",") if f]
    libs, sys_names, sys_shared, errors = load_allowlist(os.path.join(root, args.allowlist), required)
    policy_target = os.path.join(root, "target", "policy")

    errors += run_checks(os.path.join(root, "Cargo.toml"), libs, sys_names, sys_shared,
                         audits, os.path.join(root, "target"), ["--workspace", "--all-targets"])
    for e in errors:
        print(f"error: {e}")
    failed = bool(errors)

    if args.negative_fixture:
        fixture = os.path.join(root, args.negative_fixture)
        # Build only the lib: the fixture's library does not exist, so nothing may link it.
        fx_errors = run_checks(os.path.join(fixture, "Cargo.toml"), libs, sys_names, sys_shared,
                               audits, os.path.join(policy_target, "negative-fixture"), ["--lib"])
        if fx_errors:
            print(f"negative fixture {args.negative_fixture}: rejected as expected:")
            for e in fx_errors:
                print(f"  {e}")
        else:
            print(f"error: negative fixture {args.negative_fixture} passed; the gate is broken")
            failed = True

    print("ffi-allowlist: FAIL" if failed else "ffi-allowlist: ok")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
