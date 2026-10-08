#!/usr/bin/env python3
"""Probe a TLS port with hand-built ClientHello messages (REQ-0035, STORY-0021 E1).

A probe passes when the server answers the way the policy requires, independent of what the
local OpenSSL is willing to offer:

  sslv3, tls1.0, tls1.1   legacy-version ClientHello            -> must NOT get a ServerHello
  non-aead                TLS 1.2 ClientHello, CBC/RC4/3DES only  -> must NOT get a ServerHello
  aead-tls12              TLS 1.2 ClientHello, AEAD suites        -> must get a TLS 1.2 ServerHello
  plaintext               an HTTP request in the clear            -> no plaintext reply; closed

usage: tls_probe.py HOST PORT PROBE...
"""
import os
import socket
import struct
import sys

TIMEOUT = 5.0

LEGACY = {"sslv3": 0x0300, "tls1.0": 0x0301, "tls1.1": 0x0302}
NON_AEAD = [  # CBC, RC4 and 3DES suites
    0xC013, 0xC014, 0xC009, 0xC00A, 0xC027, 0xC028, 0xC023, 0xC024,
    0x002F, 0x0035, 0x003C, 0x003D, 0x000A, 0x0005, 0x0004,
]
AEAD = [0xC02B, 0xC02C, 0xC02F, 0xC030, 0xCCA8, 0xCCA9]
SIG_ALGS = [0x0403, 0x0503, 0x0804, 0x0805, 0x0401, 0x0501]
GROUPS = [0x001D, 0x0017, 0x0018]


def ext(kind, body):
    return struct.pack("!HH", kind, len(body)) + body


def client_hello(version, suites):
    body = struct.pack("!H", version) + os.urandom(32) + b"\x00"
    body += struct.pack("!H", 2 * len(suites)) + b"".join(struct.pack("!H", s) for s in suites)
    body += b"\x01\x00"  # null compression only
    exts = b""
    if version >= 0x0301:
        groups = b"".join(struct.pack("!H", g) for g in GROUPS)
        sigs = b"".join(struct.pack("!H", s) for s in SIG_ALGS)
        exts += ext(0x000A, struct.pack("!H", len(groups)) + groups)  # supported_groups
        exts += ext(0x000B, b"\x01\x00")  # ec_point_formats
        exts += ext(0x000D, struct.pack("!H", len(sigs)) + sigs)  # signature_algorithms
        exts += ext(0x0017, b"")  # extended_master_secret
        exts += ext(0xFF01, b"\x00")  # renegotiation_info
    if exts:
        body += struct.pack("!H", len(exts)) + exts
    handshake = b"\x01" + struct.pack("!I", len(body))[1:] + body
    record_version = min(version, 0x0301)
    return b"\x16" + struct.pack("!HH", record_version, len(handshake)) + handshake


def recv_some(sock, n=4096):
    try:
        return sock.recv(n)
    except (ConnectionResetError, socket.timeout, OSError):
        return b""


def server_hello_version(host, port, version, suites):
    """Version in the ServerHello, or None if the server did not send one."""
    with socket.create_connection((host, port), timeout=TIMEOUT) as s:
        s.sendall(client_hello(version, suites))
        data = b""
        while len(data) < 11:
            chunk = recv_some(s)
            if not chunk:
                break
            data += chunk
    # record: type(1) version(2) len(2); handshake: type(1) len(3) version(2)
    if len(data) >= 11 and data[0] == 0x16 and data[5] == 0x02:
        return struct.unpack("!H", data[9:11])[0]
    return None


def plaintext_refused(host, port):
    with socket.create_connection((host, port), timeout=TIMEOUT) as s:
        s.sendall(b"GET / HTTP/1.1\r\nHost: dscore\r\n\r\n")
        reply = b""
        while True:
            chunk = recv_some(s)
            if not chunk:
                break
            reply += chunk
            if len(reply) > 65536:
                break
    # Allowed: silence or a TLS alert record (0x15). Not allowed: any plaintext answer.
    return reply == b"" or reply[0] == 0x15, reply[:40]


def run(host, port, probe):
    if probe in LEGACY:
        v = server_hello_version(host, port, LEGACY[probe], AEAD + NON_AEAD)
        return v is None, f"ServerHello version {v:#06x}" if v else "refused"
    if probe == "non-aead":
        v = server_hello_version(host, port, 0x0303, NON_AEAD)
        return v is None, f"negotiated a non-AEAD suite (ServerHello {v:#06x})" if v else "refused"
    if probe == "aead-tls12":
        v = server_hello_version(host, port, 0x0303, AEAD)
        return v == 0x0303, "TLS 1.2 ServerHello" if v == 0x0303 else f"no TLS 1.2 ServerHello ({v})"
    if probe == "plaintext":
        ok, head = plaintext_refused(host, port)
        return ok, "refused" if ok else f"plaintext reply {head!r}"
    raise SystemExit(f"unknown probe {probe}")


def main():
    if len(sys.argv) < 4:
        raise SystemExit(__doc__)
    host, port, probes = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
    failed = False
    for p in probes:
        ok, detail = run(host, port, p)
        print(f"{'ok  ' if ok else 'FAIL'} {host}:{port} {p}: {detail}")
        failed |= not ok
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
