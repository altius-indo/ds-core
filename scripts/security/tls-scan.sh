#!/usr/bin/env bash
# TLS policy scan of the client and peer ports (REQ-0035, STORY-0021 E1).
#
#   scripts/security/tls-scan.sh --ports client,peer --forbid sslv3,tls1.0,tls1.1,non-aead \
#       --expect-plaintext-refused
#
# Starts a dscore-server with throwaway certificates, then for each port:
#   - each --forbid probe must be refused (scripts/security/tls_probe.py, raw ClientHellos);
#   - a TLS 1.2 AEAD ClientHello must be answered (the scan is not passing by accident);
#   - a TLS 1.3 handshake must succeed with openssl s_client (with a cluster client cert on
#     the peer port);
#   - with --expect-plaintext-refused, a cleartext request gets no cleartext reply.
set -euo pipefail

ports="client,peer" forbid="sslv3,tls1.0,tls1.1,non-aead" plaintext=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ports) ports="$2"; shift 2 ;;
    --forbid) forbid="$2"; shift 2 ;;
    --expect-plaintext-refused) plaintext=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# Throwaway cluster CA and node certificate (ECDSA P-256).
cd "$work"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
  -keyout ca.key -out ca.pem -subj "/CN=dscore scan CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout node.key -out node.csr \
  -subj "/CN=node1" 2>/dev/null
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\n' > ext.cnf
openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile ext.cnf -out node.pem 2>/dev/null
openssl pkcs8 -topk8 -nocrypt -in node.key -out node.pk8

cargo build -q --locked -p dscore-server --manifest-path "$root/Cargo.toml"
"$root/target/debug/dscore-server" serve --client-addr 127.0.0.1:0 --peer-addr 127.0.0.1:0 \
  --tls-cert node.pem --tls-key node.pk8 --cluster-ca ca.pem > server.out 2> server.err &
server_pid=$!
for _ in $(seq 1 100); do
  grep -q '^listening' server.out 2>/dev/null && break
  sleep 0.1
done
line="$(grep '^listening' server.out || true)"
[[ -n "$line" ]] || { echo "server did not start:"; cat server.err; exit 1; }
client_addr="$(sed -E 's/.*client=([^ ]+).*/\1/' <<< "$line")"
peer_addr="$(sed -E 's/.*peer=([^ ]+).*/\1/' <<< "$line")"

fail=0
IFS=, read -ra port_list <<< "$ports"
IFS=, read -ra forbid_list <<< "$forbid"
for p in "${port_list[@]}"; do
  case "$p" in
    client) addr="$client_addr"; cert_args=() ;;
    peer) addr="$peer_addr"; cert_args=(-cert node.pem -key node.key) ;;
    *) echo "unknown port $p"; exit 2 ;;
  esac
  host="${addr%:*}" port="${addr##*:}"
  probes=("${forbid_list[@]}" aead-tls12)
  [[ $plaintext -eq 1 ]] && probes+=(plaintext)
  python3 "$root/scripts/security/tls_probe.py" "$host" "$port" "${probes[@]}" || fail=1

  # Judge the handshake by its output, not s_client's exit status: some OpenSSL builds exit
  # non-zero when the server closes right after the handshake.
  out="$(openssl s_client -connect "$addr" -tls1_3 -CAfile ca.pem -verify_return_error \
        -servername localhost ${cert_args[@]+"${cert_args[@]}"} < /dev/null 2>&1 || true)"
  if grep -qE "(Protocol *: *TLSv1\.3|New, TLSv1\.3)" <<< "$out" \
     && grep -q "Verify return code: 0 (ok)" <<< "$out"; then
    echo "ok   $addr tls1.3 handshake ($p port)"
  else
    echo "FAIL $addr tls1.3 handshake ($p port); openssl output:"
    sed 's/^/    /' <<< "$out" | tail -20
    fail=1
  fi
done

if [[ $fail -ne 0 ]]; then
  echo "tls-scan: FAIL"
  exit 1
fi
echo "tls-scan: ok"
