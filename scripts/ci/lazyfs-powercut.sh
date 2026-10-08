#!/usr/bin/env bash
# STORY-0001 E2 (REQ-0016 AC1): power-cut durability on a LazyFS mount. Linux only.
#
#   scripts/ci/lazyfs-powercut.sh [TRIALS]      (default 1000)
#
# Builds LazyFS (pinned), mounts it, then runs dscore-harness twice:
#   1. self-check: with fsync disabled, the harness must observe lost writes;
#   2. the eval: with fsync, TRIALS power cuts must lose zero acknowledged writes.
# Needs: g++ cmake libfuse3-dev fuse3, and `user_allow_other` in /etc/fuse.conf.
set -euo pipefail

trials="${1:-1000}"
version="0.3.1"
root="$(cd "$(dirname "$0")/../.." && pwd)"
lazyfs="${LAZYFS_HOME:-$HOME/.cache/lazyfs-$version}"
mnt=/tmp/dscore-lazyfs.mnt
backing=/tmp/dscore-lazyfs.root
fifo=/tmp/dscore-faults.fifo
done_fifo=/tmp/dscore-faults-done.fifo
config=/tmp/dscore-lazyfs.toml

if [[ ! -e "$lazyfs/lazyfs/build/lazyfs" ]]; then
  rm -rf "$lazyfs"
  git clone --quiet --depth 1 --branch "$version" https://github.com/dsrhaslab/lazyfs "$lazyfs"
  (cd "$lazyfs/libs/libpcache" && ./build.sh > /dev/null)
  (cd "$lazyfs/lazyfs" && ./build.sh > /dev/null)
fi

cleanup() {
  fusermount3 -u "$mnt" 2>/dev/null || fusermount -u "$mnt" 2>/dev/null || true
  rm -f "$fifo" "$done_fifo"
}
trap cleanup EXIT
cleanup
mkdir -p "$mnt" "$backing"
rm -rf "${backing:?}"/*
mkfifo "$fifo" "$done_fifo"
cat > "$config" <<EOF
[faults]
fifo_path="$fifo"
fifo_path_completed="$done_fifo"
[cache]
apply_eviction=false
[cache.simple]
custom_size="2gb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile=""
EOF

# The mount script finds the binary relative to its working directory (./build/lazyfs).
(cd "$lazyfs/lazyfs" && ./scripts/mount-lazyfs.sh -c "$config" -m "$mnt" -r "$backing") \
  > /tmp/dscore-lazyfs.log 2>&1 &
for _ in $(seq 1 100); do
  mountpoint -q "$mnt" && break
  sleep 0.1
done
mountpoint -q "$mnt" || { echo "LazyFS did not mount"; cat /tmp/dscore-lazyfs.log; exit 1; }

cargo build --quiet --release --locked -p dscore-harness --manifest-path "$root/Cargo.toml"
harness="$root/target/release/dscore-harness"
common=(fault power-cut --data-root "$mnt" --lazyfs-fifo "$fifo" --lazyfs-done-fifo "$done_fifo")

echo "== self-check: fsync disabled, losses expected"
"$harness" "${common[@]}" --trials 5 --writes 50 --unsafe-no-fsync --expect-loss

echo "== E2: $trials power cuts, zero acknowledged writes may be lost"
"$harness" "${common[@]}" --trials "$trials" --assert-zero-loss
