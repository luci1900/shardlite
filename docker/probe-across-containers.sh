#!/bin/bash
# Stage 0b, Linux items 2 and 3: the -shm check across containers, and /proc/locks.
#
# An app container and a node container share one named volume (and so one
# kernel). Each runs one of the repository's line-protocol helpers, fed through
# a FIFO so it stays alive between commands. Nothing is bind mounted from macOS.
set -u
V=shardlite-xc
IMG=shardlite-linux

cleanup() { docker rm -f xc-app xc-node >/dev/null 2>&1; docker volume rm -f $V >/dev/null 2>&1; }
trap cleanup EXIT
cleanup
docker volume create $V >/dev/null

start() { # name command...
  local n=$1; shift
  docker run -d --name "$n" -v $V:/data $IMG sh -c "mkfifo /tmp/in; : > /tmp/out; $* < /tmp/in > /tmp/out & exec 3>/tmp/in; sleep infinity" >/dev/null
}
ask() { # name line (with \t for tabs)
  local n=$1 before after
  before=$(docker exec "$n" sh -c 'wc -l < /tmp/out')
  printf "$2\n" | docker exec -i "$n" sh -c 'cat > /tmp/in'
  for _ in $(seq 100); do
    after=$(docker exec "$n" sh -c 'wc -l < /tmp/out')
    [ "$after" -gt "$before" ] && break
    sleep 0.1
  done
  docker exec "$n" tail -1 /tmp/out
}
say() { printf '\n== %s\n' "$*"; }

start xc-app testapp
start xc-node testnode -blobs /data/blobs -meta /tmp/meta

say "app container: open the shard and write"
ask xc-app 'open\ta\t/data/x.db'
ask xc-app 'insert\ta\t5\t100'

# A probe from a fresh process that has nothing open on the shard. (Never probe from a
# process that holds a connection: closing the probe's descriptor would drop that
# process's own locks, which is the point of the ordering rule in design.md.)
fresh_probe() { docker run --rm -v $V:/data $IMG sh -c "printf 'probe\\ta\\t/data/x.db\\n' | testapp"; }

say "a third container, with nothing open on the shard, asks across containers: expect held"
fresh_probe
echo "pid of the app process in its own container:           $(docker exec xc-app pgrep -x testapp)"
echo "pid of the app process as seen from the node container: $(docker exec xc-node pgrep -x testapp || echo 'not visible')"

say "node captures the shard with Litestream, then tries to release it while the app still has it open: expect held (capture reopened)"
ask xc-node 'open\tusers/1\t/data/x.db'
ask xc-node 'sync\tusers/1'
ask xc-node 'release\tusers/1'
echo "(capture is open again; a fresh process asks, seeing the app and the node): $(fresh_probe)"

say "/proc/locks, read from the node container without opening the shard's files"
ino=$(docker exec xc-node stat -c %i /data/x.db-shm)
echo "inode of x.db-shm: $ino"
echo "-- /proc/locks lines for that inode, as seen from the node container:"
docker exec xc-node sh -c "grep ':$ino ' /proc/locks || echo '(none)'"
echo "-- and from the app container:"
docker exec xc-app sh -c "grep ':$ino ' /proc/locks || echo '(none)'"
echo "-- all processes with a testapp/testnode name, node container's view:"
docker exec xc-node sh -c 'ps -eo pid,comm | grep -E "testapp|testnode" || true'
echo "-- app container's view:"
docker exec xc-app sh -c 'ps -eo pid,comm | grep -E "testapp|testnode" || true'

say "the app closes its connection"
ask xc-app 'close\ta'
echo "a fresh process asks (the node's Litestream connection is still open in the node container): $(fresh_probe)"
echo "-- /proc/locks for the inode now:"
docker exec xc-node sh -c "grep ':$ino ' /proc/locks || echo '(none)'"

say "node releases the shard now that the app has closed it: expect released"
ask xc-node 'release\tusers/1'
echo "-- /proc/locks for the inode after release:"
docker exec xc-node sh -c "grep ':$ino ' /proc/locks || echo '(none)'"
echo "a fresh process asks: $(fresh_probe)"
