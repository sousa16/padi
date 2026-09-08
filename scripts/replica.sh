#!/usr/bin/env bash
# Run one replica. Its id is the single argument.
#
# Assumes the tree is already built -- `dev.sh build` runs Maven once, serially,
# because seven concurrent reactor builds would race on the shared local
# repository volume. Hence -o (offline) here.
set -euo pipefail

ID="${1:?replica id required}"
BASE_PORT="${BASE_PORT:-20000}"
SCHEDULE="${SCHEDULE:-A}"

echo ">> replica $ID -> localhost:$((BASE_PORT + ID)) (schedule $SCHEDULE)"
cd /work/src/server
exec mvn -o exec:java -Dexec.args="$BASE_PORT $ID $SCHEDULE"
