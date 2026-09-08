#!/usr/bin/env bash
# Start the Paxos replicas inside a single container.
#
# They must share one network namespace: DidaTradeServerState addresses peers
# as "localhost:<base_port + id>", so replicas split across containers would
# never reach each other.
set -euo pipefail

BASE_PORT="${BASE_PORT:-20000}"
SCHEDULE="${SCHEDULE:-A}"
REPLICAS="${REPLICAS:-3}"

# Build once, serially. Three concurrent Maven runs would race on the local
# repository the first time dependencies are resolved.
echo ">> building"
mvn -q -f /work/src/pom.xml install -DskipTests

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

cd /work/src/server
for ((i = 0; i < REPLICAS; i++)); do
    echo ">> replica $i -> localhost:$((BASE_PORT + i)) (schedule $SCHEDULE)"
    mvn -o exec:java -Dexec.args="$BASE_PORT $i $SCHEDULE" 2>&1 | sed "s/^/[r$i] /" &
    pids+=($!)
done

wait
