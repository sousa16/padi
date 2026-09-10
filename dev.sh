#!/usr/bin/env bash
# Containerized dev environment for dida-trade.
#
# Nothing but a container engine goes on the host -- no JDK, no Maven. Works
# with podman (the work Mac) or docker (Linux), same commands either way.
set -euo pipefail
cd "$(dirname "$0")"

if command -v podman >/dev/null 2>&1; then
    ENGINE=podman
elif command -v docker >/dev/null 2>&1; then
    ENGINE=docker
else
    echo "Neither podman nor docker found on PATH." >&2
    exit 1
fi

# Profiles are always enabled so that `down`, `ps` and `logs` see the whole
# deployment. Podman refuses to remove the `net` container while any replica
# still shares its namespace, so a `down` that did not know about s3-s6 would
# strand them and then fail. Commands below always name their services, so
# enabling profiles never starts anything extra.
compose() { "$ENGINE" compose --profile extended --profile tools "$@"; }

# Schedules A and B run replicas 0-2; C runs 0-6.
replicas_for() {
    case "$1" in
        A|B) echo "s0 s1 s2" ;;
        C)   echo "s0 s1 s2 s3 s4 s5 s6" ;;
        *)   echo "unknown schedule '$1' (expected A, B or C)" >&2; exit 1 ;;
    esac
}

usage() {
    cat <<'USAGE'
usage: ./dev.sh <command> [args]

  build              build the image and compile the tree
  up [A|B|C]         run a schedule (default A). A/B use 3 replicas, C uses 7
  app [id] [A|B|C]   attach an interactive client (id 1-99, default 1)
  console [A|B|C]    attach the admin console (crash/freeze/slow-mode/reconfig)
  shell              JDK 22 + Maven shell, source at /work/src
  stop <id>          stop one replica, leaving the rest running
  start <id>         bring one replica back
  logs [id]          follow logs, all replicas or one
  ps                 show what is running
  down               stop everything
  clean              stop everything and drop the Maven cache volume

Schedule A, three terminals:
  ./dev.sh up            ./dev.sh console          ./dev.sh app

Concurrent clients need distinct ids. DidaTradeApp builds every request id as
sequence_number * 100 + client_id, and replicas dedupe on that int, so a second
client left at id 1 has its requests dropped as replays of the first's:
  ./dev.sh app          # client 1
  ./dev.sh app 2        # client 2, same schedule

Step 5, state transfer on Schedule C -- stop a replica, let the others decide
values without it, then bring it back and watch it catch up:
  ./dev.sh up C
  ./dev.sh stop 6
  ./dev.sh start 6
USAGE
}

cmd="${1:-}"
[ $# -gt 0 ] && shift || true

case "$cmd" in
    build)
        compose build
        # Serial: seven concurrent reactor builds would race on the shared
        # Maven volume, and replicas start with -o (offline).
        compose run --rm builder
        ;;
    up)
        SCHEDULE="${1:-A}"; export SCHEDULE
        compose up net $(replicas_for "$SCHEDULE")
        ;;
    app)
        # `app 2 C` sets both; `app C` is still just a schedule, since a client
        # id is always numeric.
        CLIENT_ID=1
        case "${1:-}" in
            ''|[A-Za-z]*) ;;
            *[!0-9]*|0|0*) echo "client id must be 1-99, got '$1'" >&2; exit 1 ;;
            *) CLIENT_ID="$1"; shift ;;
        esac
        [ "$CLIENT_ID" -le 99 ] || { echo "client id must be 1-99, got '$CLIENT_ID'" >&2; exit 1; }
        SCHEDULE="${1:-A}"; export SCHEDULE
        # Overrides the service command, which hardcodes id 1.
        compose run --rm app \
            mvn -o exec:java -Dexec.args="$CLIENT_ID localhost ${BASE_PORT:-20000} $SCHEDULE"
        ;;
    console)
        SCHEDULE="${1:-A}"; export SCHEDULE
        compose run --rm console
        ;;
    shell)
        compose run --rm shell
        ;;
    stop)
        compose stop "s${1:?replica id required}"
        ;;
    start)
        compose start "s${1:?replica id required}"
        ;;
    logs)
        if [ $# -gt 0 ]; then compose logs -f "s$1"; else compose logs -f; fi
        ;;
    ps)
        compose ps
        ;;
    down)
        compose down
        ;;
    clean)
        compose down -v
        ;;
    *)
        usage
        [ -z "$cmd" ] && exit 1 || exit 0
        ;;
esac
