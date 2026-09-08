#!/usr/bin/env bash
# Containerized dev environment for dida-trade.
#
# Nothing but a container engine is installed on the host -- no JDK, no Maven.
# Works with podman (the work Mac) or docker (Linux), same commands either way.
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

compose() { "$ENGINE" compose "$@"; }

case "${1:-shell}" in
    build)
        compose build
        ;;
    shell)
        # JDK 22 + Maven, source mounted at /work/src. Build and run by hand
        # here exactly as the README describes -- localhost works because all
        # three replicas would be processes in this one container.
        compose run --rm shell
        ;;
    up)
        # Three replicas, schedule A, ports 20000-20002 published to the host.
        compose up replicas
        ;;
    app)
        compose run --rm app
        ;;
    console)
        compose run --rm console
        ;;
    down)
        compose down
        ;;
    clean)
        # Also drops the cached Maven repository volume.
        compose down -v
        ;;
    *)
        cat <<'USAGE'
usage: ./dev.sh [command]

  build     build the image
  shell     dev shell with JDK 22 + Maven (default)
  up        run the three replicas, ports 20000-20002 on the host
  app       attach an interactive client
  console   attach the admin console (crash/freeze/slow-mode)
  down      stop everything
  clean     stop everything and drop the Maven cache volume

Typical session: ./dev.sh up in one terminal, ./dev.sh console in another.
USAGE
        exit 1
        ;;
esac
