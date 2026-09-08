#!/usr/bin/env bash
# One-shot serial build into the shared Maven volume.
set -euo pipefail
exec mvn -f /work/src/pom.xml install -DskipTests
