#!/usr/bin/env bash
# run.sh <testA|testB|testC>  -- runs inside the dev container (see README).
# Logs are left in /tmp/logs* (inside the container).
set -u
cd /work/scripts/test
exec bash "/work/scripts/test/$1.sh"
