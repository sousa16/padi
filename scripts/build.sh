#!/usr/bin/env bash
# One-shot serial build into the shared Maven volume.
#
# Replicas start with `mvn -o` (offline) so that seven of them coming up at once
# neither race on the local repository nor need the network. `install` alone is
# not enough to make that work: it never invokes exec:java, so the exec plugin
# and its dependencies would be absent from the repository and every replica
# would fail to start offline. Resolve it here, while we still have a network.
set -euo pipefail

EXEC_PLUGIN_VERSION="$(sed -n 's|.*<version.exec-maven-plugin>\(.*\)</version.exec-maven-plugin>.*|\1|p' \
    /work/src/server/pom.xml | head -1)"
: "${EXEC_PLUGIN_VERSION:?could not read exec-maven-plugin version from server/pom.xml}"

mvn -f /work/src/pom.xml install -DskipTests
mvn -f /work/src/pom.xml -q "org.codehaus.mojo:exec-maven-plugin:${EXEC_PLUGIN_VERSION}:help" >/dev/null

echo ">> build complete (exec-maven-plugin ${EXEC_PLUGIN_VERSION} cached for offline start)"
