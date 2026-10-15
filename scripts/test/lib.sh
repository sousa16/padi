#!/usr/bin/env bash
# Shared helpers: replicas, clients and console as plain java processes in one
# container, so they all share localhost like the compose setup does.
cd /work/src
if [ ! -f /tmp/cp_server.txt ]; then
  mvn -q -o -f server/pom.xml  dependency:build-classpath -Dmdep.outputFile=/tmp/cp_server.txt  >/dev/null 2>&1 || mvn -q -f server/pom.xml dependency:build-classpath -Dmdep.outputFile=/tmp/cp_server.txt
  mvn -q -o -f app/pom.xml     dependency:build-classpath -Dmdep.outputFile=/tmp/cp_app.txt     >/dev/null 2>&1 || mvn -q -f app/pom.xml dependency:build-classpath -Dmdep.outputFile=/tmp/cp_app.txt
  mvn -q -o -f console/pom.xml dependency:build-classpath -Dmdep.outputFile=/tmp/cp_console.txt >/dev/null 2>&1 || mvn -q -f console/pom.xml dependency:build-classpath -Dmdep.outputFile=/tmp/cp_console.txt
fi
CP_SERVER="server/target/classes:$(cat /tmp/cp_server.txt)"
CP_APP="app/target/classes:$(cat /tmp/cp_app.txt)"
CP_CONSOLE="console/target/classes:$(cat /tmp/cp_console.txt)"
LOG=${LOG:-/tmp/logs}
mkdir -p "$LOG"
declare -A PID

start() {   # start <id> <schedule>
  java -cp "$CP_SERVER" didatrade.server.DidaTradeServer 20000 "$1" "$2" >>"$LOG/s$1.log" 2>&1 &
  PID[$1]=$!
}
kill_replica() { kill -9 "${PID[$1]}"; echo ">> killed replica $1"; }

client() {  # client <id> <schedule> <commands...>  (one command per arg)
  local id=$1 s=$2; shift 2
  { for c in "$@"; do echo "$c"; done; echo exit; } \
    | timeout 120 java -cp "$CP_APP" didatrade.app.DidaTradeApp "$id" localhost 20000 "$s" >>"$LOG/c$id.log" 2>&1
  echo ">> client $id done (rc=$?)"
}

console() { # console <schedule> <script with sleeps, run in a subshell>
  local s=$1; shift
  ( eval "$@"; echo exit ) | timeout 120 java -cp "$CP_CONSOLE" didatrade.console.Console localhost 20000 "$s" >>"$LOG/console.log" 2>&1
}

# Sequence of executed (instance, command) per replica, to compare them. Only
# the replica's last life counts: a restarted replica replays from instance 0.
lastlife() { local f="$LOG/s$1.log"; awk -v k="$(grep -c 'Recovery:' "$f")" '/Recovery:/{n++} n>=k' "$f"; }
executed() { lastlife "$1" | grep -o "Executar instância decidida [0-9]* (Command ID = [0-9-]*)" | sed 's/Executar instância decidida //'; }

compare() { # compare <ids...>: every replica's executed log must be a prefix of the longest
  local ref="" best=0
  for i in "$@"; do n=$(executed $i | wc -l); echo "   replica $i executed $n commands"; if [ "$n" -gt "$best" ]; then best=$n; ref=$i; fi; done
  local ok=1
  for i in "$@"; do
    n=$(executed $i | wc -l)
    if ! diff <(executed $i) <(executed $ref | head -n "$n") >/dev/null; then echo "   MISMATCH replica $i vs $ref"; ok=0; fi
  done
  [ $ok = 1 ] && echo "   OK: executed sequences agree" || echo "   FAIL: executed sequences diverge"
}

finish() { for p in "${PID[@]}"; do kill -9 "$p" 2>/dev/null; done; wait 2>/dev/null; }
