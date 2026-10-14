#!/usr/bin/env bash
# Schedule C: dynamic membership. Acceptors {0..4} -> {2..6} at Config 2.
LOG=/tmp/logsC; rm -rf $LOG
source /work/scripts/test/lib.sh

for i in 0 1 2 3 4 5 6; do start $i C; done
sleep 6

echo "== C1 ballot 0 (fast, acceptors 0-4)"
client 1 C "populate 10" "sell 1 50" "buy 2 50" "lenght 8" "looprandom"
compare 0 1 2 3 4 5 6

echo "== C2 console: ballot 2 -> leader 3, acceptor set changes"
console C 'echo "ballot 2 3"; sleep 8'
grep -h "reply received" $LOG/console.log
grep -h "acceptor set changed\|is complete\|leader 3 of ballot" $LOG/s*.log | sort | uniq -c

echo "== C3 replicas 0 and 1 leave; keep working in ballot 2"
kill_replica 0; kill_replica 1
client 2 C "lenght 8" "looprandom" "balance 2"
compare 2 3 4 5 6

echo "== C4 console: ballot 3 -> leader 4 (classic), told ballot 2 is complete"
console C 'echo "ballot 2 3"; sleep 3; echo "ballot 3 4"; sleep 8'
grep -h "reply received" $LOG/console.log | tail -2
grep -h "Fase 1 a partir" $LOG/s4.log | tail -1
client 3 C "lenght 8" "looprandom"
compare 2 3 4 5 6

echo "== C5 replica 6 crashes, misses decisions, comes back"
kill_replica 6
client 4 C "lenght 6" "looprandom"
start 6 C
sleep 5
client 5 C "sell 3 10" "buy 4 10" "balance 4"
sleep 3
lastlife 6 | grep -h "Recovery\|State transfer: installed" | head -3
compare 2 3 4 5 6
finish
