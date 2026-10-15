#!/usr/bin/env bash
# Schedule A: basic ops, two concurrent clients (fast-paxos collisions and the
# automatic move to the next ballot), then crash + restart of a replica.
LOG=/tmp/logsA; rm -rf $LOG
source /work/scripts/test/lib.sh
for i in 0 1 2; do start $i A; done
sleep 5

echo "== T1 basic ops"
client 1 A "populate 10" "sell 1 50" "buy 2 50" "balance 2" "balance 1"
grep -h "balance is" $LOG/c1.log | tail -2

echo "== T2 two concurrent clients (collisions)"
client 2 A "lenght 15" "looprandom" & c2=$!
client 3 A "lenght 15" "looprandom" & c3=$!
wait $c2 $c3
sleep 2
echo "   collisions: $(grep -h 'collision' $LOG/s*.log | wc -l), stall bumps: $(grep -h 'sem decisão' $LOG/s*.log | wc -l)"
compare 0 1 2

echo "== T3 crash replica 2, keep going, restart it"
kill_replica 2
client 4 A "lenght 10" "looprandom"
start 2 A
sleep 4
client 5 A "sell 3 20" "buy 4 20" "balance 4"
sleep 3
grep -h "Recovery\|State transfer: installed\|abstaining" $LOG/s2.log | sort | uniq -c | head
grep -h "balance is" $LOG/c5.log | tail -1
compare 0 1 2
if [ "$(executed 2 | wc -l)" -lt "$(executed 0 | wc -l)" ]; then kill -3 "${PID[2]}"; sleep 1; fi
finish
