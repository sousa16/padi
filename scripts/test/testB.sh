#!/usr/bin/env bash
# Schedule B: five Fast Paxos replicas, basic ops, two concurrent clients
# (fast-paxos collisions and the automatic move to the next ballot), then restart.
LOG=/tmp/logsB; rm -rf $LOG
source /work/scripts/test/lib.sh
for i in 0 1 2 3 4; do start $i B; done
sleep 5

echo "== T1 basic ops"
client 1 B "populate 10" "sell 1 50" "buy 2 50" "balance 2" "balance 1"
grep -h "balance is" $LOG/c1.log | tail -2

echo "== T2 two concurrent clients (collisions)"
client 2 B "lenght 15" "looprandom" & c2=$!
client 3 B "lenght 15" "looprandom" & c3=$!
wait $c2 $c3
sleep 2
echo "   collisions: $(grep -h 'collision' $LOG/s*.log | wc -l)"
compare 0 1 2 3 4

echo "== T3 crash replica 2; manual reconfiguration to ballot 100 (leader 0); keep going (single client, so no collisions)"
kill_replica 2
console B 'echo "ballot 100 0"; sleep 6'
grep -h "reply received" $LOG/console.log | tail -1
client 4 B "lenght 10" "looprandom"

echo "== T4 restart replica 2"
start 2 B
sleep 6
client 5 B "sell 3 20" "buy 4 20" "balance 4"
sleep 3
grep -h "Recovery\|State transfer: installed\|abstaining" $LOG/s2.log | sort | uniq -c | head
grep -h "balance is" $LOG/c5.log | tail -1
compare 0 1 2 3 4
if [ "$(executed 2 | wc -l)" -lt "$(executed 0 | wc -l)" ]; then kill -3 "${PID[2]}"; sleep 1; fi
finish
