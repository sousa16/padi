# padi

DAD 26/27 project — dida-trade, a trading app replicated with Paxos. Java + gRPC.
The spec is `dida-trade-v1dot0.pdf`.

## Setup

Needs Java 22 and Maven 3.8+. If you don't have them:

    cd src && ./setup_env.sh && source dev_env/env.sh

That pulls a JDK, Maven and protoc into `src/dev_env/`. In practice only the JDK
matters — the build fetches protoc from Maven Central on its own.

On an ARM Mac, swap the contract pom before the first build. The repo ships the
Intel/Linux one:

    cp src/contract/arm-pom.xml src/contract/pom.xml

Then:

    cd src && mvn clean install

## Running

Five minutes of setup, three terminals. Servers first — Schedule A uses three of them.
Base port is arbitrary; server `i` binds `port + i`.

    cd src/server
    mvn exec:java -Dexec.args="20000 0 A"
    mvn exec:java -Dexec.args="20000 1 A"
    mvn exec:java -Dexec.args="20000 2 A"

Client, and the console for admin commands:

    cd src/app     && mvn exec:java -Dexec.args="1 localhost 20000 A"
    cd src/console && mvn exec:java -Dexec.args="localhost 20000 A"

`help` lists the commands in either one.

## Layout

    src/contract   protos
    src/core       users, orders, matching
    src/configs    Paxos configurations (schedules A/B/C)
    src/util       gRPC response collectors
    src/server     replica — Paxos plus the state machine
    src/app        client
    src/console    ballot/debug commands to replicas
    report/        LaTeX report

## TODO

Going for Premium: steps 1 and 2 by the checkpoint, 3 and 4 by the final.
Checkpoint is 30 Sep, final is 16 Oct.

- [ ] Step 1 — debug modes (crash, freeze, un-freeze, slow-mode) and a real Paxos
      phase 1. The baseline fakes it; start at `PhaseOneBogusProcessor`.
- [ ] Step 2 — Multi-Paxos, still on Schedule A.
- [ ] Step 3 — Fast-Paxos. New on the syllabus this year, so the paper is the only
      real source.
- [ ] Step 4 — `balance` outside Paxos, causally ordered. Needs vector clocks.
- [ ] Step 5 — dynamic membership and state transfer, Schedule B. Stretch goal.
- [ ] Report, 6 pages. Must include the AI-use declaration (spec §13) and the
      authorship declaration (§14).
