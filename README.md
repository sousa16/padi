# padi

DAD 26/27 project — dida-trade, a trading app replicated with Paxos. Java + gRPC.
The spec is `dida-trade-v1dot0.pdf`.

## Setup

Everything builds and runs in a container. The only thing you need on the host is
Docker or Podman — no JDK, no Maven, no toolchain script.

    ./dev.sh build

The image is multi-arch, so it runs native on Apple Silicon and on Linux. protoc
and the grpc-java plugin are selected per-platform by `os-maven-plugin`, so there
is **no pom to swap** — one `src/contract/pom.xml` works everywhere.

<details>
<summary>Building on the host instead</summary>

Needs Java 22 and Maven 3.8+. `src/setup_env.sh` fetches them, but it is
Linux x86-64 only (it hardcodes `x64` and downloads Linux tarballs).

    cd src && ./setup_env.sh && source dev_env/env.sh
    cd src && mvn clean install

On Apple Silicon this does not work at all: protoc 3.12.0 publishes no
`osx-aarch_64` binary. Use the container.
</details>

## Running

    ./dev.sh up        # Schedule A: replicas 0-2, ports 20000-20002
    ./dev.sh console   # ballot/debug commands (crash, freeze, slow-mode)
    ./dev.sh app       # interactive client

`./dev.sh` on its own lists every command. `up`, `app` and `console` all take a
schedule: `A` (default) and `B` run replicas 0-2, `C` runs 0-6 on ports
20000-20006. Pass the same schedule to all three.

`app` also takes a client id, `./dev.sh app 2` (or `./dev.sh app 2 C`), default 1.
Concurrent clients must not share one: `DidaTradeApp` builds each request id as
`sequence_number * 100 + client_id` and `RequestHistory` dedupes on that int, so
a second client left at id 1 has its requests answered as replays of the first's.
One client is single-threaded -- it blocks in `waitForQuorum` before reading the
next command -- so contention, ballot preemption and Fast-Paxos collisions only
appear with two or more.

Each replica is its own container, so they can be stopped and started
independently -- which is what Step 5 needs, since a state transfer is only
observable when a replica misses decisions and then has to catch up:

    ./dev.sh up C
    ./dev.sh stop 6     # replicas 0-5 keep deciding values
    ./dev.sh start 6    # 6 comes back and must learn what it missed
    ./dev.sh logs 6

All replicas nonetheless share one network namespace, because
`DidaTradeServerState` addresses peers as `localhost:<base_port + id>`. `app`
and `console` join it too, which is why they still pass `localhost`.

## Layout

    src/contract   protos
    src/core       users, orders, matching
    src/configs    Paxos configurations (schedules A/B/C)
    src/util       gRPC response collectors
    src/server     replica — Paxos plus the state machine
    src/app        client
    src/console    ballot/debug commands to replicas
    report/        LaTeX report

    Containerfile        dev/runtime image
    compose.yaml         net namespace, replicas s0-s6, app, console
    dev.sh               entry point for all of it
    scripts/             build and per-replica launchers

## TODO

Going for Premium: steps 1 and 2 by the checkpoint, 3 and 4 by the final.
Checkpoint is 30 Sep, final is 16 Oct.

- [ ] Step 1 — debug modes (crash, freeze, un-freeze, slow-mode) and a real Paxos
      phase 1. The baseline fakes it; start at `PhaseOneBogusProcessor`.
- [ ] Step 2 — Multi-Paxos, still on Schedule A.
- [ ] Step 3 — Fast-Paxos. New on the syllabus this year, so the paper is the only
      real source.
- [ ] Step 4 — `balance` outside Paxos, causally ordered. Needs vector clocks.
- [ ] Step 5 — dynamic membership and state transfer, Schedule B (spec §6).
      Schedule C is wired up too: 7 replicas, and the acceptor set really does
      change ({0,1,2,3,4} → {2,3,4,5,6} at Config3), so it is the harder case to
      test state transfer against.
- [ ] Report, 6 pages. Must include the AI-use declaration (spec §13) and the
      authorship declaration (§14).
