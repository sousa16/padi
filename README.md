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

## Debug modes

`SetDebugRequest` carries a bare int, so the encoding is ours:

| Mode | Name          | Effect                                              |
|------|---------------|-----------------------------------------------------|
| 0    | normal        | clear freeze and slow-mode                          |
| 1    | crash         | replica exits; `./dev.sh start <id>` brings it back |
| 2    | freeze        | hold all client and peer requests until un-freeze   |
| 3    | un-freeze     | release the held requests and resume                |
| 4    | slow-mode-on  | random delay before each client and peer request    |
| 5    | slow-mode-off | stop delaying                                       |

From the console, mode first, then replica:

    console> debug 1 2      # crash replica 2
    console> debug 2 0      # freeze replica 0
    console> debug 3 0      # un-freeze it again

Modes gate `DidaTradeMainServiceImpl` and `DidaTradePaxosServiceImpl` only.
`DidaTradeMasterServiceImpl` is deliberately never gated: a frozen replica must
still be able to receive its own un-freeze (spec §5). Held requests block in
their gRPC handler thread rather than being dropped, so they resume on
un-freeze.

The gate sits on *inbound* RPCs, so freezing a replica stops it acting as
acceptor and learner but does not stop its `MainLoop` from proposing. To stall a
leader, crash it.

No compose service sets a restart policy, so a crashed replica stays down until
`./dev.sh start <id>`.

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

- [x] Step 1 — debug modes (crash, freeze, un-freeze, slow-mode) and a real Paxos
      phase 1. The baseline fakes it; started at `PhaseOneBogusProcessor`, replaced
      by `PhaseOneProcessor`.
- [x] Step 2 — Multi-Paxos, still on Schedule A. One phase 1 per leadership
      change, pipelined phase 2 across instances, strict in-order execution.
- [x] Step 3 — Fast-Paxos. Any-message proposing after the leader's phase 1,
      per-value vote tallying, collision detection, and the automatic
      reconfiguration from spec §4.4 (any replica bumps ballot on a stuck/
      collided fast round, no console needed).
- [x] Step 4 — `balance` outside Paxos, causally ordered. Every write goes
      through one totally ordered log, so the client's causal history is a
      single number: how many log slots it has seen (`seen_slot`, a vector
      clock with one entry). Each write reply carries the slot it executed at;
      `balance` sends `min_slot`, and a replica answers it locally, without
      consensus, only once it has executed that far. A lagging replica waits
      ("balance … waiting for slot N (at M)") rather than answering stale.
- [ ] Step 5 — dynamic membership and state transfer, Schedule B (spec §6).
      Schedule C is wired up too: 7 replicas, and the acceptor set really does
      change ({0,1,2,3,4} → {2,3,4,5,6} at Config3), so it is the harder case to
      test state transfer against.
- [ ] Report, 6 pages. Must include the AI-use declaration (spec §13) and the
      authorship declaration (§14).
