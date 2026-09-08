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

    ./dev.sh up        # three replicas, schedule A, ports 20000-20002
    ./dev.sh console   # ballot/debug commands (crash, freeze, slow-mode)
    ./dev.sh app       # interactive client

`help` lists the commands in the app or the console. `./dev.sh` on its own lists
every command; `./dev.sh down` stops things.

All replicas run inside a single container on purpose. `DidaTradeServerState`
addresses peers as `localhost:<base_port + id>`, so they have to share one
network namespace; `app` and `console` join it too, which is why they still pass
`localhost` in their arguments.

<details>
<summary>Running by hand, three terminals</summary>

`./dev.sh shell` gives you a shell with JDK 22 and Maven and the source mounted
at `/work/src`. Inside it, the original flow works as-is — base port is
arbitrary, server `i` binds `port + i`:

    cd /work/src/server
    mvn exec:java -Dexec.args="20000 0 A"
    mvn exec:java -Dexec.args="20000 1 A"
    mvn exec:java -Dexec.args="20000 2 A"

    cd /work/src/app     && mvn exec:java -Dexec.args="1 localhost 20000 A"
    cd /work/src/console && mvn exec:java -Dexec.args="localhost 20000 A"
</details>

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
    compose.yaml         replicas + app + console
    dev.sh               entry point for all of it
    scripts/             replica launcher

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
