package didatrade.server;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseOneProcessor;
import didatrade.configs.ConfigurationScheduler;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class MainLoop implements Runnable {
    // Valor proposto para buracos no log recuperado na Fase 1 (no-op)
    static final int NOOP = -1;

    DidaTradeServerState server_state;
    private boolean has_work;
    private final Object proposal_lock = new Object();

    // Contadores separados para permitir o Pipelining (Multi-Paxos)
    private volatile int next_instance_to_propose;
    private int next_instance_to_execute;

    // Variáveis cruciais para o Multi-Paxos
    private int phase1_completed_ballot;
    // Tocado pelo main loop e pelas threads do aceitador (noteVote), logo concorrente
    private Set<Integer> proposed_requests;
    // Fast-Paxos: ballot rápido em que este servidor já está a propor, e o que propôs/viu votar por instância
    private volatile int fast_proposer_ballot;
    private Map<Integer, Integer> fast_marks;
    private Set<Integer> fast_pending_values;
    private int catch_up_requested_instance;

    public MainLoop(DidaTradeServerState state) {
        this.server_state = state;
        this.has_work = false;
        this.next_instance_to_propose = 0;
        this.next_instance_to_execute = 0;
        this.phase1_completed_ballot = -1;
        this.proposed_requests = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        this.fast_proposer_ballot = -1;
        this.fast_marks = new ConcurrentHashMap<>();
        this.fast_pending_values = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        this.catch_up_requested_instance = -1;
    }

    public void run() {
        // 0. Step 5: antes de prometer ou votar, pergunta aos pares em que ponto está o sistema
        recover();

        while (true) {
            // 1. Executa as instâncias que já atingiram o consenso
            settleFastMarks();
            executeDecided();

            // 2. Step 5: se ficámos para trás (buraco no log, comando em falta), pede o estado aos pares
            catchUpIfStuck();

            // 3. Tenta propor novos pedidos pendentes rapidamente (Multi-Paxos)
            proposePending();

            synchronized (this) {
                if (!this.has_work) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                this.has_work = false;
            }
        }
    }

    /**
     * Step 5, recuperação. Uma réplica que reinicia perdeu o log, o estado da aplicação e,
     * pior, as promessas e votos que fez como aceitador. Pede a cada par as posições decididas
     * (com os comandos), e se o sistema já estava em andamento abstém-se de todos os ballots
     * até ao maior que os pares reportam: pode ter votado neles antes de cair e, sem memória,
     * podia agora votar noutro valor. Num arranque normal os pares não têm nada e não há restrição.
     */
    private void recover() {
        List<Integer> peers = new ArrayList<Integer>();
        for (int participant : this.server_state.all_participants) {
            if (participant != this.server_state.my_id) {
                peers.add(participant);
            }
        }

        DidaTradePaxos.FetchStateRequest request = DidaTradePaxos.FetchStateRequest.newBuilder()
                .setFromInstance(0).setServerid(this.server_state.my_id).build();
        int peer_ballot = -1;
        boolean in_progress = false;
        if (!peers.isEmpty()) {
            ArrayList<DidaTradePaxos.FetchStateReply> replies = new ArrayList<>();
            GenericResponseCollector<DidaTradePaxos.FetchStateReply> collector = new GenericResponseCollector<>(replies, peers.size());
            for (int peer : peers) {
                this.server_state.async_stubs[peer].withWaitForReady()
                        .fetchstate(request, new CollectorStreamObserver<>(collector));
            }
            collector.waitUntilDone();

            synchronized (collector) {
                for (DidaTradePaxos.FetchStateReply reply : replies) {
                    this.server_state.installDecided(reply.getCommandsList());
                    peer_ballot = Math.max(peer_ballot, reply.getBallot());
                    in_progress |= reply.getBallot() > 0 || reply.getExecuted() > 0 || reply.getHasVotes() || reply.getCommandsCount() > 0;
                }
            }
        }

        if (in_progress) {
            this.server_state.setCurrentBallot(peer_ballot);
            System.out.println("Recovery: rejoining at ballot " + peer_ballot + ", abstaining as acceptor up to it");
            this.server_state.finishRecovery(peer_ballot);
        } else {
            System.out.println("Recovery: fresh start, nothing to recover");
            this.server_state.finishRecovery(-1);
        }
    }

    /**
     * Step 5, transferência de estado a pedido. A execução está parada se a instância seguinte
     * não está decidida mas há outras decididas depois dela, ou se falta o pedido do cliente.
     * Pede o estado aos pares uma vez for cada instância em que a execução fica bloqueada.
     */
    private void catchUpIfStuck() {
        int next = this.next_instance_to_execute;
        PaxosInstance entry = this.server_state.paxos_log.getEntry(next);
        boolean gap = (entry == null || !entry.decided) && this.server_state.getMaxDecided() > next;
        boolean missing_command = entry != null && entry.decided && entry.decided_value != NOOP
                && this.server_state.req_history.getIfExists(entry.decided_value) == null;
        if (!gap && !missing_command) {
            this.catch_up_requested_instance = -1;
            return;
        }

        if (this.catch_up_requested_instance != next) {
            this.catch_up_requested_instance = next;
            System.out.println("State transfer: stuck at instance " + next + ", asking peers for the decided log");
            DidaTradePaxos.FetchStateRequest request = DidaTradePaxos.FetchStateRequest.newBuilder()
                    .setFromInstance(next).setServerid(this.server_state.my_id).build();
            for (int peer : this.server_state.all_participants) {
                if (peer == this.server_state.my_id) {
                    continue;
                }
                this.server_state.async_stubs[peer].withWaitForReady()
                        .fetchstate(request, new StreamObserver<DidaTradePaxos.FetchStateReply>() {
                            @Override public void onNext(DidaTradePaxos.FetchStateReply reply) {
                                server_state.installDecided(reply.getCommandsList());
                            }
                            @Override public void onError(Throwable t) { }
                            @Override public void onCompleted() { }
                        });
            }
        }
    }

    public synchronized void wakeup() {
        this.has_work = true;
        notifyAll();
    }

    private boolean isLeader(int ballot) {
        return ballot >= 0 && this.server_state.scheduler.leader(ballot) == this.server_state.my_id;
    }

    private void proposePending() {
        int ballot = this.server_state.getCurrentBallot();
        boolean leader = isLeader(ballot);
        boolean fast = this.server_state.scheduler.fastpaxos(ballot);
        // FAST-PAXOS: num ballot rápido qualquer servidor propõe, mas só depois do "any" do líder
        if (!leader && !(fast && this.server_state.getFastAnyBallot() == ballot)) {
            return; // Se não sou o líder (nem proponente rápido), não proponho nada
        }

        // Saímos de um ballot rápido (colisão): as propostas dele não contam para o
        // novo ballot, a Fase 1 recupera o que ficou votado e o resto volta a ser proposto
        if (this.fast_proposer_ballot != -1 && this.fast_proposer_ballot != ballot) {
            this.proposed_requests.clear();
            this.fast_marks.clear();
            this.fast_pending_values.clear();
            this.fast_proposer_ballot = -1;
        }

        // MULTI-PAXOS: a Fase 1 corre uma única vez por ballot e cobre todas as instâncias
        // Corre logo, mesmo sem pedidos: só no fim da Fase 1 (e da transferência de estado) o
        // ballot fica completo, e a consola espera por isso para avançar a configuração
        if (leader && ballot > this.phase1_completed_ballot) {
            if (!runPhaseOne(ballot)) {
                return;
            }
            if (fast) {
                // Recuperadas as instâncias antigas, abre todas as seguintes a qualquer proponente
                this.fast_marks.clear();
                this.fast_proposer_ballot = ballot;
                sendAny(ballot, this.next_instance_to_propose);
            }
        }
        if (fast && this.fast_proposer_ballot != ballot) {
            enterFastBallot(ballot);
        }
        if (fast && this.server_state.getFastAnyBallot() != ballot) {
            return;
        }

        while (this.server_state.getCurrentBallot() == ballot) {
            RequestRecord request_record = this.server_state.req_history.getFirstUnproposed(this.proposed_requests);
            if (request_record == null) {
                break; // Acabaram-se os pedidos por propor
            }

            this.proposed_requests.add(request_record.getId());
            startProposalThread(ballot, fast, request_record.getId());
        }
    }

    /**
     * Give every client command its own proposer thread. Multi-Paxos still
     * assigns each command a unique slot; which thread reserves first determines
     * the proposal order. Fast Paxos lets each command race at the acceptors.
     */
    private void startProposalThread(int ballot, boolean fast, int request_id) {
        Thread proposer = new Thread(() -> {
            if (this.server_state.getCurrentBallot() != ballot) {
                return;
            }
            if (fast) {
                if (this.server_state.getFastAnyBallot() == ballot) {
                    sendFastProposal(ballot, request_id);
                }
                return;
            }

            int reserved_instance;
            synchronized (this.proposal_lock) {
                if (this.server_state.getCurrentBallot() != ballot) {
                    return;
                }
                reserved_instance = this.next_instance_to_propose;
                PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(reserved_instance);
                while (entry.decided) {
                    reserved_instance++;
                    entry = this.server_state.paxos_log.testAndSetEntry(reserved_instance);
                }
                this.next_instance_to_propose = reserved_instance + 1;
            }
            sendPhaseTwo(reserved_instance, ballot, request_id);
        }, "paxos-proposal-" + ballot + "-" + request_id);
        proposer.setDaemon(true);
        proposer.start();
    }

    /**
     * Fast-Paxos: servidor que não é o líder entra num ballot rápido. Começa na primeira
     * instância aberta pelo "any" e não volta a propor pedidos que outros já têm em votação.
     */
    private void enterFastBallot(int ballot) {
        this.proposed_requests.clear();
        this.fast_marks.clear();
        this.fast_pending_values.clear();
        this.next_instance_to_propose = this.server_state.getFastAnyFrom();
        this.fast_proposer_ballot = ballot;
        for (PaxosInstance entry : this.server_state.paxos_log.snapshotFrom(this.next_instance_to_propose)) {
            if (entry.write_ballot == ballot && !entry.decided) {
                noteVote(entry.instance_nb, ballot, entry.command_id);
            }
        }
    }

    /**
     * Fast-Paxos: instâncias já decididas deixam de estar "em voo". Se foram decididas com
     * outro valor, o pedido que propus (ou vi votar) aí perdeu e tem de poder ser proposto de novo.
     */
    private void settleFastMarks() {
        for (Map.Entry<Integer, Integer> mark : this.fast_marks.entrySet()) {
            PaxosInstance entry = this.server_state.paxos_log.getEntry(mark.getKey());
            if (entry != null && entry.decided) {
                this.fast_marks.remove(mark.getKey());
                this.fast_pending_values.remove(mark.getValue());
                if (mark.getValue() != entry.decided_value) {
                    this.proposed_requests.remove(mark.getValue());
                }
            }
        }
    }

    /** Chamado pelo aceitador quando vota: o pedido já está a caminho, não o proponho de novo. */
    void noteVote(int instance, int ballot, int value) {
        if (value == NOOP || ballot != this.fast_proposer_ballot) {
            return;
        }
        this.proposed_requests.add(value);
        this.fast_marks.put(instance, value);
        this.next_instance_to_propose = Math.max(this.next_instance_to_propose, instance + 1);
    }

    /** Fast-Paxos: o líder diz a todos os aceitadores/proponentes que as instâncias >= from estão abertas. */
    private void sendAny(int ballot, int from) {
        System.out.println("Fast-Paxos: a enviar \"any\" a partir da instância " + from + " (Ballot " + ballot + ")");
        DidaTradePaxos.PhaseTwoRequest any_req = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(from).setRequestballot(ballot).setAny(true).build();
        for (int target : this.server_state.scheduler.acceptors(ballot)) {
            this.server_state.async_stubs[target].withWaitForReady().phasetwo(any_req, new StreamObserver<DidaTradePaxos.PhaseTwoReply>() {
                @Override public void onNext(DidaTradePaxos.PhaseTwoReply reply) { }
                @Override public void onError(Throwable t) { }
                @Override public void onCompleted() { }
            });
        }
    }

    /**
     * Fase 1 do Multi-Paxos: um único prepare para todas as instâncias a partir de
     * next_instance_to_execute. Cada aceitador devolve o seu log a partir daí; para cada
     * posição, o líder volta a propor (Fase 2, com o novo ballot) o valor com o maior ballot.
     * Posições sem valor em nenhuma resposta são preenchidas com no-ops.
     */
    private boolean runPhaseOne(int ballot) {
        int from = this.next_instance_to_execute;
        System.out.println("Multi-Paxos: A correr Fase 1 a partir da instância " + from + " (Ballot " + ballot + ")");

        int low_ballot = Math.max(this.server_state.getCompletedBallot(), 0);
        List<Integer> phase_one_acceptors = this.server_state.scheduler.acceptorsinrange(low_ballot, ballot);
        PhaseOneProcessor phase_one_processor = new PhaseOneProcessor(this.server_state.scheduler, low_ballot, ballot);
        ArrayList<DidaTradePaxos.PhaseOneReply> phase_one_responses = new ArrayList<>();
        GenericResponseCollector<DidaTradePaxos.PhaseOneReply> phase_one_collector =
                new GenericResponseCollector<>(phase_one_responses, phase_one_acceptors.size(), phase_one_processor);

        DidaTradePaxos.PhaseOneRequest p1_req = DidaTradePaxos.PhaseOneRequest.newBuilder()
                .setInstance(from).setRequestballot(ballot).build();

        for (int target : phase_one_acceptors) {
            CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> obs = new CollectorStreamObserver<>(phase_one_collector);
            this.server_state.async_stubs[target].withWaitForReady().phaseone(p1_req, obs);
        }
        phase_one_collector.waitUntilDone();

        if (!phase_one_processor.getAccepted()) {
            System.out.println("Multi-Paxos: Fase 1 rejeitada para o ballot " + ballot);
            int max_ballot = phase_one_processor.getMaxballot();
            this.server_state.setCurrentBallot(max_ballot);
            if (max_ballot > ballot) {
                wakeup();
            }
            return false;
        }

        // Junta os logs recebidos: para cada posição, os votos de cada aceitador que prometeu
        Map<Integer, List<DidaTradePaxos.LogEntry>> votes_by_position = new HashMap<Integer, List<DidaTradePaxos.LogEntry>>();
        // Um valor que algum aceitador sabe estar decidido é final, seja qual for o ballot
        Map<Integer, Integer> decided_values = new HashMap<Integer, Integer>();
        Set<Integer> promised_acceptors = new HashSet<Integer>();
        synchronized (phase_one_collector) {
            for (DidaTradePaxos.PhaseOneReply reply : phase_one_responses) {
                if (!reply.getAccepted()) {
                    continue;
                }
                promised_acceptors.add(reply.getServerid());
                for (DidaTradePaxos.LogEntry log_entry : reply.getLogList()) {
                    if (log_entry.getDecided()) {
                        decided_values.put(log_entry.getPosition(), log_entry.getCommandId());
                    }
                    List<DidaTradePaxos.LogEntry> votes = votes_by_position.get(log_entry.getPosition());
                    if (votes == null) {
                        votes = new ArrayList<DidaTradePaxos.LogEntry>();
                        votes_by_position.put(log_entry.getPosition(), votes);
                    }
                    votes.add(log_entry);
                }
            }
        }
        Map<Integer, Integer> merged_value = new HashMap<Integer, Integer>();
        for (Map.Entry<Integer, List<DidaTradePaxos.LogEntry>> position : votes_by_position.entrySet()) {
            Integer decided = decided_values.get(position.getKey());
            merged_value.put(position.getKey(), decided != null ? decided : chooseValue(position.getValue(), promised_acceptors));
        }

        int last = from - 1;
        for (Integer position : merged_value.keySet()) {
            last = Math.max(last, position);
        }

        this.phase1_completed_ballot = ballot;
        this.next_instance_to_propose = from;
        // Os pedidos pendentes que não aparecerem no log recuperado voltam a ser propostos
        this.proposed_requests.clear();

        // Step 5: o ballot só fica completo quando cada valor recuperado foi aceite por um quórum
        // dos aceitadores deste ballot. Por isso repropõe também as posições que já sabemos
        // decididas: os aceitadores novos de uma reconfiguração podem não as ter.
        CompletionTracker completion = new CompletionTracker(ballot, last - from + 1);
        for (int instance = from; instance <= last; instance++) {
            PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
            Integer value = entry.decided ? Integer.valueOf(entry.decided_value) : merged_value.get(instance);
            if (value == null) {
                value = NOOP;
            }
            if (value != NOOP) {
                this.proposed_requests.add(value);
            }
            System.out.println("Multi-Paxos: A repropor instância " + instance + " (Command ID = " + value + ")");
            sendPhaseTwo(instance, ballot, value, completion);
        }
        this.next_instance_to_propose = last + 1;
        transferState(ballot, low_ballot, completion);
        return true;
    }

    /**
     * Step 5, Vertical Paxos. Se o conjunto de aceitadores mudou desde o último ballot completo
     * (Schedule C, Config 2), os aceitadores novos não sabem nada do que foi decidido antes de
     * `from`, e um líder futuro que só lhes pergunte a eles podia escolher outro valor numa
     * posição já decidida. Por isso o líder empurra-lhes o log decidido (com os comandos) e só
     * dá o ballot por completo quando um quórum confirmou.
     */
    private void transferState(int ballot, int low_ballot, CompletionTracker completion) {
        Set<Integer> previous_acceptors = new HashSet<Integer>(this.server_state.scheduler.acceptorsinrange(low_ballot, ballot));
        List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
        if (previous_acceptors.equals(new HashSet<Integer>(acceptors))) {
            completion.transferDone(); // mesmos aceitadores: não há nada a transferir
            return;
        }

        List<DidaTradePaxos.DecidedCommand> commands = this.server_state.decidedCommandsFrom(0);
        System.out.println("State transfer: acceptor set changed for ballot " + ballot + ", pushing " + commands.size() + " decided positions to " + acceptors);
        DidaTradePaxos.InstallStateRequest request = DidaTradePaxos.InstallStateRequest.newBuilder()
                .setBallot(ballot).setServerid(this.server_state.my_id).addAllCommands(commands).build();
        int quorum = this.server_state.scheduler.quorum(ballot);
        AtomicInteger acks = new AtomicInteger(0);
        for (int target : acceptors) {
            this.server_state.async_stubs[target].withWaitForReady().installstate(request, new StreamObserver<DidaTradePaxos.InstallStateReply>() {
                @Override
                public void onNext(DidaTradePaxos.InstallStateReply reply) {
                    if (reply.getAck() && acks.incrementAndGet() == quorum) {
                        completion.transferDone();
                    }
                }
                @Override public void onError(Throwable t) { }
                @Override public void onCompleted() { }
            });
        }
    }

    /* Step 5: quando é que o ballot do líder fica completo (ver DidaTradeServerState.setCompletedBallot). */
    private class CompletionTracker {
        final int ballot;
        int pending_instances;
        boolean transferred;
        boolean done;

        CompletionTracker(int ballot, int recovered_instances) {
            this.ballot = ballot;
            this.pending_instances = recovered_instances;
            this.transferred = false;
            this.done = false;
        }

        synchronized void instanceAccepted() {
            this.pending_instances--;
            check();
        }

        synchronized void transferDone() {
            this.transferred = true;
            check();
        }

        private void check() {
            if (!this.done && this.transferred && this.pending_instances <= 0) {
                this.done = true;
                System.out.println("Vertical Paxos: ballot " + this.ballot + " is complete");
                server_state.setCompletedBallot(this.ballot);
            }
        }
    }

    /**
     * Valor a propor numa posição, dados os votos de um quórum de `promises` aceitadores.
     * Só conta o ballot mais alto k. Num ballot clássico há um único valor. Num ballot rápido
     * os aceitadores podem ter votado em valores diferentes: um valor v pode ter sido escolhido
     * se algum quórum rápido R votou v, e então Q∩R (>= |Q|+|R|-n aceitadores de Q) votou v, por
     * isso v é obrigatório. Se nenhum valor chega a esse mínimo, nada foi escolhido e qualquer
     * dos votados serve (escolhe-se o mais votado).
     */
    private int chooseValue(List<DidaTradePaxos.LogEntry> votes, Set<Integer> promised_acceptors) {
        int k = -1;
        for (DidaTradePaxos.LogEntry vote : votes) {
            k = Math.max(k, vote.getBallot());
        }
        Map<Integer, Integer> count = new HashMap<Integer, Integer>();
        for (DidaTradePaxos.LogEntry vote : votes) {
            if (vote.getBallot() == k) {
                count.merge(vote.getCommandId(), 1, Integer::sum);
            }
        }
        List<Integer> fast_acceptors = this.server_state.scheduler.acceptors(k);
        int promises_in_fast_configuration = 0;
        for (Integer acceptor : fast_acceptors) {
            if (promised_acceptors.contains(acceptor)) {
                promises_in_fast_configuration++;
            }
        }
        int forced_at_least = Math.max(0, promises_in_fast_configuration
                + this.server_state.scheduler.fastquorum(k) - fast_acceptors.size());
        int best = NOOP;
        int best_count = 0;
        for (Map.Entry<Integer, Integer> candidate : count.entrySet()) {
            if (this.server_state.scheduler.fastpaxos(k) && candidate.getValue() >= forced_at_least) {
                return candidate.getKey();
            }
            if (candidate.getValue() > best_count || (candidate.getValue() == best_count && candidate.getKey() < best)) {
                best = candidate.getKey();
                best_count = candidate.getValue();
            }
        }
        return best;
    }

    /**
     * MULTI-PAXOS: Fase 2 assíncrona (pipelining). O líder não espera pelas respostas:
     * a decisão chega pelos learners, e as respostas são tratadas pelo PhaseTwoObserver.
     */
    private void sendPhaseTwo(int instance, int ballot, int value) {
        sendPhaseTwo(instance, ballot, value, null);
    }

    /* `completion` is set for the values recovered by phase 1: each one counts towards completing the ballot. */
    private void sendPhaseTwo(int instance, int ballot, int value, CompletionTracker completion) {
        System.out.println("Multi-Paxos: A enviar Fase 2 assíncrona para a instância " + instance + " (Ballot " + ballot + ", Command ID = " + value + ")");
        List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
        PhaseTwoTracker tracker = new PhaseTwoTracker(instance, ballot, this.server_state.scheduler.quorum(ballot), completion);

        DidaTradePaxos.PhaseTwoRequest p2_req = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(instance).setRequestballot(ballot).setValue(value).build();

        for (int target : acceptors) {
            this.server_state.async_stubs[target].withWaitForReady().phasetwo(p2_req, new PhaseTwoObserver(tracker));
        }
    }

    /** Fast Paxos: send a value without choosing its slot; each acceptor picks
     * its lowest open position and returns that position in the reply. */
    private void sendFastProposal(int ballot, int value) {
        this.fast_pending_values.add(value);
        wakeup();
        DidaTradePaxos.PhaseTwoRequest request = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setRequestballot(ballot).setValue(value).setAllocate(true).build();
        for (int target : this.server_state.scheduler.acceptors(ballot)) {
            sendFastProposalTo(target, request, ballot, value);
        }
    }

    private void sendFastProposalTo(int target, DidaTradePaxos.PhaseTwoRequest request, int ballot, int value) {
        this.server_state.async_stubs[target].withWaitForReady().phasetwo(request, new StreamObserver<DidaTradePaxos.PhaseTwoReply>() {
            @Override
            public void onNext(DidaTradePaxos.PhaseTwoReply reply) {
                if (reply.getAccepted() && fast_proposer_ballot == ballot) {
                    fast_pending_values.remove(value);
                    noteVote(reply.getInstance(), ballot, value);
                } else if (reply.getMaxballot() > ballot) {
                    server_state.setCurrentBallot(reply.getMaxballot());
                    wakeup();
                }
            }

            @Override
            public void onError(Throwable t) {
                // Another acceptor may still form the fast quorum.
            }

            @Override
            public void onCompleted() {
            }
        });
    }

    /* Estado partilhado pelas respostas de Fase 2 de uma instância. */
    private class PhaseTwoTracker {
        final int instance;
        final int ballot;
        final int quorum;
        final CompletionTracker completion;
        int accepts;
        boolean rejected;

        PhaseTwoTracker(int instance, int ballot, int quorum, CompletionTracker completion) {
            this.instance = instance;
            this.ballot = ballot;
            this.quorum = quorum;
            this.completion = completion;
            this.accepts = 0;
            this.rejected = false;
        }

        synchronized void onReply(DidaTradePaxos.PhaseTwoReply reply) {
            if (reply.getAccepted()) {
                this.accepts++;
                if (this.accepts == this.quorum) {
                    System.out.println("Multi-Paxos: Fase 2 aceite por um quórum na instância " + this.instance + " (Ballot " + this.ballot + ")");
                    // Step 5: um quórum ter aceite uma instância qualquer não completa o ballot
                    // (podia haver valores recuperados ainda só nos aceitadores antigos)
                    if (this.completion != null) {
                        this.completion.instanceAccepted();
                    }
                }
            } else if (!this.rejected) {
                // Um aceitador já prometeu um ballot maior: deixamos de ser líder deste ballot.
                // O main loop volta a correr a Fase 1 se ainda formos líder do novo ballot.
                this.rejected = true;
                System.out.println("Multi-Paxos: Fase 2 rejeitada na instância " + this.instance + " (maxballot " + reply.getMaxballot() + ")");
                server_state.setCurrentBallot(reply.getMaxballot());
                wakeup();
            }
        }
    }

    private class PhaseTwoObserver implements StreamObserver<DidaTradePaxos.PhaseTwoReply> {
        private final PhaseTwoTracker tracker;

        PhaseTwoObserver(PhaseTwoTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        public void onNext(DidaTradePaxos.PhaseTwoReply reply) {
            this.tracker.onReply(reply);
        }

        @Override
        public void onError(Throwable t) {
            // Aceitador em baixo: outro membro do quórum responde por ele
        }

        @Override
        public void onCompleted() {
        }
    }

    private void executeDecided() {
        while (true) {
            PaxosInstance entry = this.server_state.paxos_log.getEntry(this.next_instance_to_execute);
            if (entry == null || !entry.decided) {
                break; // Ainda não atingiu o quórum
            }

            if (entry.decided_value == NOOP || this.server_state.req_history.getIfProcessed(entry.decided_value) != null) {
                // No-op de preenchimento, ou pedido já executado noutra instância
                System.out.println("Multi-Paxos: A saltar instância " + this.next_instance_to_execute + " (Command ID = " + entry.decided_value + ")");
                this.next_instance_to_execute++;
                this.server_state.setExecutedSlots(this.next_instance_to_execute);
                continue;
            }

            RequestRecord request_record = this.server_state.req_history.getIfPending(entry.decided_value);
            if (request_record == null) {
                // A instância está decidida na rede, mas o pedido gRPC do cliente ainda não
                // chegou a esta réplica específica. Sai e espera que o wakeup() seja chamado.
                break;
            }

            System.out.println("Multi-Paxos: Executar instância decidida " + this.next_instance_to_execute + " (Command ID = " + entry.decided_value + ")");
            DidaTradeCommand command = request_record.getRequest();
            boolean result = false;
            DidaTradeAction action = command.getAction();
            int slot = this.next_instance_to_execute + 1;

            // Step 4: o estado e o contador de slots mudam juntos, debaixo do mesmo lock que
            // o balance causal usa para os ler. Assim um balance nunca vê o estado depois deste
            // comando com um contador de antes (o cliente podia depois ler um estado mais antigo).
            synchronized (this.server_state.trade_manager) {
                switch (action) {
                    case POPULATE: result = this.server_state.trade_manager.populate(command.getQuantity()); break;
                    case ADDUSER: result = this.server_state.trade_manager.add_user(command.getUserId(), command.getQuantity(), command.getStock()); break;
                    case SELL: result = this.server_state.trade_manager.sell(command.getUserId(), command.getQuantity()); break;
                    case BUY: result = this.server_state.trade_manager.acquire(command.getUserId(), command.getQuantity()); break;
                    case DUMP:
                        this.server_state.trade_manager.dump();
                        result = true;
                        break;
                    default:
                        result = false;
                        System.err.println("*** Unknown command ****");
                        break;
                }
                this.server_state.setExecutedSlots(slot);
            }

            // Fecha a transação
            request_record.setSlot(slot);
            request_record.setResponse(result);
            this.server_state.req_history.moveToProcessed(request_record.getId());
            this.proposed_requests.remove(request_record.getId());

            this.next_instance_to_execute++; // Avança estritamente em ordem 0, 1, 2...
        }
    }
}
