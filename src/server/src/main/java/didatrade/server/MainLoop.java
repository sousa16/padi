package didatrade.server;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final long PHASE_ONE_RETRY_MS = 1000;
    // Fast ballots need every acceptor (fast quorum = 3 of 3 in schedule A): if proposals
    // sit undecided this long, give up on the ballot and let the next one's phase 1 sort it out
    private static final long FAST_STALL_MS = 3000;
    private static final long FAST_POLL_MS  = 500;

    DidaTradeServerState server_state;
    private boolean has_work;

    // Contadores separados para permitir o Pipelining (Multi-Paxos)
    private int next_instance_to_propose;
    private int next_instance_to_execute;

    // Variáveis cruciais para o Multi-Paxos
    private int phase1_completed_ballot;
    // Tocado pelo main loop e pelas threads do aceitador (noteVote), logo concorrente
    private Set<Integer> proposed_requests;
    // Fast-Paxos: ballot rápido em que este servidor já está a propor, e o que propôs/viu votar por instância
    private volatile int fast_proposer_ballot;
    private Map<Integer, Integer> fast_marks;
    private long fast_stall_since;
    // Fase 1 falhou sem haver um ballot maior (ex.: aceitadores inalcançáveis): tenta de novo
    private boolean retry_phase_one;

    public MainLoop(DidaTradeServerState state) {
        this.server_state = state;
        this.has_work = false;
        this.next_instance_to_propose = 0;
        this.next_instance_to_execute = 0;
        this.phase1_completed_ballot = -1;
        this.proposed_requests = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        this.fast_proposer_ballot = -1;
        this.fast_marks = new ConcurrentHashMap<>();
        this.fast_stall_since = 0;
    }

    public void run() {
        while (true) {
            // 1. Executa as instâncias que já atingiram o consenso
            settleFastMarks();
            executeDecided();

            // 2. Tenta propor novos pedidos pendentes rapidamente (Multi-Paxos)
            proposePending();

            // 3. Fast-Paxos: se nada se decide, passa ao ballot seguinte
            checkFastStall();

            synchronized (this) {
                if (!this.has_work) {
                    long timeout = this.retry_phase_one ? PHASE_ONE_RETRY_MS : (this.fast_marks.isEmpty() ? 0 : FAST_POLL_MS);
                    try { wait(timeout); } catch (InterruptedException e) {}
                }
                this.has_work = false;
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

        // Saímos de um ballot rápido (colisão ou timeout): as propostas dele não contam para o
        // novo ballot, a Fase 1 recupera o que ficou votado e o resto volta a ser proposto
        if (this.fast_proposer_ballot != -1 && this.fast_proposer_ballot != ballot) {
            this.proposed_requests.clear();
            this.fast_marks.clear();
            this.fast_proposer_ballot = -1;
        }

        // MULTI-PAXOS: a Fase 1 corre uma única vez por ballot e cobre todas as instâncias
        this.retry_phase_one = false;
        if (leader && ballot > this.phase1_completed_ballot) {
            if (this.server_state.req_history.getFirstUnproposed(this.proposed_requests) == null) {
                return; // Nada para propor: a Fase 1 espera pelo primeiro pedido
            }
            if (!runPhaseOne(ballot)) {
                // Se o ballot não subiu ninguém nos ultrapassou, só não houve quórum: repetir
                this.retry_phase_one = (this.server_state.getCurrentBallot() == ballot);
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

        while (this.server_state.getCurrentBallot() == ballot) {
            RequestRecord request_record = this.server_state.req_history.getFirstUnproposed(this.proposed_requests);
            if (request_record == null) {
                break; // Acabaram-se os pedidos por propor
            }

            int instance = this.next_instance_to_propose;
            PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
            if (entry.decided || (fast && entry.write_ballot == ballot)) {
                this.next_instance_to_propose++;
                continue; // Já decidida, ou (ballot rápido) já votada por outro proponente: avança
            }

            this.proposed_requests.add(request_record.getId());
            if (fast) {
                this.fast_marks.put(instance, request_record.getId());
            }
            sendPhaseTwo(instance, ballot, request_record.getId());
            this.next_instance_to_propose++;
        }
    }

    /**
     * Fast-Paxos: servidor que não é o líder entra num ballot rápido. Começa na primeira
     * instância aberta pelo "any" e não volta a propor pedidos que outros já têm em votação.
     */
    private void enterFastBallot(int ballot) {
        this.proposed_requests.clear();
        this.fast_marks.clear();
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
    }

    /** Fast-Paxos: o líder diz a todos os aceitadores/proponentes que as instâncias >= from estão abertas. */
    private void sendAny(int ballot, int from) {
        System.out.println("Fast-Paxos: a enviar \"any\" a partir da instância " + from + " (Ballot " + ballot + ")");
        DidaTradePaxos.PhaseTwoRequest any_req = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(from).setRequestballot(ballot).setAny(true).build();
        for (int target : this.server_state.scheduler.acceptors(ballot)) {
            this.server_state.async_stubs[target].phasetwo(any_req, new StreamObserver<DidaTradePaxos.PhaseTwoReply>() {
                @Override public void onNext(DidaTradePaxos.PhaseTwoReply reply) { }
                @Override public void onError(Throwable t) { }
                @Override public void onCompleted() { }
            });
        }
    }

    /**
     * Fast-Paxos precisa de todos os aceitadores (quórum rápido 3 de 3). Se há propostas
     * por decidir há demasiado tempo (aceitador em baixo, mensagens perdidas) passa-se ao
     * ballot seguinte: é seguro, a Fase 1 desse ballot recupera o que possa ter sido escolhido.
     */
    private void checkFastStall() {
        settleFastMarks();
        int ballot = this.server_state.getCurrentBallot();
        if (!this.server_state.scheduler.fastpaxos(ballot) || this.fast_proposer_ballot != ballot || this.fast_marks.isEmpty()) {
            this.fast_stall_since = 0;
            return;
        }
        long now = System.currentTimeMillis();
        if (this.fast_stall_since == 0) {
            this.fast_stall_since = now;
        } else if (now - this.fast_stall_since > FAST_STALL_MS) {
            System.out.println("Fast-Paxos: sem decisão há " + FAST_STALL_MS + "ms, a passar ao ballot " + (ballot + 1));
            this.server_state.setCurrentBallot(ballot + 1);
            this.fast_stall_since = 0;
            wakeup();
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
            this.server_state.async_stubs[target].phaseone(p1_req, obs);
        }
        phase_one_collector.waitUntilDone();

        if (!phase_one_processor.getAccepted()) {
            System.out.println("Multi-Paxos: Fase 1 rejeitada para o ballot " + ballot);
            this.server_state.setCurrentBallot(phase_one_processor.getMaxballot());
            return false;
        }

        // Junta os logs recebidos: para cada posição, os votos de cada aceitador que prometeu
        Map<Integer, List<DidaTradePaxos.LogEntry>> votes_by_position = new HashMap<Integer, List<DidaTradePaxos.LogEntry>>();
        int promises = 0;
        synchronized (phase_one_collector) {
            for (DidaTradePaxos.PhaseOneReply reply : phase_one_responses) {
                if (!reply.getAccepted()) {
                    continue;
                }
                promises++;
                for (DidaTradePaxos.LogEntry log_entry : reply.getLogList()) {
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
            merged_value.put(position.getKey(), chooseValue(position.getValue(), promises));
        }

        int last = from - 1;
        for (Integer position : merged_value.keySet()) {
            last = Math.max(last, position);
        }

        this.phase1_completed_ballot = ballot;
        this.next_instance_to_propose = from;
        // Os pedidos pendentes que não aparecerem no log recuperado voltam a ser propostos
        this.proposed_requests.clear();

        for (int instance = from; instance <= last; instance++) {
            PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
            Integer value = entry.decided ? Integer.valueOf(entry.decided_value) : merged_value.get(instance);
            if (value == null) {
                value = NOOP;
            }
            if (value != NOOP) {
                this.proposed_requests.add(value);
            }
            if (!entry.decided) {
                System.out.println("Multi-Paxos: A repropor instância " + instance + " (Command ID = " + value + ")");
                sendPhaseTwo(instance, ballot, value);
            }
        }
        this.next_instance_to_propose = last + 1;
        return true;
    }

    /**
     * Valor a propor numa posição, dados os votos de um quórum de `promises` aceitadores.
     * Só conta o ballot mais alto k. Num ballot clássico há um único valor. Num ballot rápido
     * os aceitadores podem ter votado em valores diferentes: um valor v pode ter sido escolhido
     * se algum quórum rápido R votou v, e então Q∩R (>= |Q|+|R|-n aceitadores de Q) votou v, por
     * isso v é obrigatório. Se nenhum valor chega a esse mínimo, nada foi escolhido e qualquer
     * dos votados serve (escolhe-se o mais votado).
     */
    private int chooseValue(List<DidaTradePaxos.LogEntry> votes, int promises) {
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
        int forced_at_least = promises + this.server_state.scheduler.fastquorum(k)
                - this.server_state.scheduler.acceptors(k).size();
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
        System.out.println("Multi-Paxos: A enviar Fase 2 assíncrona para a instância " + instance + " (Ballot " + ballot + ", Command ID = " + value + ")");
        List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
        PhaseTwoTracker tracker = new PhaseTwoTracker(instance, ballot, this.server_state.scheduler.quorum(ballot));

        DidaTradePaxos.PhaseTwoRequest p2_req = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(instance).setRequestballot(ballot).setValue(value).build();

        for (int target : acceptors) {
            this.server_state.async_stubs[target].phasetwo(p2_req, new PhaseTwoObserver(tracker));
        }
    }

    /* Estado partilhado pelas respostas de Fase 2 de uma instância. */
    private class PhaseTwoTracker {
        final int instance;
        final int ballot;
        final int quorum;
        int accepts;
        boolean rejected;

        PhaseTwoTracker(int instance, int ballot, int quorum) {
            this.instance = instance;
            this.ballot = ballot;
            this.quorum = quorum;
            this.accepts = 0;
            this.rejected = false;
        }

        synchronized void onReply(DidaTradePaxos.PhaseTwoReply reply) {
            if (reply.getAccepted()) {
                this.accepts++;
                if (this.accepts == this.quorum) {
                    System.out.println("Multi-Paxos: Fase 2 aceite por um quórum na instância " + this.instance + " (Ballot " + this.ballot + ")");
                    server_state.setCompletedBallot(this.ballot);
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
            int balance = 0;
            DidaTradeAction action = command.getAction();

            switch (action) {
                case POPULATE: result = this.server_state.trade_manager.populate(command.getQuantity()); break;
                case ADDUSER: result = this.server_state.trade_manager.add_user(command.getUserId(), command.getQuantity(), command.getStock()); break;
                case SELL: result = this.server_state.trade_manager.sell(command.getUserId(), command.getQuantity()); break;
                case BUY: result = this.server_state.trade_manager.acquire(command.getUserId(), command.getQuantity()); break;
                case BALANCE:
                    balance = this.server_state.trade_manager.balance(command.getUserId());
                    command.setQuantity(balance);
                    result = (balance != -1);
                    break;
                case DUMP:
                    this.server_state.trade_manager.dump();
                    result = true;
                    break;
                default:
                    result = false;
                    System.err.println("*** Unknown command ****");
                    break;
            }

            // Fecha a transação
            request_record.setResponse(result);
            this.server_state.req_history.moveToProcessed(request_record.getId());
            this.proposed_requests.remove(request_record.getId());

            this.next_instance_to_execute++; // Avança estritamente em ordem 0, 1, 2...
        }
    }
}
