package didatrade.server;

import java.util.*;
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

    DidaTradeServerState server_state;
    private boolean has_work;

    // Contadores separados para permitir o Pipelining (Multi-Paxos)
    private int next_instance_to_propose;
    private int next_instance_to_execute;

    // Variáveis cruciais para o Multi-Paxos
    private int phase1_completed_ballot;
    private Set<Integer> proposed_requests;
    // Fase 1 falhou sem haver um ballot maior (ex.: aceitadores inalcançáveis): tenta de novo
    private boolean retry_phase_one;

    public MainLoop(DidaTradeServerState state) {
        this.server_state = state;
        this.has_work = false;
        this.next_instance_to_propose = 0;
        this.next_instance_to_execute = 0;
        this.phase1_completed_ballot = -1;
        this.proposed_requests = new HashSet<>();
    }

    public void run() {
        while (true) {
            // 1. Executa as instâncias que já atingiram o consenso
            executeDecided();

            // 2. Tenta propor novos pedidos pendentes rapidamente (Multi-Paxos)
            proposePending();

            synchronized (this) {
                if (!this.has_work) {
                    try { wait(this.retry_phase_one ? PHASE_ONE_RETRY_MS : 0); } catch (InterruptedException e) {}
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
        if (!isLeader(ballot)) {
            return; // Se não sou o líder, não proponho nada
        }

        // MULTI-PAXOS: a Fase 1 corre uma única vez por ballot e cobre todas as instâncias
        this.retry_phase_one = false;
        if (ballot > this.phase1_completed_ballot) {
            if (this.server_state.req_history.getFirstUnproposed(this.proposed_requests) == null) {
                return; // Nada para propor: a Fase 1 espera pelo primeiro pedido
            }
            if (!runPhaseOne(ballot)) {
                // Se o ballot não subiu ninguém nos ultrapassou, só não houve quórum: repetir
                this.retry_phase_one = (this.server_state.getCurrentBallot() == ballot);
                return;
            }
        }

        while (this.server_state.getCurrentBallot() == ballot) {
            RequestRecord request_record = this.server_state.req_history.getFirstUnproposed(this.proposed_requests);
            if (request_record == null) {
                break; // Acabaram-se os pedidos por propor
            }

            int instance = this.next_instance_to_propose;
            PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
            if (entry.decided) {
                this.next_instance_to_propose++;
                continue; // Alguém já decidiu esta instância, avança
            }

            sendPhaseTwo(instance, ballot, request_record.getId());
            this.proposed_requests.add(request_record.getId());
            this.next_instance_to_propose++;
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

        // Junta os logs recebidos: para cada posição fica o valor com o maior ballot
        Map<Integer, Integer> merged_value  = new HashMap<Integer, Integer>();
        Map<Integer, Integer> merged_ballot = new HashMap<Integer, Integer>();
        synchronized (phase_one_collector) {
            for (DidaTradePaxos.PhaseOneReply reply : phase_one_responses) {
                if (!reply.getAccepted()) {
                    continue;
                }
                for (DidaTradePaxos.LogEntry log_entry : reply.getLogList()) {
                    Integer best = merged_ballot.get(log_entry.getPosition());
                    if (best == null || log_entry.getBallot() > best) {
                        merged_ballot.put(log_entry.getPosition(), log_entry.getBallot());
                        merged_value.put(log_entry.getPosition(), log_entry.getCommandId());
                    }
                }
            }
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
            Integer value = entry.decided ? Integer.valueOf(entry.command_id) : merged_value.get(instance);
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

            if (entry.command_id == NOOP || this.server_state.req_history.getIfProcessed(entry.command_id) != null) {
                // No-op de preenchimento, ou pedido já executado noutra instância
                System.out.println("Multi-Paxos: A saltar instância " + this.next_instance_to_execute + " (Command ID = " + entry.command_id + ")");
                this.next_instance_to_execute++;
                continue;
            }

            RequestRecord request_record = this.server_state.req_history.getIfPending(entry.command_id);
            if (request_record == null) {
                // A instância está decidida na rede, mas o pedido gRPC do cliente ainda não
                // chegou a esta réplica específica. Sai e espera que o wakeup() seja chamado.
                break;
            }

            System.out.println("Multi-Paxos: Executar instância decidida " + this.next_instance_to_execute + " (Command ID = " + entry.command_id + ")");
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
