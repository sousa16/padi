package didatrade.server;

import java.util.*;
import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseOneProcessor;
import didatrade.util.PhaseTwoResponseProcessor;
import didatrade.configs.ConfigurationScheduler;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class MainLoop implements Runnable {
    DidaTradeServerState server_state;
    private boolean has_work;
    
    // Contadores separados para permitir o Pipelining (Multi-Paxos)
    private int next_instance_to_propose;
    private int next_instance_to_execute;
    
    // Variáveis cruciais para o Multi-Paxos
    private int phase1_completed_ballot;
    private Set<Integer> proposed_requests;

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
                    try { wait(); } catch (InterruptedException e) {}
                }
                this.has_work = false;
            }
        }
    }

    public synchronized void wakeup() {
        this.has_work = true;
        notifyAll();
    }

    private void proposePending() {
        int ballot = this.server_state.getCurrentBallot();
        if (ballot < 0 || this.server_state.scheduler.leader(ballot) != this.server_state.my_id) {
            return; // Se não sou o líder, não proponho nada
        }

        while (true) {
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

            int phase_two_value = request_record.getId();
            boolean ballot_aborted = false;

            // MULTI-PAXOS: Saltar a Fase 1 (Prepare) se já a fizemos com sucesso neste ballot!
            if (ballot > this.phase1_completed_ballot) {
                System.out.println("Multi-Paxos: A correr Fase 1 para a instância " + instance + " (Ballot " + ballot + ")");
                int low_ballot = Math.max(this.server_state.getCompletedBallot(), 0);
                List<Integer> phase_one_acceptors = this.server_state.scheduler.acceptorsinrange(low_ballot, ballot);
                PhaseOneProcessor phase_one_processor = new PhaseOneProcessor(this.server_state.scheduler, low_ballot, ballot);
                ArrayList<DidaTradePaxos.PhaseOneReply> phase_one_responses = new ArrayList<>();
                GenericResponseCollector<DidaTradePaxos.PhaseOneReply> phase_one_collector = 
                        new GenericResponseCollector<>(phase_one_responses, phase_one_acceptors.size(), phase_one_processor);

                DidaTradePaxos.PhaseOneRequest p1_req = DidaTradePaxos.PhaseOneRequest.newBuilder()
                        .setInstance(instance).setRequestballot(ballot).build();

                for (int target : phase_one_acceptors) {
                    CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> obs = new CollectorStreamObserver<>(phase_one_collector);
                    this.server_state.async_stubs[target].phaseone(p1_req, obs);
                }
                phase_one_collector.waitUntilDone();

                Map<Integer, PaxosInstance> merged_log = new HashMap<Integer, PaxosInstance>();
                for (DidaTradePaxos.PhaseOneReply reply : phase_one_responses) {
                    for (DidaTradePaxos.LogEntry log_entry : reply.getLogList()) {
                        PaxosInstance existing = merged_log.get(log_entry.getPosition());
                        if (existing == null || log_entry.getBallot() > existing.accept_ballot ||
                                (log_entry.getBallot() == existing.accept_ballot && log_entry.getCommandId() > existing.command_id)) {
                            merged_log.put(log_entry.getPosition(), new PaxosInstance(log_entry.getPosition(), log_entry.getBallot(), log_entry.getCommandId()));
                        }
                    }
                }

                if (!phase_one_processor.getAccepted()) {
                    ballot_aborted = true;
                    if (phase_one_processor.getMaxballot() > this.server_state.getCurrentBallot()) {
                        this.server_state.setCurrentBallot(phase_one_processor.getMaxballot());
                    }
                } else {
                    if (phase_one_processor.getValballot() > -1) {
                        phase_two_value = phase_one_processor.getValue();
                    }
                    if (!merged_log.containsKey(instance)) {
                        merged_log.put(instance, new PaxosInstance(instance, ballot, phase_two_value));
                    }
                    this.phase1_completed_ballot = ballot; 
                }

                this.server_state.paxos_log.getEntry(instance);
                for (Map.Entry<Integer, PaxosInstance> merged_entry : merged_log.entrySet()) {
                    this.server_state.paxos_log.applyLogEntry(merged_entry.getKey(), merged_entry.getValue().accept_ballot, merged_entry.getValue().command_id);
                }
            }

            // MULTI-PAXOS: Fase 2 Assíncrona (Pipelining)
            if (!ballot_aborted) {
                System.out.println("Multi-Paxos: A enviar Fase 2 assíncrona para a instância " + instance);
                List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
                PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(this.server_state.scheduler.quorum(ballot));
                ArrayList<DidaTradePaxos.PhaseTwoReply> phase_two_responses = new ArrayList<>();
                GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> phase_two_collector = 
                        new GenericResponseCollector<>(phase_two_responses, acceptors.size(), phase_two_processor);

                DidaTradePaxos.PhaseTwoRequest.Builder p2_builder = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                        .setInstance(instance).setRequestballot(ballot).setValue(phase_two_value);

                List<Integer> positions = new ArrayList<Integer>(this.server_state.paxos_log.snapshotLog().size());
                for (PaxosInstance log_entry : this.server_state.paxos_log.snapshotLog()) {
                    positions.add(log_entry.instance_nb);
                }
                Collections.sort(positions);

                for (Integer pos : positions) {
                    PaxosInstance log_entry = this.server_state.paxos_log.getEntry(pos);
                    if (log_entry == null) {
                        continue;
                    }
                    DidaTradePaxos.LogEntry log_message = DidaTradePaxos.LogEntry.newBuilder()
                            .setPosition(log_entry.instance_nb)
                            .setBallot(log_entry.accept_ballot)
                            .setCommandId(log_entry.command_id)
                            .build();
                    p2_builder.addLog(log_message);
                }

                DidaTradePaxos.PhaseTwoRequest p2_req = p2_builder.build();

                for (int target : acceptors) {
                    CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply> obs = new CollectorStreamObserver<>(phase_two_collector);
                    this.server_state.async_stubs[target].phasetwo(p2_req, obs);
                }

                if (phase_two_value == request_record.getId()) {
                    this.proposed_requests.add(request_record.getId());
                }
                this.next_instance_to_propose++;
                
            } else {
                break; // Perdemos a liderança ou fomos abortados
            }
        }
    }

    private void executeDecided() {
        while (true) {
            PaxosInstance entry = this.server_state.paxos_log.getEntry(this.next_instance_to_execute);
            if (entry == null || !entry.decided) {
                break; // Ainda não atingiu o quórum
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
