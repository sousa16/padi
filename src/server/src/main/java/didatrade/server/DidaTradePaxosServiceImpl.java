
package didatrade.server;

import java.util.*;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Context;

public class DidaTradePaxosServiceImpl extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
    DidaTradeServerState server_state;

    public DidaTradePaxosServiceImpl(DidaTradeServerState state) {
	this.server_state = state;
    }
 
    @Override
    public void phaseone(DidaTradePaxos.PhaseOneRequest request, StreamObserver<DidaTradePaxos.PhaseOneReply> responseObserver) {
	this.server_state.debugGate();

	// System.out.println("Receive phase1 request: \n" + request);

	int instance          = request.getInstance();
	int ballot            = request.getRequestballot();
	PaxosInstance entry   = this.server_state.paxos_log.testAndSetEntry(instance, ballot);
	boolean accepted      = false;
	int  value            = entry.command_id;
	int  valballot        = entry.write_ballot;

	if (ballot >= this.server_state.getCurrentBallot()) {
	    accepted = true;
	    this.server_state.setCurrentBallot(ballot);
	    entry.read_ballot = ballot;
	}

	int maxballot = this.server_state.getCurrentBallot();

	DidaTradePaxos.PhaseOneReply.Builder response_builder = DidaTradePaxos.PhaseOneReply.newBuilder();
	response_builder.setInstance(instance);
	response_builder.setServerid(this.server_state.my_id);
	response_builder.setRequestballot(ballot);
	response_builder.setAccepted(accepted);
	response_builder.setValue(value);
	response_builder.setValballot(valballot);
	response_builder.setMaxballot(maxballot);

	// Multi-Paxos: the promise covers every instance from `instance` onwards,
	// so send back everything this acceptor has accepted (or knows to be
	// decided) from there. The new leader must re-propose these values.
	if (accepted) {
	    for (PaxosInstance log_entry : this.server_state.paxos_log.snapshotFrom(instance)) {
		if (log_entry.write_ballot < 0 && !log_entry.decided)
		    continue;
		DidaTradePaxos.LogEntry.Builder log_entry_builder = DidaTradePaxos.LogEntry.newBuilder();
		log_entry_builder.setPosition(log_entry.instance_nb);
		log_entry_builder.setBallot(Math.max(log_entry.accept_ballot, log_entry.write_ballot));
		log_entry_builder.setCommandId(log_entry.command_id);
		response_builder.addLog(log_entry_builder.build());
	    }
	}

	DidaTradePaxos.PhaseOneReply response = response_builder.build();

	responseObserver.onNext(response);
	responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DidaTradePaxos.PhaseTwoRequest request, StreamObserver<DidaTradePaxos.PhaseTwoReply> responseObserver) {
	this.server_state.debugGate();

	// System.out.println ("Receive phase two request: \n" + request);

	int instance          = request.getInstance();
	int ballot            = request.getRequestballot();
	int value             = request.getValue();
	PaxosInstance entry   = this.server_state.paxos_log.testAndSetEntry(instance);
	boolean accepted      = false;
	int  maxballot        = ballot;

	if (ballot >= this.server_state.getCurrentBallot()) {
	    accepted           = true;
	    entry.command_id   = value;
	    entry.write_ballot = ballot;
	    this.server_state.setCurrentBallot(ballot);
	}
	else
	    maxballot = this.server_state.getCurrentBallot();
	

	DidaTradePaxos.PhaseTwoReply.Builder response_builder = DidaTradePaxos.PhaseTwoReply.newBuilder();
	response_builder.setAccepted(accepted);
	response_builder.setInstance(instance);
	response_builder.setServerid(this.server_state.my_id);
	response_builder.setRequestballot(ballot);
	response_builder.setMaxballot(maxballot);

	DidaTradePaxos.PhaseTwoReply response = response_builder.build();
	
	responseObserver.onNext(response);
	responseObserver.onCompleted();
	
	// Notify learners
	if (accepted == true) {
	    
	    Context ctx = Context.current().fork();
	    ctx.run(() -> {
		    List<Integer> learners = this.server_state.scheduler.learners(ballot);
		    int n_targets          = learners.size();
		    
		    DidaTradePaxos.LearnRequest.Builder learn_request_builder = DidaTradePaxos.LearnRequest.newBuilder();
		    learn_request_builder.setInstance(instance);
		    learn_request_builder.setValue(value);
		    learn_request_builder.setBallot(ballot);
		    
		    DidaTradePaxos.LearnRequest learn_request = learn_request_builder.build();
		    
		    System.out.println("Paxos acceptor: going to notify learners for entry " + instance + " with timestamp " + ballot + " request = " + learn_request);
		    ArrayList<DidaTradePaxos.LearnReply> learn_responses = new ArrayList<DidaTradePaxos.LearnReply>();
		    GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector = new GenericResponseCollector<DidaTradePaxos.LearnReply>(learn_responses, n_targets);;
		    for (int i = 0; i < n_targets; i++) {
			CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer = new CollectorStreamObserver<DidaTradePaxos.LearnReply>(learn_collector);
			this.server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
		    }
		});
	}
	
	
    }

    @Override
    public void learn(DidaTradePaxos.LearnRequest request, StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
	this.server_state.debugGate();

	// System.out.println("Receive learn request: \n" + request);

	int instance         = request.getInstance();
	int ballot           = request.getBallot();
	int value            = request.getValue();

	
	synchronized (this) {
	    PaxosInstance entry  = this.server_state.paxos_log.testAndSetEntry(instance);
	    
	    // System.out.println("Paxos learner: learnin entry " + instance + " with timestamp " + ballot);

	    this.server_state.setCurrentBallot(ballot);
	    
	    if (ballot > entry.accept_ballot && !entry.decided) {
		System.out.println("Paxos learner for instance " + instance + " : resetting ");
		entry.command_id     = value;
		entry.accept_ballot  = ballot;
		entry.n_accepts      = 0;
	    }
	    if (ballot == entry.accept_ballot && !entry.decided) {
		entry.n_accepts++;
		System.out.println("Paxos learner for instance " + instance + " : number of accepts " +  entry.n_accepts);
		if (entry.n_accepts >= this.server_state.scheduler.quorum(ballot)) {
		    System.out.println("Paxos learner: waking up the main loop");
		    entry.decided = true;
		    this.server_state.updateCompletedBallot(ballot);
		    this.server_state.main_loop.wakeup ();
		}
	    }
	}
	
	DidaTradePaxos.LearnReply.Builder response_builder = DidaTradePaxos.LearnReply.newBuilder();
	response_builder.setInstance(instance);
	response_builder.setBallot(ballot);

	DidaTradePaxos.LearnReply response = response_builder.build();
	
	// System.out.println("Sending learn response");
		
	responseObserver.onNext(response);
	responseObserver.onCompleted();
    }

}
