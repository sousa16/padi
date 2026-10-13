
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

    // phaseone and phasetwo read-modify-write the same acceptor state, and in a
    // fast ballot several proposers hit the same instance concurrently
    private final Object acceptor_lock = new Object();

    public DidaTradePaxosServiceImpl(DidaTradeServerState state) {
	this.server_state = state;
    }
 
    @Override
    public void phaseone(DidaTradePaxos.PhaseOneRequest request, StreamObserver<DidaTradePaxos.PhaseOneReply> responseObserver) {
	this.server_state.debugGate();

	// System.out.println("Receive phase1 request: \n" + request);

	int instance          = request.getInstance();
	int ballot            = request.getRequestballot();
	boolean accepted      = false;
	int  value, valballot, maxballot;
	List<DidaTradePaxos.LogEntry> log = new ArrayList<DidaTradePaxos.LogEntry>();

	synchronized (acceptor_lock) {
	    PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance, ballot);
	    value               = entry.command_id;
	    valballot           = entry.write_ballot;

	    if (ballot >= this.server_state.getCurrentBallot()) {
		accepted = true;
		this.server_state.setCurrentBallot(ballot);
		entry.read_ballot = ballot;
	    }

	    maxballot = this.server_state.getCurrentBallot();

	    // Multi-Paxos: the promise covers every instance from `instance` onwards,
	    // so send back everything this acceptor has accepted (or knows to be
	    // decided) from there. The new leader must re-propose these values.
	    // Read under acceptor_lock, the same lock phasetwo uses to write
	    // write_ballot/command_id -- otherwise this loop can read a stale,
	    // pre-vote value for an instance another thread just voted on.
	    if (accepted) {
		for (PaxosInstance log_entry : this.server_state.paxos_log.snapshotFrom(instance)) {
		    if (log_entry.write_ballot < 0 && !log_entry.decided)
			continue;
		    DidaTradePaxos.LogEntry.Builder log_entry_builder = DidaTradePaxos.LogEntry.newBuilder();
		    log_entry_builder.setPosition(log_entry.instance_nb);
		    // A decided value is final. Otherwise report this acceptor's own vote
		    // (value + the ballot it was cast in), never what it merely learned.
		    if (log_entry.decided) {
			log_entry_builder.setBallot(log_entry.accept_ballot);
			log_entry_builder.setCommandId(log_entry.decided_value);
		    }
		    else {
			log_entry_builder.setBallot(log_entry.write_ballot);
			log_entry_builder.setCommandId(log_entry.command_id);
		    }
		    log.add(log_entry_builder.build());
		}
	    }
	}

	DidaTradePaxos.PhaseOneReply.Builder response_builder = DidaTradePaxos.PhaseOneReply.newBuilder();
	response_builder.setInstance(instance);
	response_builder.setServerid(this.server_state.my_id);
	response_builder.setRequestballot(ballot);
	response_builder.setAccepted(accepted);
	response_builder.setValue(value);
	response_builder.setValballot(valballot);
	response_builder.setMaxballot(maxballot);
	response_builder.addAllLog(log);

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
	boolean accepted      = false;
	int  maxballot        = ballot;
	boolean is_any        = request.getAny();
	boolean allocate      = request.getAllocate();

	synchronized (acceptor_lock) {
	    if (ballot < this.server_state.getCurrentBallot()) {
		maxballot = this.server_state.getCurrentBallot();
	    }
	    else if (is_any) {
		// Fast Paxos "any": no value and no vote, just opens the instances from here on
		accepted = true;
		this.server_state.setCurrentBallot(ballot);
		this.server_state.openFastBallot(ballot, instance);
	    }
	    else {
		if (allocate) {
		    if (!this.server_state.scheduler.fastpaxos(ballot)
			    || this.server_state.getFastAnyBallot() != ballot) {
			maxballot = this.server_state.getCurrentBallot();
			instance = -1;
		    }
		    else {
			instance = lowestOpenFastInstance(ballot, value);
		    }
		}
		if (allocate && instance < 0) {
		    maxballot = this.server_state.getCurrentBallot();
		} else {
		    PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
		    if (this.server_state.scheduler.fastpaxos(ballot) && entry.write_ballot == ballot && entry.command_id != value) {
			// fast ballot: the first value voted for in an instance is the only one, a different proposal is a collision
			maxballot = this.server_state.getCurrentBallot();
		    } else {
			accepted = true;
			if (entry.write_ballot != ballot) {
			    entry.command_id   = value;
			    entry.write_ballot = ballot;
			}
			this.server_state.setCurrentBallot(ballot);
		    }
		}
	    }
	}

	final int accepted_instance = instance;
	if (is_any) {
	    if (accepted)
		this.server_state.main_loop.wakeup();
	}
	else if (accepted) {
	    this.server_state.main_loop.noteVote(accepted_instance, ballot, value);
	}

	DidaTradePaxos.PhaseTwoReply.Builder response_builder = DidaTradePaxos.PhaseTwoReply.newBuilder();
	response_builder.setAccepted(accepted);
	response_builder.setInstance(accepted_instance);
	response_builder.setServerid(this.server_state.my_id);
	response_builder.setRequestballot(ballot);
	response_builder.setMaxballot(maxballot);

	DidaTradePaxos.PhaseTwoReply response = response_builder.build();
	
	responseObserver.onNext(response);
	responseObserver.onCompleted();
	
	// Notify learners (an "any" is not a vote)
	if (accepted == true && !is_any) {
	    
	    Context ctx = Context.current().fork();
	    ctx.run(() -> {
		    List<Integer> learners = this.server_state.scheduler.learners(ballot);
		    int n_targets          = learners.size();
		    
		    DidaTradePaxos.LearnRequest.Builder learn_request_builder = DidaTradePaxos.LearnRequest.newBuilder();
		    learn_request_builder.setInstance(accepted_instance);
		    learn_request_builder.setValue(value);
		    learn_request_builder.setBallot(ballot);
		    learn_request_builder.setServerid(this.server_state.my_id);
		    
		    DidaTradePaxos.LearnRequest learn_request = learn_request_builder.build();
		    
		    System.out.println("Paxos acceptor: going to notify learners for entry " + accepted_instance + " with timestamp " + ballot + " request = " + learn_request);
		    ArrayList<DidaTradePaxos.LearnReply> learn_responses = new ArrayList<DidaTradePaxos.LearnReply>();
		    GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector = new GenericResponseCollector<DidaTradePaxos.LearnReply>(learn_responses, n_targets);;
		    for (int i = 0; i < n_targets; i++) {
			CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer = new CollectorStreamObserver<DidaTradePaxos.LearnReply>(learn_collector);
			this.server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
		    }
		});
	}
    }

    /* Fast proposals have no slot until each acceptor places them. Repeated
     * deliveries of the same request reuse its slot rather than consuming one. */
    private int lowestOpenFastInstance(int ballot, int value) {
	int instance = this.server_state.getFastAnyFrom();
	while (instance >= 0) {
	    PaxosInstance entry = this.server_state.paxos_log.getEntry(instance);
	    if (entry == null || (!entry.decided && entry.write_ballot < 0))
		return instance;
	    if (!entry.decided && entry.write_ballot == ballot && entry.command_id == value)
		return instance;
	    if (instance == Integer.MAX_VALUE)
		break;
	    instance++;
	}
	return -1;
    }

    @Override
    public void learn(DidaTradePaxos.LearnRequest request, StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
	this.server_state.debugGate();

	// System.out.println("Receive learn request: \n" + request);

	int instance         = request.getInstance();
	int ballot           = request.getBallot();
	int value            = request.getValue();
	int acceptor         = request.getServerid();

	synchronized (this) {
	    PaxosInstance entry  = this.server_state.paxos_log.testAndSetEntry(instance);

	    this.server_state.setCurrentBallot(ballot);

	    if (ballot > entry.accept_ballot && !entry.decided) {
		// a higher ballot supersedes whatever was being tallied
		entry.accept_ballot = ballot;
		entry.votes.clear();
	    }
	    if (ballot == entry.accept_ballot && !entry.decided) {
		// count each acceptor once per value, so a value is decided only when
		// enough distinct acceptors voted for that same value
		Set<Integer> voters = entry.votes.get(value);
		if (voters == null) {
		    voters = new HashSet<Integer>();
		    entry.votes.put(value, voters);
		}
		voters.add(acceptor);
		System.out.println("Paxos learner for instance " + instance + " ballot " + ballot + " : value " + value + " has " + voters.size() + " votes");

		if (voters.size() >= this.server_state.scheduler.decisionquorum(ballot)) {
		    System.out.println("Paxos learner: instance " + instance + " decided (Command ID = " + value + ")");
		    entry.decided_value = value;
		    entry.decided       = true;
		    this.server_state.updateCompletedBallot(ballot);
		    this.server_state.main_loop.wakeup ();
		}
		else if (collided(entry, ballot)) {
		    // No value can reach the fast quorum any more in this ballot. Moving
		    // to the next ballot is safe, its phase 1 recovers anything that may
		    // have been chosen.
		    System.out.println("Paxos learner: collision in instance " + instance + ", moving to ballot " + (ballot + 1));
		    this.server_state.setCurrentBallot(ballot + 1);
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

    /* Fast ballots only: true when no value can still collect a decision quorum. */
    private boolean collided(PaxosInstance entry, int ballot) {
	if (!this.server_state.scheduler.fastpaxos(ballot))
	    return false;
	int needed    = this.server_state.scheduler.decisionquorum(ballot);
	int voted     = 0;
	int best      = 0;
	for (Set<Integer> voters : entry.votes.values()) {
	    voted += voters.size();
	    best   = Math.max(best, voters.size());
	}
	int missing = this.server_state.scheduler.acceptors(ballot).size() - voted;
	return best + missing < needed;
    }

}
