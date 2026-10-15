package didatrade.server;

import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DidaTradeMasterServiceImpl extends DidaTradeMasterServiceGrpc.DidaTradeMasterServiceImplBase {
    DidaTradeServerState server_state;

    public DidaTradeMasterServiceImpl(DidaTradeServerState state) {
	this.server_state = state;
    }

    @Override
    public void newballot(DidaTradeMaster.NewBallotRequest request, StreamObserver<DidaTradeMaster.NewBallotReply> responseObserver) {
	System.out.println(request);

	int request_id       = request.getReqid();
	int new_ballot       = request.getNewballot();
	int completed_ballot = request.getCompletedballot();;

	// for debug purposes
	System.out.println("Current ballot = " + this.server_state.getCurrentBallot() + " new ballot = " + new_ballot + " completed ballot = " + completed_ballot);

	this.server_state.setCompletedBallot (completed_ballot);
	
	if (new_ballot > this.server_state.getCurrentBallot()) {
	    this.server_state.setCurrentBallot (new_ballot);

	    // Only the leader of new_ballot runs its phase 1 and completes it;
	    // anyone else would leave the console waiting forever.
	    if (this.server_state.scheduler.leader(new_ballot) == this.server_state.my_id)
		completed_ballot = this.server_state.waitForCompletedBallot(new_ballot);
	    else
		completed_ballot = this.server_state.getCompletedBallot();
	}
	else {
	    completed_ballot = this.server_state.getCompletedBallot();
	}
	
	DidaTradeMaster.NewBallotReply.Builder response_builder = DidaTradeMaster.NewBallotReply.newBuilder();
	response_builder.setReqid(request_id);
	response_builder.setCompletedballot(completed_ballot);

	DidaTradeMaster.NewBallotReply response = response_builder.build();
	responseObserver.onNext(response);
       	responseObserver.onCompleted();
    }


    @Override
    public void activate(DidaTradeMaster.ActivateRequest request, StreamObserver<DidaTradeMaster.ActivateReply> responseObserver) {
	System.out.println(request);

	int request_id       = request.getReqid();
	int completed_ballot = request.getCompletedballot();;

	// for debug purposes
	System.out.println("Current ballot = " + this.server_state.getCurrentBallot() + " activated ballot = " + completed_ballot);

	// Vertical Paxos: the console (the master) tells the leader which ballot is
	// complete, so its phase 1 only needs the acceptors from that ballot on.
	// Ballots before it may belong to replicas that have left the configuration.
	this.server_state.setCompletedBallot (completed_ballot);
	this.server_state.main_loop.wakeup();

	DidaTradeMaster.ActivateReply.Builder response_builder = DidaTradeMaster.ActivateReply.newBuilder();
	response_builder.setReqid(request_id);
	response_builder.setAck(true);

	DidaTradeMaster.ActivateReply response = response_builder.build();
	responseObserver.onNext(response);
       	responseObserver.onCompleted();
    }

    @Override
    public void setdebug(DidaTradeMaster.SetDebugRequest request, StreamObserver<DidaTradeMaster.SetDebugReply> responseObserver) {
	// for debug purposes
	System.out.println(request);

	boolean response_value = true;

	int request_id   = request.getReqid();
	int mode         = request.getMode();

	// Slow mode lives in its own flag, so that turning it on or off does not
	// disturb a freeze that is already in place.
	if ((mode != DidaTradeServerState.DEBUG_SLOW_ON) && (mode != DidaTradeServerState.DEBUG_SLOW_OFF))
	    this.server_state.setDebugMode (mode);

	// for debug purposes
	System.out.println("Setting debug mode to = " + this.server_state.getDebugMode());

	DidaTradeMaster.SetDebugReply.Builder response_builder = DidaTradeMaster.SetDebugReply.newBuilder();
	response_builder.setReqid(request_id);
	response_builder.setAck(response_value);
	
	DidaTradeMaster.SetDebugReply response = response_builder.build();
	responseObserver.onNext(response);
	responseObserver.onCompleted();

	// Done last, after the reply has been sent, so that a crash cannot kill
	// its own reply on the way out.
	switch (mode) {
	case DidaTradeServerState.DEBUG_CRASH:
	    this.server_state.crash();
	    break;
	case DidaTradeServerState.DEBUG_NORMAL:
	    this.server_state.setSlowMode (false);
	    System.out.println("Debug mode normal: freeze and slow mode cleared");
	    break;
	case DidaTradeServerState.DEBUG_SLOW_ON:
	    this.server_state.setSlowMode (true);
	    System.out.println("Debug mode slow enabled");
	    break;
	case DidaTradeServerState.DEBUG_SLOW_OFF:
	    this.server_state.setSlowMode (false);
	    System.out.println("Debug mode slow disabled");
	    break;
	case DidaTradeServerState.DEBUG_FREEZE:
	    System.out.println("Debug mode freeze enabled");
	    break;
	case DidaTradeServerState.DEBUG_UNFREEZE:
	    System.out.println("Debug mode freeze disabled");
	    break;
	default:
	    System.err.println("*** Unknown debug mode " + mode + " ***");
	    break;
	}
    }
}
