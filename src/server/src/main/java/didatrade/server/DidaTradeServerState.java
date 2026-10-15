package didatrade.server;

import java.util.*;

import didatrade.core.*;
import didatrade.configs.*;
import didatrade.configs.ConfigurationScheduler;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class DidaTradeServerState {
    public static final int     DEFAULT_POPULATION = 10;

    /* Debug modes, as typed at the console: `debug <mode> <replica>`. See README. */
    public static final int     DEBUG_NORMAL    = 0;
    public static final int     DEBUG_CRASH     = 1;
    public static final int     DEBUG_FREEZE    = 2;
    public static final int     DEBUG_UNFREEZE  = 3;
    public static final int     DEBUG_SLOW_ON   = 4;
    public static final int     DEBUG_SLOW_OFF  = 5;

    /* How long to wait before a simulated crash, so the reply to the console gets sent first. */
    private static final int    CRASH_DELAY_MS  = 200;

    /* Upper bound of the random delay applied in slow mode. */
    private static final int    SLOW_MODE_MAX_MS = 1000;
    int                         max_participants;
    TradeManager                trade_manager;
    ConfigurationScheduler      scheduler;
    int                         base_port;
    int                         my_id;
    RequestHistory              req_history;
    PaxosLog                    paxos_log;

    List<Integer>               all_participants;
    int                         n_participants;
    String[]                    targets;
    ManagedChannel[]            channels;
    DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;

    private int                 current_ballot;
    private int                 completed_ballot;

    /* Log slots executed so far, i.e. the next instance to execute. A causal
     * balance read waits until this reaches what the client has already seen. */
    private int                 executed_slots = 0;
    private int                 debug_mode;
    private boolean             slow_mode;
    private Random              debug_rnd;
    private boolean             fastpaxos_on;

    /* Latest "any" from a fast ballot's leader: instances >= fast_any_from are open to every proposer. */
    private int                 fast_any_ballot = -1;
    private int                 fast_any_from   = 0;

    /* Step 5, recovery. A replica that comes back has lost its promises and
     * votes, so until it has asked its peers where the system is it neither
     * promises nor votes (recovering), and afterwards it abstains from every
     * ballot up to the highest one its peers reported (abstain_upto): it may
     * have voted in those before the crash, and could now contradict itself. */
    private boolean             recovering    = true;
    private int                 abstain_upto  = -1;
    private int                 max_decided   = -1;

    MainLoop                    main_loop;
    Thread                      main_loop_worker;
    
    public DidaTradeServerState(int port, int myself, char schedule) {
	this.trade_manager    = new TradeManager();
	this.scheduler        = new ConfigurationScheduler (schedule);
	this.base_port        = port;
	this.my_id            = myself;
	this.debug_mode       = 0;
	this.slow_mode        = false;
	this.debug_rnd        = new Random();
	this.fastpaxos_on     = false;
	this.current_ballot   = 0;
	this.completed_ballot = -1;
	this.req_history      = new RequestHistory();
	this.paxos_log        = new PaxosLog();
	this.main_loop        = new MainLoop(this);

	// populate manager
	this.trade_manager.populate(DEFAULT_POPULATION);
	
	// init comms
	this.all_participants = this.scheduler.allparticipants ();
	this.n_participants = all_participants.size();
	
	this.targets = new String[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) {
	    int target_port = this.base_port + all_participants.get(i);
	    this.targets[i] = new String();
	    this.targets[i] = "localhost:" + target_port;
	    System.out.printf("targets[%d] = %s%n", i, targets[i]);
	}
	
	this.channels = new ManagedChannel[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) 
	    this.channels[i] = ManagedChannelBuilder.forTarget(this.targets[i]).usePlaintext().build();
	
	this.async_stubs = new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) 
	    this.async_stubs[i] = DidaTradePaxosServiceGrpc.newStub(this.channels[i]);	

	// start worker
	this.main_loop_worker = new Thread (main_loop);
	this.main_loop_worker.start();
    }

    public synchronized int getCurrentBallot () {
	return this.current_ballot;
    }
    
    public synchronized void setCurrentBallot (int ballot) {
	if (ballot > this.current_ballot) {
	    this.current_ballot = ballot;
	    this.main_loop.onBallotChanged(ballot);
	}
    }

    public synchronized int getCompletedBallot () {
	return this.completed_ballot;
    }

    /* Step 5 (Vertical Paxos): a ballot b is complete once its leader finished
     * phase 1, every value it recovered is accepted by a quorum of b's
     * acceptors, and, if the acceptor set changed, those acceptors also hold
     * the decided log (state transfer). Later leaders then only query
     * acceptors from b on. Only the leader of b (MainLoop.CompletionTracker)
     * or the console, relaying it, may raise this: a value set too high
     * makes a later phase 1 skip acceptors that may hold a chosen value. */
    public synchronized void setCompletedBallot (int ballot) {
	if (ballot > this.completed_ballot)
	    this.completed_ballot = ballot;
	this.notifyAll();
    }

    
    public synchronized void setExecutedSlots (int n) {
	this.executed_slots = n;
	this.notifyAll();
    }

    public synchronized int getExecutedSlots () {
	return this.executed_slots;
    }

    public synchronized void waitForExecutedSlots (int n) {
	while (this.executed_slots < n) {
	    try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
    }

    public synchronized int waitForCompletedBallot(int ballot) {
        while (this.completed_ballot < ballot) {
            try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
	return this.completed_ballot;
    }

    public synchronized void openFastBallot (int ballot, int from) {
	if (ballot > this.fast_any_ballot) {
	    this.fast_any_ballot = ballot;
	    this.fast_any_from   = from;
	    this.notifyAll();
	}
    }

    /* A proposal can arrive before the leader's "any" because RPCs may be reordered.
     * Wait for that protocol event instead of resending the proposal after a timer. */
    public synchronized boolean waitForFastBallot (int ballot) {
	while (this.fast_any_ballot < ballot && this.current_ballot <= ballot) {
	    try {
		this.wait();
	    }
	    catch (InterruptedException e) {
		Thread.currentThread().interrupt();
		return false;
	    }
	}
	return this.fast_any_ballot == ballot;
    }

    public synchronized int getFastAnyBallot () {
	return this.fast_any_ballot;
    }

    public synchronized int getFastAnyFrom () {
	return this.fast_any_from;
    }

    public synchronized void finishRecovery (int abstain_upto) {
	this.abstain_upto = abstain_upto;
	this.recovering   = false;
	this.notifyAll();
    }

    /* Acceptor side: blocks while recovering, then tells whether this replica
     * may take part (promise or vote) in `ballot`. */
    public synchronized boolean mayParticipate (int ballot) {
	while (this.recovering) {
	    try {
		wait ();
	    }
	    catch (InterruptedException e) {
		Thread.currentThread().interrupt();
		return false;
	    }
	}
	return ballot > this.abstain_upto;
    }

    public synchronized void noteDecided (int instance) {
	if (instance > this.max_decided)
	    this.max_decided = instance;
    }

    public synchronized int getMaxDecided () {
	return this.max_decided;
    }

    /* State transfer, sending side: every position from `from` on that is
     * decided here and whose command body this replica has. */
    public List<DidaTradePaxos.DecidedCommand> decidedCommandsFrom (int from) {
	List<DidaTradePaxos.DecidedCommand> commands = new ArrayList<DidaTradePaxos.DecidedCommand>();
	for (PaxosInstance entry : this.paxos_log.snapshotFrom(from)) {
	    if (!entry.decided)
		continue;
	    DidaTradePaxos.DecidedCommand.Builder builder = DidaTradePaxos.DecidedCommand.newBuilder();
	    builder.setPosition(entry.instance_nb).setBallot(entry.accept_ballot).setCommandId(entry.decided_value);
	    if (entry.decided_value != MainLoop.NOOP) {
		RequestRecord record = this.req_history.getIfExists(entry.decided_value);
		if (record == null || record.getRequest() == null)
		    continue;   // decided, but the client's request never reached us
		DidaTradeCommand command = record.getRequest();
		builder.setAction(command.getAction().ordinal())
		    .setUserid(command.getUserId())
		    .setQuantity(command.getQuantity())
		    .setStock(command.getStock());
	    }
	    commands.add(builder.build());
	}
	return commands;
    }

    /* State transfer, receiving side: record each position as decided and
     * keep its command, so MainLoop can execute it like any other. Uses the
     * paxos_log lock, as the learner does, so the two never race on an entry. */
    public void installDecided (List<DidaTradePaxos.DecidedCommand> commands) {
	int installed = 0;
	synchronized (this.paxos_log) {
	    for (DidaTradePaxos.DecidedCommand command : commands) {
		PaxosInstance entry = this.paxos_log.testAndSetEntry(command.getPosition());
		int id = command.getCommandId();
		// The command body is needed even where the decision itself was
		// learned already: that is exactly the case of a client request
		// sent while this replica was down.
		if (id != MainLoop.NOOP && this.req_history.getIfExists(id) == null) {
		    DidaTradeAction action = DidaTradeAction.values()[command.getAction()];
		    DidaTradeCommand body = new DidaTradeCommand(action, command.getUserid(), command.getQuantity(), command.getStock());
		    this.req_history.addToPending(id, new RequestRecord(id, body));
		    if (entry.decided)
			installed++;   // decision already learned, only the command was missing
		}
		if (entry.decided)
		    continue;
		entry.accept_ballot = Math.max(entry.accept_ballot, command.getBallot());
		entry.decided_value = id;
		entry.decided       = true;
		this.noteDecided(command.getPosition());
		installed++;
	    }
	}
	if (installed > 0) {
	    System.out.println("State transfer: installed " + installed + " decided positions");
	    this.main_loop.wakeup();
	}
    }

    /* Whether this acceptor has ever voted, as reported to a recovering peer. */
    public boolean hasVotes () {
	for (PaxosInstance entry : this.paxos_log.snapshotFrom(0))
	    if (entry.write_ballot >= 0)
		return true;
	return false;
    }

    public synchronized boolean getFastPaxosMode () {
	return this.fastpaxos_on;
    }

    public synchronized void setFastPaxosMode (boolean mode) {
	this.fastpaxos_on = mode;
    }

    public synchronized int getDebugMode () {
	return this.debug_mode;
    }

    public synchronized void setDebugMode (int mode) {
	this.debug_mode = mode;
	if (mode != DEBUG_FREEZE)
	    this.notifyAll();
    }

    public synchronized boolean getSlowMode () {
	return this.slow_mode;
    }

    public synchronized void setSlowMode (boolean mode) {
	this.slow_mode = mode;
    }

    /**
     * debug gate placed at the head of every inbound client and Paxos request.
     *
     * freeze is applied first and the slow mode delay second, so that the
     * delay applies to the request when it is really being processed, the way
     * a slow machine would behave.
     *
     * This method is deliberately NOT synchronized. wait() releases the lock,
     * so freezing is safe, but Thread.sleep() would hold it for the whole
     * delay and block every other thread touching this object, the console
     * included. Both checks therefore take the lock briefly and release it
     * before anything sleeps.
     */
    public void debugGate () {
	this.waitWhileFrozen ();

	int delay = this.nextSlowDelay ();
	if (delay > 0) {
	    try {
		Thread.sleep (delay);
	    }
	    catch (InterruptedException e) {
		Thread.currentThread().interrupt();
	    }
	}
    }

    private synchronized void waitWhileFrozen () {
	while (this.debug_mode == DEBUG_FREEZE) {
	    try {
		this.wait();
	    }
	    catch (InterruptedException e) {
		Thread.currentThread().interrupt();
		return;
	    }
	}
    }

    /* Zero when slow mode is off, otherwise a random delay in milliseconds. */
    private synchronized int nextSlowDelay () {
	if (this.slow_mode == false)
	    return 0;
	return this.debug_rnd.nextInt (SLOW_MODE_MAX_MS);
    }

    /**
     * pretends this replica crashed, by killing the process.
	 * 
     * adds a small sleep to ensure the reply to the console has been sent.
	 * not adding this timeout caused the response to not leave sometimes
     *
     * halt() rather than exit() because halt() stops the process dead,
     * the way a real crash does. exit() would first run Java's tidy-up code.
     */
    public void crash () {
	System.out.println("Debug: crash requested by the console, going down in " + CRASH_DELAY_MS + "ms");

	Thread killer = new Thread (() -> {
		try {
		    Thread.sleep (CRASH_DELAY_MS);
		}
		catch (InterruptedException e) {
		}
		Runtime.getRuntime().halt (0);
	    });
	killer.setDaemon (false);
	killer.start ();
    }

}
