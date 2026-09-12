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
    private int                 debug_mode;
    private boolean             fastpaxos_on;
 
    MainLoop                    main_loop;
    Thread                      main_loop_worker;
    
    public DidaTradeServerState(int port, int myself, char schedule) {
	this.trade_manager    = new TradeManager();
	this.scheduler        = new ConfigurationScheduler (schedule);
	this.base_port        = port;
	this.my_id            = myself;
	this.debug_mode       = 0;
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
	if (ballot > this.current_ballot)
	    this.current_ballot = ballot;
    }

    public synchronized int getCompletedBallot () {
	return this.completed_ballot;
    }

    public int findMaxDecidedBallot () {
	int ballot = -1;
	int length = this.paxos_log.length();

	for (int i=0; i< length; i++) {
	    PaxosInstance entry = this.paxos_log.getEntry(i);
	    if (entry == null)
		return ballot;
	    if (!entry.decided)
		return ballot;
	    else if (entry.accept_ballot > ballot)
		ballot = entry.accept_ballot;

	}
	return ballot;
    }
    
    public synchronized void updateCompletedBallot (int ballot) {
	// WARNING: THIS ONLY WORKS FOR CONFIGURATIONS WHERE THERE IS NO NEED FOR STATE-TRANSFER!!!!!
	// NEEDS TO BE UPDATE FOR THE PROJECT


	ballot = this.findMaxDecidedBallot ();
	if (ballot > this.completed_ballot)
	    this.completed_ballot = ballot;
	this.notifyAll();
    }

    
    public synchronized void setCompletedBallot (int ballot) {
	if (ballot > this.completed_ballot)
	    this.completed_ballot = ballot;
	this.notifyAll();
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

	/**
	 * Blocks an inbound client or Paxos request while this replica is frozen.
	 *
	 * The master service is deliberately not protected by this gate. That
	 * service must remain available so that the console can send mode 3 and
	 * release requests waiting here. Requests are kept in their gRPC handler
	 * threads rather than being discarded, so they continue normally after
	 * the replica is unfrozen.
	 */
	public synchronized void waitForUnfreeze () {
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

    /**
     * Pretends this replica crashed, by killing the process.
     *
     * Call this only after the reply to the console has been sent. Sending a
     * gRPC reply does not put it on the network straight away: it is queued,
     * and another thread writes it out a moment later. That is why we wait
     * before dying. If we killed the process immediately, the reply would
     * sometimes never leave, and the console would print "no reply received" --
     * which looks exactly like having typed the id of a replica that was
     * already down.
     *
     * We use halt() rather than exit() because halt() stops the process dead,
     * the way a real crash does. exit() would first run Java's tidy-up code,
     * which a machine losing power never gets to do.
     *
     * Killing this process also stops its container, because the replica is
     * what the container runs. Nothing brings it back automatically: restart it
     * with `dev.sh start <id>`.
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
	// A new thread copies the daemon flag of whoever created it, and gRPC's
	// handler threads are daemons -- daemon threads get abandoned when the JVM
	// shuts down. Making this one ordinary guarantees it lives long enough to
	// do its job.
	killer.setDaemon (false);
	killer.start ();
    }

}
