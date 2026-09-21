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
    private int                 debug_mode;
    private boolean             slow_mode;
    private Random              debug_rnd;
    private boolean             fastpaxos_on;
 
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
