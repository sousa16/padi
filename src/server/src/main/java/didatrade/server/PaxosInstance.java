package didatrade.server;

import java.util.*;


public class PaxosInstance {
    int instance_nb;
    int command_id;
    int read_ballot;
    int write_ballot;
    int accept_ballot;
    int n_accepts;
    boolean value_is_locked;

    /* Acceptor state is command_id/write_ballot (the vote). Learner state is
     * everything below: with several proposers in a fast ballot the two must
     * not share a field, or learning another acceptor's vote would overwrite
     * our own. accept_ballot is the ballot being tallied. */
    volatile boolean decided;
    volatile int decided_value;
    Map<Integer, Set<Integer>> votes;   // value -> acceptors that voted it, at accept_ballot

    public PaxosInstance() {
	this.instance_nb     = 0;
        this.command_id      = 0;
        this.read_ballot     = -1;
        this.write_ballot    = -1;
	this.accept_ballot   = -1;
        this.n_accepts       = 0;
	this.decided         = false;
        this.value_is_locked = false;
	this.decided_value   = -1;
	this.votes           = new HashMap<Integer, Set<Integer>>();
    }

    
    public PaxosInstance(int id) {
	this.instance_nb     = id;
        this.command_id      = 0;
        this.read_ballot     = -1;
        this.write_ballot    = -1;
	this.accept_ballot   = -1;
        this.n_accepts       = 0;
	this.decided         = false;
        this.value_is_locked = false;
	this.decided_value   = -1;
	this.votes           = new HashMap<Integer, Set<Integer>>();
    }

    public PaxosInstance(int id, int ballot) {
	this.instance_nb     = id;
        this.command_id      = 0;
        this.read_ballot     = ballot;
        this.write_ballot    = -1;
	this.accept_ballot   = -1;
        this.n_accepts       = 0;
	this.decided         = false;
        this.value_is_locked = false;
	this.decided_value   = -1;
	this.votes           = new HashMap<Integer, Set<Integer>>();
    }
}
