package didatrade.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import didatrade.DidaTradePaxos;

import didatrade.configs.ConfigurationScheduler;

/**
 * decides when Paxos phase 1 is over, and what it found.
 *
 * succeeds once enough acceptors have promised. 
 * enough" = majority of the acceptors of every ballot from low_ballot to high_ballot
 * 
 * ballots before low_ballot are known to be finished, so they need no checking.
 * in schedules A and B every ballot has the same acceptors, so this is simply a majority of that set.
 * in schedule C, different ballots have different acceptors, so we need a majority of all groups of acceptors.
 *
 * phase 1 fails as soon as one acceptor refuses, because that acceptor has already promised a higher ballot.
 * it also fails if every acceptor has answered, or failed to answer, without a quorum being reached.
 */
public class PhaseOneProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply>  {
    private ConfigurationScheduler   scheduler;
    private boolean                  accepted;
    private int                      value;
    private int                      valballot;
    private int                      maxballot;
    private int                      low_ballot;
    private int                      high_ballot;
    private Set<Integer>             promised;
   
    
    public PhaseOneProcessor (ConfigurationScheduler s, int l, int h) {
	// false until a quorum has promised: if the replies run out first, the
	// leader must treat phase 1 as failed
	this.accepted    = false;
	this.value       = -1;
	this.valballot   = -1;
	this.maxballot   = -1;
	this.low_ballot  = l;
	this.high_ballot = h;
	this.scheduler   = s;
	this.promised    = new HashSet<Integer>();
    }

    public synchronized boolean getAccepted() {
	return this.accepted;
    }
    
    public synchronized int getValue() {
	return this.value;
    }
    
    public synchronized int getValballot() {
	return this.valballot;
    }
    
    public synchronized int getMaxballot() {
	return this.maxballot;
    }

    /* called once per reply. Returns true when phase 1 is decided, either way. */
    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses, DidaTradePaxos.PhaseOneReply last_response){
        if (last_response.getMaxballot() > this.maxballot)
            this.maxballot = last_response.getMaxballot();      // remember the highest promise it has seen

        // this acceptor has promised a higher ballot: we have lost, stop now
        if (last_response.getAccepted() == false) {
            this.accepted = false;
            return true;
        }

        this.promised.add(last_response.getServerid());         // acceptor said yes, record the yes

        // remember the value accepted in the highest ballot seen so far; if
        // there is one, the leader must propose it instead of its own
        if (last_response.getValballot() > this.valballot) {
            this.valballot = last_response.getValballot();
            this.value     = last_response.getValue();
        }

        if (this.hasQuorum()) {
            this.accepted = true;
            return true;
        }
        return false;
    }

    /* true when, for every ballot in range, a majority of its acceptors has promised. */
    private boolean hasQuorum() {
	int from = Math.min(this.low_ballot, this.high_ballot);

	for (int ballot = from; ballot <= this.high_ballot; ballot++) {
	    int count = 0;
	    for (Integer acceptor : this.scheduler.acceptors(ballot))
		if (this.promised.contains(acceptor))
		    count++;
	    if (count < this.scheduler.quorum(ballot))
		return false;
	}
	return true;
    }
}
