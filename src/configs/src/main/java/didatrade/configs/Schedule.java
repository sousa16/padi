package didatrade.configs;

import java.util.*;


public abstract class Schedule {

    abstract List<Integer> learners(int ballot);
    
    abstract List<Integer> acceptors(int ballot);

    abstract List<Integer> acceptorsinrange(int low_ballot, int high_ballot);

    abstract boolean isacceptor(int node, int ballot);

    abstract List<Integer> allparticipants ();
 
    abstract List<Integer> allparticipantsinballot (int ballot);
    
    abstract Integer leader (int ballot);

    /* Classic quorum: phase 2 acceptances needed in a classic ballot. */
    abstract Integer quorum (int ballot);

    /* Super-majority: votes for the same value needed to decide in a fast ballot. */
    abstract Integer fastquorum (int ballot);

    /* Recovery quorum: promises a new leader needs in phase 1 so that, together
     * with the fast quorum, it still sees every value a fast ballot may have chosen. */
    abstract Integer recoveryquorum (int ballot);

    abstract boolean fastpaxos(int ballot);
}
