package didatrade.configs;

import java.util.*;
import didatrade.configs.Schedule;


public class ScheduleC extends Schedule {
    private List<Integer> all_participants = Arrays.asList(0, 1, 2, 3, 4, 5, 6);
    private List<Integer> all_learners     = Arrays.asList(0, 1, 2, 3, 4, 5, 6);
    private List<Integer> acceptors_start  = Arrays.asList(0, 1, 2, 3, 4);
    private List<Integer> acceptors_end    = Arrays.asList(2, 3, 4, 5, 6);
    private List<Integer> acceptors_all    = Arrays.asList(0, 1, 2, 3, 4, 5, 6);
	
    public ScheduleC() {
    }
   
    public List<Integer> learners(int ballot) {
	return this.all_learners;
    }
    
    public List<Integer> acceptors(int ballot) {
	if (ballot < 2)
	    return this.acceptors_start;
	else
	    return this.acceptors_end;
    }
  
    public List<Integer> acceptorsinrange(int low_ballot, int high_ballot) {
	if (high_ballot < 2)
	    return this.acceptors_start;
	else if (low_ballot >= 2)
	    return this.acceptors_end;
	else
	    return this.acceptors_all;
    }
 
    public List<Integer> allparticipantsinballot(int ballot) {
	return this.all_participants;
    }
    
    public List<Integer> allparticipants() {
	return this.all_participants;
    }

    public boolean isacceptor (int node, int ballot) {
	if (ballot < 2)
	    return this.acceptors_start.contains(node);
	else 
	    return this.acceptors_end.contains(node);
    }
      
    public Integer leader (int ballot) {
	if (ballot < 2)
	    return new Integer(ballot);
	else if (ballot < 5)
	    return new Integer(ballot+1);
	else
	    return new Integer(5);
    }
        
    /* Both acceptor sets have 5 members, so a majority is 3 in every ballot.
     * (2 of 5 would let two disjoint quorums decide different values.) */
    public Integer quorum (int ballot) {
	return new Integer(3);
    }

    /* Same 5-acceptor numbers as schedule B: fast 4 of 5, recovery 3 of 5. */
    public Integer fastquorum (int ballot) {
	return new Integer(4);
    }

    public Integer recoveryquorum (int ballot) {
	return new Integer(3);
    }

    public boolean fastpaxos(int ballot) {
	return ((ballot == 0)||(ballot ==2));
    }
}
