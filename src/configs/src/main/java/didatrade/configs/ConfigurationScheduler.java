package didatrade.configs;

import java.util.*;
import didatrade.configs.Schedule;
import didatrade.configs.ScheduleA;
import didatrade.configs.ScheduleB;

public class ConfigurationScheduler {
    private Schedule s;

    public ConfigurationScheduler() {
	s = new ScheduleA ();
    }

    public ConfigurationScheduler(char option) {
	if (option == 'C')
	    s = new ScheduleC ();
	else if (option == 'B')
	    s = new ScheduleB ();
	else 
	    s = new ScheduleA ();
    }

    public void setSchedule (char option) {
	if (option == 'A')
	    s = new ScheduleA ();
	else if (option == 'B')
	    s = new ScheduleB ();   
 	else if (option == 'C')
	    s = new ScheduleC ();   
    }
    
    public List<Integer> learners(int ballot) {
	return s.learners(ballot);
    }
    
    public List<Integer> acceptors(int ballot) {
	return s.acceptors(ballot);
    }
        
    public List<Integer> acceptorsinrange(int low_ballot, int high_ballot) {
	return s.acceptorsinrange(low_ballot, high_ballot);
    }

    public boolean isacceptor (int node, int ballot) {
	return s.isacceptor (node, ballot);
    }
    
    public Integer leader (int ballot) {
	return s.leader(ballot);
    }
    
    public List<Integer> allparticipantsinballot(int ballot) {
	return s.allparticipantsinballot(ballot);
    }
   
    public List<Integer> allparticipants() {
	return s.allparticipants();
    }
    
    public Integer quorum(int ballot) {
	return s.quorum(ballot);
    }

    public boolean fastpaxos(int ballot) {
	return s.fastpaxos(ballot);
    }

    /* Fast Paxos quorum: ceil(3n/4) of the ballot's acceptors (3 of 3 in schedule A). */
    public Integer fastquorum(int ballot) {
	int n = s.acceptors(ballot).size();
	return (3 * n + 3) / 4;
    }

    /* Votes a learner needs to see for the same value before it is decided. */
    public Integer decisionquorum(int ballot) {
	return s.fastpaxos(ballot) ? fastquorum(ballot) : s.quorum(ballot);
    }
}
