package didatrade.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;


public class PaxosLog {
    private Hashtable<Integer, PaxosInstance>  log;
     
    public PaxosLog() {
	this.log = new Hashtable<Integer, PaxosInstance>();
    }

    public synchronized int length() {
        return this.log.size();
    }
    
    public synchronized PaxosInstance getEntry(int position) {
        return this.log.get(position);
    }

    /* entries at position >= from, in order of position. */
    public synchronized List<PaxosInstance> snapshotFrom(int from) {
        List<PaxosInstance> entries = new ArrayList<PaxosInstance>();
        for (PaxosInstance entry : this.log.values())
            if (entry.instance_nb >= from)
                entries.add(entry);
        Collections.sort(entries, new Comparator<PaxosInstance>() {
            @Override
            public int compare(PaxosInstance left, PaxosInstance right) {
                return Integer.compare(left.instance_nb, right.instance_nb);
            }
        });
        return entries;
    }
   
   
    public synchronized PaxosInstance testAndSetEntry(int position) {
	PaxosInstance entry = this.log.get(position);

	if (entry == null){
	    entry = new PaxosInstance(position);
	    this.log.put (position, entry);
	}
	return entry;
    }

    
    public synchronized PaxosInstance testAndSetEntry(int position, int ballot) {
	PaxosInstance entry = this.log.get(position);

	if (entry == null){
	    entry = new PaxosInstance(position, ballot);
	    this.log.put (position, entry);
	}
	return entry;
    }
}
