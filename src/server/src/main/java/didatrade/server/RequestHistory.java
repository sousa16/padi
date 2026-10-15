package didatrade.server;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set; // <-- O import que faltava para resolver o erro do Set!

public class RequestHistory {
    private Hashtable<Integer, RequestRecord> pending;
    private Hashtable<Integer, RequestRecord> processed;
          
    public RequestHistory() {
        this.pending = new Hashtable<Integer, RequestRecord>();
        this.processed = new Hashtable<Integer, RequestRecord>();
    }

    public synchronized RequestRecord getIfPending(int requestid) {
        Integer id = new Integer(requestid);
        return this.pending.get(id);
    }

    public synchronized RequestRecord getFirstPending() {
        Enumeration<Integer> pendingids = this.pending.keys();
        if (pendingids.hasMoreElements()) 
             return this.pending.get(pendingids.nextElement());
        else
             return null;
    }

    // A nova função para o Multi-Paxos
    public synchronized RequestRecord getFirstUnproposed(Set<Integer> proposed) {
        Enumeration<Integer> pendingids = this.pending.keys();
        while (pendingids.hasMoreElements()) {
            Integer id = pendingids.nextElement();
            if (!proposed.contains(id)) {
                return this.pending.get(id);
            }
        }
        return null;
    }

    public synchronized RequestRecord getIfProcessed(int requestid) {
        Integer id = new Integer(requestid);
        return this.processed.get(id);
    }

    public synchronized RequestRecord getIfExists(int requestid) {
        RequestRecord record;
        Integer id = new Integer(requestid);
        record = this.pending.get(id);
        if (record == null)
            record = this.processed.get(id);
        return record;
    }

    public synchronized void addToPending(int requestid, RequestRecord record) {
        Integer id = new Integer(requestid);
        this.pending.put(id, record);
    }

    /* Step 5: the command may already be here, brought by a state transfer
     * before the client's own copy arrived. Hand back that record (already
     * answered, if it was executed) rather than queueing the command twice. */
    public synchronized RequestRecord addIfAbsent(int requestid, RequestRecord record) {
        RequestRecord existing = getIfExists(requestid);
        if (existing != null)
            return existing;
        addToPending(requestid, record);
        return record;
    }

    public synchronized RequestRecord moveToProcessed(int requestid) {
        Integer id = new Integer(requestid);
        RequestRecord record = this.pending.remove(id);
        this.processed.put(id, record);
        return record;
    }
}
