package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;

import java.util.Collection;

/*
    TODO [basri] we can use a single file to persist both persisted raft state fields and raft log.
    TODO [basri] localEndpoint, initialMembers, term, voteFor can be persisted to the file with custom keys.
    TODO [basri] currently there is no point in restoring raft state concurrently while it is being loaded from disk
    TODO [basri] because the fields other than the snapshot is already quite small and their restore procedure is basically
    TODO [basri] initializing some fields in Raft-related classes. Most likely the biggest state will be in the snapshot
    TODO [basri] and we already load it from disk first, then provide it to Raft.
 */
public class RestoredRaftState {

    public static RestoredRaftState initialState(RaftEndpoint localEndpoint, Collection<RaftEndpoint> initialMembers) {
        return new RestoredRaftState(localEndpoint, initialMembers, 0, null, 0, null, null, true);
    }

    private RaftEndpoint localEndpoint;

    private Collection<RaftEndpoint> initialMembers;

    private int term;

    private RaftEndpoint votedFor;

    private int lastVoteTerm;

    private SnapshotEntry snapshot;

    private LogEntry[] entries;

    private boolean initial;

    public RestoredRaftState(RaftEndpoint localEndpoint, Collection<RaftEndpoint> initialMembers, int term, RaftEndpoint votedFor,
            int lastVoteTerm, SnapshotEntry snapshot, LogEntry[] entries, boolean initial) {
        this.localEndpoint = localEndpoint;
        this.initialMembers = initialMembers;
        this.term = term;
        this.votedFor = votedFor;
        this.lastVoteTerm = lastVoteTerm;
        this.snapshot = snapshot;
        this.entries = entries;
        this.initial = initial;
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    public RestoredRaftState setLocalEndpoint(RaftEndpoint localEndpoint) {
        this.localEndpoint = localEndpoint;
        return this;
    }

    public Collection<RaftEndpoint> getInitialMembers() {
        return initialMembers;
    }

    public RestoredRaftState setInitialMembers(Collection<RaftEndpoint> initialMembers) {
        this.initialMembers = initialMembers;
        return this;
    }

    public int getTerm() {
        return term;
    }

    public RestoredRaftState setTerm(int term) {
        this.term = term;
        return this;
    }

    public RaftEndpoint getVotedFor() {
        return votedFor;
    }

    public RestoredRaftState setVotedFor(RaftEndpoint votedFor) {
        this.votedFor = votedFor;
        return this;
    }

    public int getLastVoteTerm() {
        return lastVoteTerm;
    }

    public RestoredRaftState setLastVoteTerm(int lastVoteTerm) {
        this.lastVoteTerm = lastVoteTerm;
        return this;
    }

    public SnapshotEntry getSnapshot() {
        return snapshot;
    }

    public RestoredRaftState setSnapshot(SnapshotEntry snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public LogEntry[] getEntries() {
        return entries;
    }

    public RestoredRaftState setEntries(LogEntry[] entries) {
        this.entries = entries;
        return this;
    }

    public boolean isInitial() {
        return initial;
    }
}
