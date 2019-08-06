package com.hazelcast.cp.internal;

import com.hazelcast.internal.metrics.Probe;

/**
 * Container object for single RaftNode metrics.
 */
public class RaftNodeMetrics {

    @Probe
    public volatile int term;

    @Probe
    public volatile long commitIndex;

    @Probe
    public volatile long lastApplied;

    @Probe
    public volatile long lastLogTerm;

    @Probe
    public volatile long snapshotIndex;

    @Probe
    public volatile long lastLogIndex;

    @Probe
    public volatile long availableLogCapacity;

    void update(int term, long commitIndex, long lastApplied, long lastLogTerm, long snapshotIndex,
            long lastLogIndex, long availableLogCapacity) {
        this.term = term;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.lastLogTerm = lastLogTerm;
        this.snapshotIndex = snapshotIndex;
        this.lastLogIndex = lastLogIndex;
        this.availableLogCapacity = availableLogCapacity;
    }
}
