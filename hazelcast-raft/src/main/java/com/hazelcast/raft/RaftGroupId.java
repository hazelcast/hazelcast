package com.hazelcast.raft;

/**
 * Identifier for a specific Raft group. Each Raft group is denoted by
 * a unique name and a commitIndex in Raft log.
 */
public interface RaftGroupId {

    /**
     * Returns the name of the Raft group.
     */
    String name();

    /**
     * Returns the commit index of the log when this Raft group is created.
     */
    long commitIndex();
}
