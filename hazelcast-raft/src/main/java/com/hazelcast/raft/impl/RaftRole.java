package com.hazelcast.raft.impl;

/**
 * Represents role of a member in a Raft group.
 * <p>
 * At any given time each member is in one of three roles: {@link #LEADER}, {@link #FOLLOWER},
 * or {@link #CANDIDATE}.
 * <p>
 * Normally, there is exactly one leader and all of the other members are followers.
 * But during leader election some of the followers can become candidates.
 */
public enum RaftRole {

    /**
     * Followers are passive, they issue no requests on their own
     * but simply respond to requests from leaders and candidates.
     */
    FOLLOWER,

    /**
     * Candidate is used to elect a new leader. When a candidate wins votes of majority, it becomes the leader.
     * Otherwise it becomes follower or candidate again.
     */
    CANDIDATE,

    /**
     * The leader handles all client requests (append entry, membership change etc) and replicates them to followers.
     */
    LEADER
}
