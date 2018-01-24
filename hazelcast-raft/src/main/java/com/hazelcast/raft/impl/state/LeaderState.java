package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Mutable state maintained by the leader of the Raft group. Leader keeps two indices for each server:
 * <ul>
 * <li>{@code nextIndex}: index of the next log entry to send to that server
 * (initialized to leader's {@code lastLogIndex + 1})</li>
 * <li>{@code matchIndex}: index of highest log entry known to be replicated on server
 * (initialized to 0, increases monotonically)</li>
 * </ul>
 */
public class LeaderState {

    private final Map<RaftEndpoint, Long> nextIndices = new HashMap<RaftEndpoint, Long>();

    private final Map<RaftEndpoint, Long> matchIndices = new HashMap<RaftEndpoint, Long>();

    LeaderState(Collection<RaftEndpoint> remoteMembers, long lastLogIndex) {
        for (RaftEndpoint follower : remoteMembers) {
            nextIndices.put(follower, lastLogIndex + 1);
            matchIndices.put(follower, 0L);
        }
    }

    /**
     * Add a new follower with the leader's {@code lastLogIndex}. Follower's {@code nextIndex} will be set
     * to {@code lastLogIndex + 1} and {@code matchIndex} to 0.
     */
    public void add(RaftEndpoint follower, long lastLogIndex) {
        assertNotFollower(nextIndices, follower);
        nextIndices.put(follower, lastLogIndex + 1);
        matchIndices.put(follower, 0L);
    }

    /**
     * Removes a follower from leader maintained state.
     */
    public void remove(RaftEndpoint follower) {
        assertFollower(nextIndices, follower);
        nextIndices.remove(follower);
        matchIndices.remove(follower);
    }

    /**
     * Sets {@code nextIndex} for a known follower.
     */
    public void setNextIndex(RaftEndpoint follower, long index) {
        assertFollower(nextIndices, follower);
        assert index > 0 : "Invalid next index: " + index;
        nextIndices.put(follower, index);
    }

    /**
     * Sets {@code matchIndex} for a known follower.
     */
    public void setMatchIndex(RaftEndpoint follower, long index) {
        assertFollower(matchIndices, follower);
        assert index >= 0 : "Invalid match index: " + index;
        matchIndices.put(follower, index);
    }

    public Collection<Long> matchIndices() {
        return matchIndices.values();
    }

    /**
     * Returns the {@code nextIndex} for a known follower.
     */
    public long getNextIndex(RaftEndpoint follower) {
        assertFollower(nextIndices, follower);
        return nextIndices.get(follower);
    }

    /**
     * Returns the {@code matchIndex} for a known follower.
     */
    public long getMatchIndex(RaftEndpoint follower) {
        assertFollower(matchIndices, follower);
        return matchIndices.get(follower);
    }

    private void assertFollower(Map<RaftEndpoint, Long> indices, RaftEndpoint follower) {
        assert indices.containsKey(follower) : "Unknown follower " + follower;
    }

    private void assertNotFollower(Map<RaftEndpoint, Long> indices, RaftEndpoint follower) {
        assert !indices.containsKey(follower) : "Already known follower " + follower;
    }

}
