package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderState {

    private final Map<RaftEndpoint, Integer> nextIndices = new HashMap<RaftEndpoint, Integer>();

    private final Map<RaftEndpoint, Integer> matchIndices = new HashMap<RaftEndpoint, Integer>();

    public LeaderState(Collection<RaftEndpoint> remoteMembers, int lastLogIndex) {
        for (RaftEndpoint follower : remoteMembers) {
            nextIndices.put(follower, lastLogIndex + 1);
            matchIndices.put(follower, 0);
        }
    }

    public void setNextIndex(RaftEndpoint follower, int index) {
        assertFollower(nextIndices, follower);
        assert index > 0 : "Invalid next index: " + index;
        nextIndices.put(follower, index);
    }

    public void setMatchIndex(RaftEndpoint follower, int index) {
        assertFollower(matchIndices, follower);
        assert index >= 0 : "Invalid match index: " + index;
        matchIndices.put(follower, index);
    }

    public Collection<Integer> matchIndices() {
        return matchIndices.values();
    }

    public int getNextIndex(RaftEndpoint follower) {
        assertFollower(nextIndices, follower);
        return nextIndices.get(follower);
    }

    public int getMatchIndex(RaftEndpoint follower) {
        assertFollower(matchIndices, follower);
        return matchIndices.get(follower);
    }

    private void assertFollower(Map<RaftEndpoint, Integer> indices, RaftEndpoint follower) {
        assert indices.containsKey(follower) : "Unknown address " + follower;
    }

}
