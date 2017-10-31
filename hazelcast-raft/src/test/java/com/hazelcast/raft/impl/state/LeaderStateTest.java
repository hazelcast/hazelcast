package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.raft.impl.RaftUtil.newRaftEndpoint;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LeaderStateTest {

    private LeaderState state;
    private Set<RaftEndpoint> remoteEndpoints;
    private int lastLogIndex;

    @Before
    public void setUp() throws Exception {
        lastLogIndex = 123;
        remoteEndpoints = new HashSet<RaftEndpoint>(asList(
                newRaftEndpoint(5001),
                newRaftEndpoint(5002),
                newRaftEndpoint(5003),
                newRaftEndpoint(5004)));

        state = new LeaderState(remoteEndpoints, lastLogIndex);
    }

    @Test
    public void test_initialState() throws Exception {
        for (RaftEndpoint endpoint : remoteEndpoints) {
            assertEquals(0, state.getMatchIndex(endpoint));
            assertEquals(lastLogIndex + 1, state.getNextIndex(endpoint));
        }

        Collection<Integer> matchIndices = state.matchIndices();
        assertEquals(remoteEndpoints.size(), matchIndices.size());
        for (int index : matchIndices) {
            assertEquals(0, index);
        }
    }

    @Test
    public void test_nextIndex() throws Exception {
        Map<RaftEndpoint, Integer> indices = new HashMap<RaftEndpoint, Integer>();
        for (RaftEndpoint endpoint : remoteEndpoints) {
            int index = RandomPicker.getInt(100);
            state.setNextIndex(endpoint, index);
            indices.put(endpoint, index);
        }

        for (RaftEndpoint endpoint : remoteEndpoints) {
            int index = indices.get(endpoint);
            assertEquals(index, state.getNextIndex(endpoint));
        }
    }

    @Test
    public void test_matchIndex() throws Exception {
        Map<RaftEndpoint, Integer> indices = new HashMap<RaftEndpoint, Integer>();
        for (RaftEndpoint endpoint : remoteEndpoints) {
            int index = RandomPicker.getInt(100);
            state.setMatchIndex(endpoint, index);
            indices.put(endpoint, index);
        }

        for (RaftEndpoint endpoint : remoteEndpoints) {
            int index = indices.get(endpoint);
            assertEquals(index, state.getMatchIndex(endpoint));
        }

        Collection<Integer> matchIndices = new HashSet<Integer>(state.matchIndices());
        assertEquals(new HashSet<Integer>(indices.values()), matchIndices);
    }

}
