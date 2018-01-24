package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.raft.impl.RaftUtil.newRaftEndpoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CandidateStateTest {

    private CandidateState state;
    private int majority;

    @Before
    public void setUp() throws Exception {
        majority = 3;
        state = new CandidateState(majority);
    }

    @Test
    public void test_initialState() throws Exception {
        assertEquals(majority, state.majority());
        assertEquals(0, state.voteCount());
        assertFalse(state.isMajorityGranted());
    }

    @Test
    public void test_grantVote_withoutMajority() throws Exception {
        RaftEndpoint endpoint = newRaftEndpoint(1000);

        assertTrue(state.grantVote(endpoint));
        assertFalse(state.grantVote(endpoint));

        assertEquals(1, state.voteCount());
        assertFalse(state.isMajorityGranted());
    }

    @Test
    public void test_grantVote_withMajority() throws Exception {
        for (int i = 0; i < majority; i++) {
            RaftEndpoint endpoint = newRaftEndpoint(1000 + i);
            assertTrue(state.grantVote(endpoint));

        }
        assertEquals(majority, state.voteCount());
        assertTrue(state.isMajorityGranted());
    }

}
