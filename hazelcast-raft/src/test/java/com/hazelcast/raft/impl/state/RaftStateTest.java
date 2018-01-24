package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.testing.TestRaftEndpoint;
import com.hazelcast.raft.impl.testing.TestRaftGroupId;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;

import static com.hazelcast.raft.impl.RaftUtil.majority;
import static com.hazelcast.raft.impl.RaftUtil.newRaftEndpoint;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftStateTest {

    private RaftState state;
    private String name = randomName();
    private RaftGroupId groupId;
    private TestRaftEndpoint localEndpoint;
    private Collection<RaftEndpoint> endpoints;

    @Before
    public void setup() {
        groupId = new TestRaftGroupId(name);
        localEndpoint = newRaftEndpoint(5000);
        endpoints = new HashSet<RaftEndpoint>(asList(
                localEndpoint,
                newRaftEndpoint(5001),
                newRaftEndpoint(5002),
                newRaftEndpoint(5003),
                newRaftEndpoint(5004)));

        state = new RaftState(groupId, localEndpoint, endpoints);
    }

    @Test
    public void test_initialState() {
        assertEquals(name, state.name());
        assertEquals(groupId, state.groupId());
        assertEquals(endpoints.size(), state.memberCount());

        assertEquals(endpoints, state.members());

        Collection<RaftEndpoint> remoteMembers = new HashSet<RaftEndpoint>(endpoints);
        remoteMembers.remove(localEndpoint);
        assertEquals(remoteMembers, state.remoteMembers());

        assertEquals(0, state.term());
        Assert.assertEquals(RaftRole.FOLLOWER, state.role());
        assertNull(state.leader());
        assertEquals(0, state.commitIndex());
        assertEquals(0, state.lastApplied());
        assertEquals(3, state.majority());
        assertNull(state.votedFor());
        assertEquals(0, state.lastVoteTerm());
        assertNull(state.leaderState());
        assertNull(state.candidateState());

        RaftLog log = state.log();
        assertEquals(0, log.lastLogOrSnapshotIndex());
        assertEquals(0, log.lastLogOrSnapshotTerm());
    }

    @Test
    public void incrementTerm() {
        int term = state.incrementTerm();
        assertEquals(1, term);
        assertEquals(term, state.term());
    }

    @Test
    public void test_Leader() {
        state.leader(localEndpoint);
        assertEquals(localEndpoint, state.leader());
    }

    @Test
    public void test_commitIndex() {
        int ix = 123;
        state.commitIndex(ix);
        assertEquals(ix, state.commitIndex());
    }

    @Test
    public void test_lastApplied() {
        int last = 123;
        state.lastApplied(last);
        assertEquals(last, state.lastApplied());
    }

    @Test
    public void persistVote() {
        int term = 13;
        state.persistVote(term, localEndpoint);

        assertEquals(term, state.lastVoteTerm());
        assertEquals(localEndpoint, state.votedFor());
    }

    @Test
    public void toFollower_fromCandidate() {
        state.toCandidate();

        int term = 23;
        state.toFollower(term);

        assertEquals(term, state.term());
        assertEquals(RaftRole.FOLLOWER, state.role());
        assertNull(state.leader());
        assertNull(state.leaderState());
        assertNull(state.candidateState());
    }

    @Test
    public void toFollower_fromLeader() {
        state.toLeader();

        int term = 23;
        state.toFollower(term);

        assertEquals(term, state.term());
        assertEquals(RaftRole.FOLLOWER, state.role());
        assertNull(state.leader());
        assertNull(state.leaderState());
        assertNull(state.candidateState());
    }

    @Test
    public void toCandidate_fromFollower() {
        int term = 23;
        state.toFollower(term);

        state.toCandidate();
        assertEquals(RaftRole.CANDIDATE, state.role());
        assertNull(state.leaderState());
        assertEquals(term + 1, state.lastVoteTerm());
        assertEquals(localEndpoint, state.votedFor());

        CandidateState candidateState = state.candidateState();
        assertNotNull(candidateState);
        assertEquals(state.majority(), candidateState.majority());
        assertFalse(candidateState.isMajorityGranted());
        assertEquals(1, candidateState.voteCount());
    }

    @Test
    public void toLeader_fromCandidate() {
        state.toCandidate();

        int term = state.term();
        RaftLog log = state.log();
        log.appendEntries(new LogEntry(term, 1, null), new LogEntry(term, 2, null), new LogEntry(term, 3, null));
        long lastLogIndex = log.lastLogOrSnapshotIndex();

        state.toLeader();

        assertEquals(RaftRole.LEADER, state.role());
        assertEquals(localEndpoint, state.leader());
        assertNull(state.candidateState());

        LeaderState leaderState = state.leaderState();
        assertNotNull(leaderState);

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            assertEquals(0, leaderState.getMatchIndex(endpoint));
            assertEquals(lastLogIndex + 1, leaderState.getNextIndex(endpoint));
        }

        Collection<Long> matchIndices = leaderState.matchIndices();
        assertEquals(state.remoteMembers().size(), matchIndices.size());
        for (long index : matchIndices) {
            assertEquals(0, index);
        }
    }

    @Test
    public void isKnownEndpoint() {
        for (RaftEndpoint endpoint : endpoints) {
            assertTrue(state.isKnownEndpoint(endpoint));
        }

        assertFalse(state.isKnownEndpoint(newRaftEndpoint(1234)));
        assertFalse(state.isKnownEndpoint(new TestRaftEndpoint(randomString(), localEndpoint.getPort())));
        assertFalse(state.isKnownEndpoint(new TestRaftEndpoint(localEndpoint.getUid(), 1234)));
    }

    @Test
    public void test_majority_withOddMemberGroup() {
        test_majority(7);
    }

    @Test
    public void test_majority_withEvenMemberGroup() {
        test_majority(8);
    }

    private void test_majority(int count) {
        endpoints = new HashSet<RaftEndpoint>();
        endpoints.add(localEndpoint);

        for (int i = 1; i < count; i++) {
            endpoints.add(newRaftEndpoint(1000 + i));
        }

        state = new RaftState(groupId, localEndpoint, endpoints);

        assertEquals(majority(count), state.majority());
    }
}
