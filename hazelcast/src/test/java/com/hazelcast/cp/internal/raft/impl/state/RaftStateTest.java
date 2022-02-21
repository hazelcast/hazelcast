/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftRole;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftGroupId;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.majority;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newRaftMember;
import static com.hazelcast.cp.internal.raft.impl.state.RaftState.newRaftState;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftStateTest {

    private RaftState state;
    private String name = randomName();
    private CPGroupId groupId;
    private TestRaftEndpoint localMember;
    private Collection<RaftEndpoint> members;

    @Before
    public void setup() {
        groupId = new TestRaftGroupId(name);
        localMember = newRaftMember(5000);
        members = new HashSet<RaftEndpoint>(asList(localMember,
                newRaftMember(5001),
                newRaftMember(5002),
                newRaftMember(5003),
                newRaftMember(5004)));

        state = newRaftState(groupId, localMember, members, 100);
    }

    @Test
    public void test_initialState() {
        assertEquals(name, state.name());
        assertEquals(groupId, state.groupId());
        assertEquals(members.size(), state.memberCount());

        assertEquals(members, state.members());

        Collection<RaftEndpoint> remoteMembers = new HashSet<RaftEndpoint>(members);
        remoteMembers.remove(localMember);
        assertEquals(remoteMembers, state.remoteMembers());

        assertEquals(0, state.term());
        assertEquals(RaftRole.FOLLOWER, state.role());
        assertNull(state.leader());
        assertEquals(0, state.commitIndex());
        assertEquals(0, state.lastApplied());
        assertEquals(3, state.majority());
        assertNull(state.votedFor());
        assertNull(state.leaderState());
        assertNull(state.candidateState());

        RaftLog log = state.log();
        assertEquals(0, log.lastLogOrSnapshotIndex());
        assertEquals(0, log.lastLogOrSnapshotTerm());
    }

    @Test
    public void test_Leader() {
        state.leader(localMember);
        assertEquals(localMember, state.leader());
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
        state.toFollower(term);
        state.persistVote(term, localMember);

        assertEquals(term, state.term());
        assertEquals(localMember, state.votedFor());
    }

    @Test
    public void toFollower_fromCandidate() {
        state.toCandidate(false);

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

        state.toCandidate(false);
        assertEquals(RaftRole.CANDIDATE, state.role());
        assertNull(state.leaderState());
        assertEquals(term + 1, state.term());
        assertEquals(localMember, state.votedFor());

        CandidateState candidateState = state.candidateState();
        assertNotNull(candidateState);
        assertEquals(state.majority(), candidateState.majority());
        assertFalse(candidateState.isMajorityGranted());
        assertEquals(1, candidateState.voteCount());
    }

    @Test
    public void toLeader_fromCandidate() {
        state.toCandidate(false);

        int term = state.term();
        RaftLog log = state.log();
        log.appendEntries(new LogEntry(term, 1, null), new LogEntry(term, 2, null), new LogEntry(term, 3, null));
        long lastLogIndex = log.lastLogOrSnapshotIndex();

        state.toLeader();

        assertEquals(RaftRole.LEADER, state.role());
        assertEquals(localMember, state.leader());
        assertNull(state.candidateState());

        LeaderState leaderState = state.leaderState();
        assertNotNull(leaderState);

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            FollowerState followerState = leaderState.getFollowerState(endpoint);
            assertEquals(0, followerState.matchIndex());
            assertEquals(lastLogIndex + 1, followerState.nextIndex());
        }

        long[] matchIndices = leaderState.matchIndices();
        assertEquals(state.remoteMembers().size() + 1, matchIndices.length);
        for (long index : matchIndices) {
            assertEquals(0, index);
        }
    }

    @Test
    public void isKnownEndpoint() {
        for (RaftEndpoint endpoint : members) {
            assertTrue(state.isKnownMember(endpoint));
        }

        assertFalse(state.isKnownMember(newRaftMember(1234)));
        assertFalse(state.isKnownMember(new TestRaftEndpoint(UUID.randomUUID(), localMember.getPort())));
        assertFalse(state.isKnownMember(new TestRaftEndpoint(localMember.getUuid(), 1234)));
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
        members = new HashSet<RaftEndpoint>();
        members.add(localMember);

        for (int i = 1; i < count; i++) {
            members.add(newRaftMember(1000 + i));
        }

        state = newRaftState(groupId, localMember, members, 100);

        assertEquals(majority(count), state.majority());
    }
}
