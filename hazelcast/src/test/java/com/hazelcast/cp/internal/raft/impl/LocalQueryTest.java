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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.QueryRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder.newGroup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalQueryTest extends HazelcastTestSupport {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void when_queryFromLeader_withoutAnyCommit_thenReturnDefaultValue() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object o = leader.query(new QueryRaftRunnable(), QueryPolicy.LEADER_LOCAL).get();
        assertNull(o);
    }

    @Test
    public void when_queryFromFollower_withoutAnyCommit_thenReturnDefaultValue() throws Exception {
        group = newGroup(3);
        group.start();

        group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        Object o = follower.query(new QueryRaftRunnable(), QueryPolicy.ANY_LOCAL).get();
        assertNull(o);
    }

    @Test
    public void when_queryFromLeader_onStableCluster_thenReadLatestValue() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(new ApplyRaftRunnable("value" + i)).get();
        }

        Object result = leader.query(new QueryRaftRunnable(), QueryPolicy.LEADER_LOCAL).get();
        assertEquals("value" + count, result);
    }

    @Test(expected = NotLeaderException.class)
    public void when_queryFromFollower_withLeaderLocalPolicy_thenFail() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("value")).get();

        RaftNodeImpl follower = group.getAnyFollowerNode();

        follower.query(new QueryRaftRunnable(), QueryPolicy.LEADER_LOCAL).joinInternal();
    }

    @Test
    public void when_queryFromFollower_onStableCluster_thenReadLatestValue() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(new ApplyRaftRunnable("value" + i)).get();
        }

        String latestValue = "value" + count;
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                Object result = follower.query(new QueryRaftRunnable(), QueryPolicy.ANY_LOCAL).get();
                assertEquals(latestValue, result);
            }
        });
    }

    @Test
    public void when_queryFromSlowFollower_thenReadStaleValue() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl slowFollower = group.getAnyFollowerNode();

        Object firstValue = "value1";
        leader.replicate(new ApplyRaftRunnable(firstValue)).get();
        long leaderCommitIndex = getCommitIndex(leader);

        assertTrueEventually(() -> assertEquals(leaderCommitIndex, getCommitIndex(slowFollower)));

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        leader.replicate(new ApplyRaftRunnable("value2")).get();

        Object result = slowFollower.query(new QueryRaftRunnable(), QueryPolicy.ANY_LOCAL).get();
        assertEquals(firstValue, result);
    }

    @Test
    public void when_queryFromSlowFollower_thenEventuallyReadLatestValue() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("value1")).get();

        RaftNodeImpl slowFollower = group.getAnyFollowerNode();
        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        Object lastValue = "value2";
        leader.replicate(new ApplyRaftRunnable(lastValue)).get();

        group.allowAllMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember());

        assertTrueEventually(() -> {
            Object result = slowFollower.query(new QueryRaftRunnable(), QueryPolicy.ANY_LOCAL).get();
            assertEquals(lastValue, result);
        });
    }

    @Test
    public void when_queryFromSplitLeader_thenReadStaleValue() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object firstValue = "value1";
        leader.replicate(new ApplyRaftRunnable(firstValue)).get();
        long leaderCommitIndex = getCommitIndex(leader);

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertEquals(leaderCommitIndex, getCommitIndex(node));
            }
        });

        RaftNodeImpl followerNode = group.getAnyFollowerNode();
        group.split(leader.getLocalMember());

        assertTrueEventually(() -> {
            RaftEndpoint leaderEndpoint = getLeaderMember(followerNode);
            assertNotNull(leaderEndpoint);
            assertNotEquals(leader.getLocalMember(), leaderEndpoint);
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followerNode));
        Object lastValue = "value2";
        newLeader.replicate(new ApplyRaftRunnable(lastValue)).get();

        Object result1 = newLeader.query(new QueryRaftRunnable(), QueryPolicy.ANY_LOCAL).get();
        assertEquals(lastValue, result1);

        Object result2 = leader.query(new QueryRaftRunnable(), QueryPolicy.ANY_LOCAL).get();
        assertEquals(firstValue, result2);
    }

    @Test
    public void when_queryFromSplitLeader_thenEventuallyReadLatestValue() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object firstValue = "value1";
        leader.replicate(new ApplyRaftRunnable(firstValue)).get();
        long leaderCommitIndex = getCommitIndex(leader);

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertEquals(leaderCommitIndex, getCommitIndex(node));
            }
        });

        RaftNodeImpl followerNode = group.getAnyFollowerNode();
        group.split(leader.getLocalMember());

        assertTrueEventually(() -> {
            RaftEndpoint leaderEndpoint = getLeaderMember(followerNode);
            assertNotNull(leaderEndpoint);
            assertNotEquals(leader.getLocalMember(), leaderEndpoint);
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followerNode));
        Object lastValue = "value2";
        newLeader.replicate(new ApplyRaftRunnable(lastValue)).get();

        group.merge();

        assertTrueEventually(() -> {
            Object result = leader.query(new QueryRaftRunnable(), QueryPolicy.ANY_LOCAL).get();
            assertEquals(lastValue, result);
        });
    }
}
