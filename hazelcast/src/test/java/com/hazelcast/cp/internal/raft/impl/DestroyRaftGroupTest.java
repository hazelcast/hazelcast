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

import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
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

import java.util.concurrent.ExecutionException;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getStatus;
import static com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder.newGroup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DestroyRaftGroupTest extends HazelcastTestSupport {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void when_destroyOpIsAppendedButNotCommitted_then_cannotAppendNewEntry() throws ExecutionException, InterruptedException {
        group = newGroup(2);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        group.dropAllMessagesToMember(leader.getLocalMember(), follower.getLocalMember());

        leader.replicate(new DestroyRaftGroupCmd());

        try {
            leader.replicate(new ApplyRaftRunnable("val")).joinInternal();
            fail();
        } catch (CannotReplicateException ignored) {
        }
    }

    @Test
    public void when_destroyOpIsAppended_then_statusIsTerminating() {
        group = newGroup(2);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        group.dropAllMessagesToMember(follower.getLocalMember(), leader.getLocalMember());

        leader.replicate(new DestroyRaftGroupCmd());

        assertTrueEventually(() -> {
            assertEquals(RaftNodeStatus.TERMINATING, getStatus(leader));
            assertEquals(RaftNodeStatus.TERMINATING, getStatus(follower));
        });
    }

    @Test
    public void when_destroyOpIsCommitted_then_raftNodeIsTerminated() throws ExecutionException, InterruptedException {
        group = newGroup(2);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        leader.replicate(new DestroyRaftGroupCmd()).get();

        assertTrueEventually(() -> {
            assertEquals(1, getCommitIndex(leader));
            assertEquals(1, getCommitIndex(follower));
            assertEquals(RaftNodeStatus.TERMINATED, getStatus(leader));
            assertEquals(RaftNodeStatus.TERMINATED, getStatus(follower));
        });

        try {
            leader.replicate(new ApplyRaftRunnable("val")).joinInternal();
            fail();
        } catch (NotLeaderException ignored) {
        }

        try {
            follower.replicate(new ApplyRaftRunnable("val")).joinInternal();
            fail();
        } catch (NotLeaderException ignored) {
        }
    }

    @Test
    public void when_destroyOpIsTruncated_then_statusIsActive() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        group.dropMessagesToAll(leader.getLocalMember(), AppendRequest.class);

        leader.replicate(new DestroyRaftGroupCmd());

        group.split(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
                assertNotNull(leaderEndpoint);
                assertNotEquals(leader.getLocalMember(), leaderEndpoint);
            }
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));

        for (int i = 0; i < 10; i++) {
            newLeader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        group.merge();

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
            }
        });
    }

}
