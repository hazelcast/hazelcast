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

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder;
import com.hazelcast.cp.internal.raft.impl.testing.NopEntry;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getRole;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newRaftMember;
import static com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder.newGroup;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LeadershipTransferTest extends HazelcastTestSupport {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testLeadershipTransferToLeaderItself() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.transferLeadership(leader.getLocalMember()).get();
    }

    @Test
    public void testCannotTransferLeadershipToNull() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        InternalCompletableFuture future = leader.transferLeadership(null);

        exceptionRule.expect(IllegalArgumentException.class);
        future.joinInternal();
    }

    @Test
    public void testCannotTransferLeadershipToInvalidEndpoint() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftEndpoint invalidEndpoint = newRaftMember(1000);
        InternalCompletableFuture future = leader.transferLeadership(invalidEndpoint);

        exceptionRule.expect(IllegalArgumentException.class);
        future.joinInternal();
    }

    @Test
    public void testFollowerCannotTransferLeadership() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalMember())[0];
        InternalCompletableFuture f = follower.transferLeadership(leader.getLocalMember());

        exceptionRule.expect(IllegalStateException.class);
        f.joinInternal();
    }

    @Test
    public void testCannotTransferLeadershipWhileChangingMemberList() throws Exception {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(30));
        group = new LocalRaftGroupBuilder(3, config).build();
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new NopEntry()).get();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);

        leader.replicateMembershipChange(followers[0].getLocalMember(), MembershipChangeMode.REMOVE);
        InternalCompletableFuture f = leader.transferLeadership(followers[0].getLocalMember());

        exceptionRule.expect(IllegalStateException.class);
        f.joinInternal();
    }

    @Test
    public void testTransferLeadershipWhenNoLogEntryAppended() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int term1 = getTerm(leader);

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalMember())[0];
        Future f = leader.transferLeadership(follower.getLocalMember());

        f.get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        int term2 = getTerm(newLeader);
        assertNotSame(leader, newLeader);
        assertTrue(term2 > term1);
        assertEquals(RaftRole.FOLLOWER, getRole(leader));
    }


    @Test
    public void testTransferLeadershipWhenEntriesAppended() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int term1 = getTerm(leader);

        leader.replicate(new NopEntry()).get();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalMember())[0];
        Future f = leader.transferLeadership(follower.getLocalMember());

        f.get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        int term2 = getTerm(newLeader);
        assertNotSame(leader, newLeader);
        assertTrue(term2 > term1);
        assertEquals(RaftRole.FOLLOWER, getRole(leader));
    }

    @Test
    public void testTransferLeadershipTimesOutWhenTargetCannotCatchupInTime() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalMember())[0];
        group.dropMessagesToMember(leader.getLocalMember(), follower.getLocalMember(), AppendRequest.class);

        leader.replicate(new NopEntry()).get();

        InternalCompletableFuture f = leader.transferLeadership(follower.getLocalMember());

        exceptionRule.expect(IllegalStateException.class);
        f.joinInternal();
    }

    @Test
    public void testDuplicateTransferLeadershipRequestsHandled() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalMember())[0];
        group.dropMessagesToMember(leader.getLocalMember(), follower.getLocalMember(), AppendRequest.class);

        leader.replicate(new NopEntry()).get();

        Future f1 = leader.transferLeadership(follower.getLocalMember());
        Future f2 = leader.transferLeadership(follower.getLocalMember());

        group.allowAllMessagesToMember(leader.getLocalMember(), follower.getLocalMember());

        f1.get();
        f2.get();
    }

    @Test
    public void testAnotherTransferLeadershipRequestFailsDuringOngoingLeadershipTransfer() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl follower1 = followers[0];
        RaftNodeImpl follower2 = followers[1];
        group.dropMessagesToMember(leader.getLocalMember(), follower1.getLocalMember(), AppendRequest.class);

        leader.replicate(new NopEntry()).get();

        leader.transferLeadership(follower1.getLocalMember());
        InternalCompletableFuture f = leader.transferLeadership(follower2.getLocalMember());

        exceptionRule.expect(IllegalStateException.class);
        f.joinInternal();
    }

    @Test
    public void testCannotReplicateNewEntryDuringLeadershipTransfer() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalMember())[0];
        group.dropMessagesToMember(leader.getLocalMember(), follower.getLocalMember(), AppendRequest.class);

        leader.replicate(new NopEntry()).get();
        leader.transferLeadership(follower.getLocalMember());

        InternalCompletableFuture f = leader.replicate(new NopEntry());

        exceptionRule.expect(CannotReplicateException.class);
        f.joinInternal();
    }

    @Test
    public void testOldLeaderCannotReplicateAfterLeadershipTransfer() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalMember())[0];

        leader.transferLeadership(follower.getLocalMember()).get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertNotSame(leader, newLeader);

        InternalCompletableFuture f = leader.replicate(new NopEntry());

        exceptionRule.expect(NotLeaderException.class);
        f.joinInternal();
    }

    @Test
    public void testNewLeaderCommitsAfterLeadershipTransfer() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalMember())[0];

        leader.transferLeadership(follower.getLocalMember()).get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertNotSame(leader, newLeader);

        Future f = newLeader.replicate(new NopEntry());
        f.get();
    }

    @Test
    public void testInflightOperationsCommittedByCurrentLeaderBeforeLeadershipTransfer() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);

        Future f1 = leader.replicate(new NopEntry());
        Future f2 = leader.transferLeadership(followers[0].getLocalMember());
        group.allowAllMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember());
        group.allowAllMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember());

        f2.get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertNotSame(leader, newLeader);

        f1.get();
    }

}
