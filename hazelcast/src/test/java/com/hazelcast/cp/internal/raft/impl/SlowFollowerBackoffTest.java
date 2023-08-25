/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 * Portions Copyright (c) 2020, MicroRaft.
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
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.state.FollowerState;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder.newGroup;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SlowFollowerBackoffTest {
    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void when_majoritySlowFollowers_then_rejectNewAppends() {
        int uncommittedEntryCount = 10;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setUncommittedEntryCountToRejectNewAppends(uncommittedEntryCount);
        group = newGroup(3, config);
        group.start();
        RaftNode leader = group.waitUntilLeaderElected();

        // we are slowing down the followers
        // by making their Raft thread sleep for 3 seconds
        for (RaftNode follower : group.getNodesExcept(leader.getLocalMember())) {
            group.slowDownNode(follower.getLocalMember(), 3);
        }

        while (true) {
            // we are filling up the request buffer of the leader.
            // since the followers are slowed down, the leader won't be able to
            // keep up with the incoming request rate and after some time it
            // will start to fail new requests with CannotReplicateException
            CompletableFuture<Object> future = leader.replicate(new ApplyRaftRunnable("val"));
            sleepMillis(10);

            if (future.isCompletedExceptionally()) {
                try {
                    future.join();
                } catch (CompletionException e) {
                    assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
                    return;
                }
            }
        }
    }

    @Test
    public void when_slowFollower_then_checkBackoffAppendRequestsCount() {
        int appendRequestBackoffTimeout = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setAppendRequestBackoffTimeoutInMillis(appendRequestBackoffTimeout);
        group = newGroup(3, config);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftEndpoint slowFollower = followers[0].getLocalMember();
        AtomicInteger appendRequestsCounter = new AtomicInteger();

        // we are slowing down the follower
        // by making their Raft thread sleep for 3 seconds
        group.slowDownNode(slowFollower, 3);

        // track the AppendRequests count sent to slowFollower
        group.alterMessagesToMember(leader.getLocalMember(), slowFollower, o -> {
            if (o instanceof AppendRequest) {
                appendRequestsCounter.incrementAndGet();
            }
            return o;
        });

        // sent 3 messages
        leader.replicate(new ApplyRaftRunnable("val1"));
        leader.replicate(new ApplyRaftRunnable("val2"));
        leader.replicate(new ApplyRaftRunnable("val3"));

        FollowerState followerState = leader.state().leaderState().getFollowerState(slowFollower);
        // waiting for slowFollower to acknowledge receiving messages
        assertTrueEventually(() -> assertEquals(3, followerState.matchIndex()));

        /** With 50ms for appendRequestBackoffTimeout and 3sec for slowFollower delay,
         * there should be five AppendRequests at 0, 200, 400, 800 and 1000 milliseconds.
         * The next backoff period calculation logic {@link FollowerState#setAppendRequestBackoff}
         */
        assertEquals(5, appendRequestsCounter.get());
        assertEquals(5, followerState.flowControlSequenceNumber());
    }

    @Test
    public void when_slowFollowerAndOutdatedSuccessResponse_then_checkNoBackoffResetHappen() {
        int appendRequestBackoffTimeout = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setAppendRequestBackoffTimeoutInMillis(appendRequestBackoffTimeout);
        group = newGroup(3, config);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftEndpoint slowFollower = followers[0].getLocalMember();
        AtomicInteger appendRequestsCounter = new AtomicInteger();
        CountDownLatch countDownLatch = new CountDownLatch(2);

        // track the AppendRequests count sent to slowFollower
        group.alterMessagesToMember(leader.getLocalMember(), slowFollower, o -> {
            if (o instanceof AppendRequest) {
                appendRequestsCounter.incrementAndGet();
                countDownLatch.countDown();
            }
            return o;
        });

        // we are slowing down the follower
        // by making their Raft thread sleep for 3 seconds
        group.slowDownNode(slowFollower, 3);

        // emulate receiving AppendSuccessResponse with outdated flowControlSequenceNumber
        new Thread(() -> {
            try {
                // wait until the current flowControlSequenceNumber will be equal 2
                countDownLatch.await();
                leader.handleAppendResponse(
                        // send response with outdated flowControlSequenceNumber equal 1
                        new AppendSuccessResponse(slowFollower, leader.state().term(), 0, 0, 1));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        leader.replicate(new ApplyRaftRunnable("val"));

        FollowerState followerState = leader.state().leaderState().getFollowerState(slowFollower);
        // waiting for slowFollower to acknowledge receiving messages
        assertTrueEventually(() -> assertEquals(1, followerState.matchIndex()));

        // verify that outdated response didn't affect the AppendRequest's count
        assertEquals(5, appendRequestsCounter.get());
        assertEquals(5, followerState.flowControlSequenceNumber());
    }

    @Test
    public void when_slowFollowerAndOutdatedFailureResponse_then_checkNoBackoffResetHappen() {
        int appendRequestBackoffTimeout = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setAppendRequestBackoffTimeoutInMillis(appendRequestBackoffTimeout);
        group = newGroup(3, config);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftEndpoint slowFollower = followers[0].getLocalMember();
        AtomicInteger appendRequestsCounter = new AtomicInteger();
        CountDownLatch countDownLatch = new CountDownLatch(2);

        // track the AppendRequests count sent to slowFollower
        group.alterMessagesToMember(leader.getLocalMember(), slowFollower, o -> {
            if (o instanceof AppendRequest) {
                appendRequestsCounter.incrementAndGet();
                countDownLatch.countDown();
            }
            return o;
        });

        // we are slowing down the follower
        // by making their Raft thread sleep for 3 seconds
        group.slowDownNode(slowFollower, 3);

        // emulate receiving AppendSuccessResponse with outdated flowControlSequenceNumber
        new Thread(() -> {
            try {
                // wait until the current flowControlSequenceNumber will be equal 2
                countDownLatch.await();
                leader.handleAppendResponse(
                        // send response with outdated flowControlSequenceNumber equal 1
                        new AppendFailureResponse(slowFollower, leader.state().term(), 0, 1));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        leader.replicate(new ApplyRaftRunnable("val"));

        FollowerState followerState = leader.state().leaderState().getFollowerState(slowFollower);
        // waiting for slowFollower to acknowledge receiving messages
        assertTrueEventually(() -> assertEquals(1, followerState.matchIndex()));

        // verify that outdated response didn't affect the AppendRequest's count
        assertEquals(5, appendRequestsCounter.get());
        assertEquals(5, followerState.flowControlSequenceNumber());
    }


    @Test
    public void when_slowFollower_then_checkBackoffInstallSnapshotsCount() {
        int appendRequestBackoffTimeout = 100;
        int commitIndexAdvanceCountToSnapshot = 5;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setAppendRequestBackoffTimeoutInMillis(appendRequestBackoffTimeout)
                .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);
        group = newGroup(3, config);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftEndpoint slowFollower = followers[0].getLocalMember();
        AtomicInteger installSnapshotCounter = new AtomicInteger();

        // we are slowing down the follower
        // by making their Raft thread sleep for 3 seconds
        group.slowDownNode(slowFollower, 3);

        // track the InstallSnapshots count sent to slowFollower
        group.alterMessagesToMember(leader.getLocalMember(), slowFollower, o -> {
            if (o instanceof InstallSnapshot) {
                installSnapshotCounter.incrementAndGet();
            }
            return o;
        });

        // sent 6 messages to trigger a snapshot
        leader.replicate(new ApplyRaftRunnable("val1"));
        leader.replicate(new ApplyRaftRunnable("val2"));
        leader.replicate(new ApplyRaftRunnable("val3"));
        leader.replicate(new ApplyRaftRunnable("val4"));
        leader.replicate(new ApplyRaftRunnable("val5"));
        leader.replicate(new ApplyRaftRunnable("val6"));

        FollowerState followerState = leader.state().leaderState().getFollowerState(slowFollower);
        // waiting for slowFollower to acknowledge receiving messages via snapshot
        assertTrueEventually(() -> assertEquals(6, followerState.matchIndex()));

        /**
         * With 100ms for appendRequestBackoffTimeout and 3sec for slowFollower delay,
         * there should be two InstallSnapshots at 0 and 2000 milliseconds.
         * The next backoff period calculation logic {@link FollowerState#setAppendRequestBackoff}
         * For InstallSnapshot the MAX_BACKOFF_ROUND is used.
         */
        assertEquals(2, installSnapshotCounter.get());
    }

}
