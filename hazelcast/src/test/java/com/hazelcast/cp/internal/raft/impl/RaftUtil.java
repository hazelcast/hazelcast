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

import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.cp.internal.raft.impl.state.RaftGroupMembers;
import com.hazelcast.cp.internal.raft.impl.testing.InMemoryRaftStateStore;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftEndpoint;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.UuidUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class RaftUtil {

    public static RaftRole getRole(RaftNodeImpl node) {
        Callable<RaftRole> task = () -> node.state().role();
        return readRaftState(node, task);
    }

    public static <T extends RaftEndpoint> T getLeaderMember(final RaftNodeImpl node) {
        Callable<RaftEndpoint> task = () -> node.state().leader();
        return (T) readRaftState(node, task);
    }

    public static LogEntry getLastLogOrSnapshotEntry(RaftNodeImpl node) {
        Callable<LogEntry> task = () -> node.state().log().lastLogOrSnapshotEntry();

        return readRaftState(node, task);
    }

    public static LogEntry getSnapshotEntry(RaftNodeImpl node) {
        Callable<LogEntry> task = () -> node.state().log().snapshot();

        return readRaftState(node, task);
    }

    public static long getCommitIndex(RaftNodeImpl node) {
        Callable<Long> task = () -> node.state().commitIndex();

        return readRaftState(node, task);
    }

    public static long getLastApplied(final RaftNodeImpl node) {
        Callable<Long> task = () -> node.state().lastApplied();
        return readRaftState(node, task);
    }

    public static int getTerm(RaftNodeImpl node) {
        Callable<Integer> task = () -> node.state().term();

        return readRaftState(node, task);
    }

    public static long getMatchIndex(RaftNodeImpl leader, RaftEndpoint follower) {
        Callable<Long> task = () -> {
            LeaderState leaderState = leader.state().leaderState();
            return leaderState.getFollowerState(follower).matchIndex();
        };

        return readRaftState(leader, task);
    }

    public static long getLeaderQueryRound(RaftNodeImpl leader) {
        Callable<Long> task = () -> {
            LeaderState leaderState = leader.state().leaderState();
            assertNotNull(leader.getLocalMember() + " has no leader state!", leaderState);
            return leaderState.queryRound();
        };

        return readRaftState(leader, task);
    }

    public static RaftNodeStatus getStatus(RaftNodeImpl node) {
        Callable<RaftNodeStatus> task = node::getStatus;

        return readRaftState(node, task);
    }

    public static RaftGroupMembers getLastGroupMembers(RaftNodeImpl node) {
        Callable<RaftGroupMembers> task = () -> node.state().lastGroupMembers();

        return readRaftState(node, task);
    }

    public static RaftGroupMembers getCommittedGroupMembers(RaftNodeImpl node) {
        Callable<RaftGroupMembers> task = () -> node.state().committedGroupMembers();

        return readRaftState(node, task);
    }

    public static RaftEndpoint getVotedFor(RaftNodeImpl node) {
        Callable<RaftEndpoint> task = () -> node.state().votedFor();

        return readRaftState(node, task);
    }

    public static void waitUntilLeaderElected(RaftNodeImpl node) {
        assertTrueEventually(() -> assertNotNull("Leader is null on " + node, getLeaderMember(node)));
    }

    private static <T> T readRaftState(RaftNodeImpl node, Callable<T> task) {
        FutureTask<T> futureTask = new FutureTask<>(task);
        node.execute(futureTask);
        try {
            return futureTask.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static TestRaftEndpoint newRaftMember(int port) {
        return new TestRaftEndpoint(UuidUtil.newUnsecureUUID(), port);
    }

    public static Address newAddress(int port) {
        try {
            return new Address(InetAddress.getByName("127.0.0.1"), port);
        } catch (UnknownHostException e) {
            fail("Could not create new Address: " + e.getMessage());
        }
        return null;
    }

    public static int majority(int count) {
        return count / 2 + 1;
    }

    public static int minority(int count) {
        return count - majority(count);
    }

    public static RestoredRaftState getRestoredState(final RaftNodeImpl node) {
        Callable<RestoredRaftState> task = () -> {
            InMemoryRaftStateStore store = (InMemoryRaftStateStore) node.state().stateStore();
            return store.toRestoredRaftState();
        };

        return readRaftState(node, task);
    }

    public static <T extends RaftStateStore> T getRaftStateStore(final RaftNodeImpl node) {
        Callable<RaftStateStore> task = () -> node.state().stateStore();
        return (T) readRaftState(node, task);
    }

}
