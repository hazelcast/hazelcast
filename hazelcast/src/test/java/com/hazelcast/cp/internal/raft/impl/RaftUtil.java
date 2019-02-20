/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Endpoint;
import com.hazelcast.cp.internal.raft.impl.dataservice.RaftDataService;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.cp.internal.raft.impl.state.RaftGroupMembers;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftMember;
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.util.ExceptionUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.cp.internal.raft.impl.dataservice.RaftDataService.SERVICE_NAME;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class RaftUtil {

    public static RaftRole getRole(final RaftNodeImpl node) {
        Callable<RaftRole> task = new Callable<RaftRole>() {
            @Override
            public RaftRole call() {
                return node.state().role();
            }
        };
        return readRaftState(node, task);
    }

    public static <T extends Endpoint> T getLeaderMember(final RaftNodeImpl node) {
        Callable<Endpoint> task = new Callable<Endpoint>() {
            @Override
            public Endpoint call() {
                return node.state().leader();
            }
        };
        return (T) readRaftState(node, task);
    }

    public static LogEntry getLastLogOrSnapshotEntry(final RaftNodeImpl node) {
        Callable<LogEntry> task = new Callable<LogEntry>() {
            @Override
            public LogEntry call() {
                return node.state().log().lastLogOrSnapshotEntry();
            }
        };

        return readRaftState(node, task);
    }

    public static LogEntry getSnapshotEntry(final RaftNodeImpl node) {
        Callable<LogEntry> task = new Callable<LogEntry>() {
            @Override
            public LogEntry call() {
                return node.state().log().snapshot();
            }
        };

        return readRaftState(node, task);
    }

    public static long getCommitIndex(final RaftNodeImpl node) {
        Callable<Long> task = new Callable<Long>() {
            @Override
            public Long call() {
                return node.state().commitIndex();
            }
        };

        return readRaftState(node, task);
    }

    public static int getTerm(final RaftNodeImpl node) {
        Callable<Integer> task = new Callable<Integer>() {
            @Override
            public Integer call() {
                return node.state().term();
            }
        };

        return readRaftState(node, task);
    }

    public static long getMatchIndex(final RaftNodeImpl leader, final Endpoint follower) {
        Callable<Long> task = new Callable<Long>() {
            @Override
            public Long call() {
                LeaderState leaderState = leader.state().leaderState();
                return leaderState.getFollowerState(follower).matchIndex();
            }
        };

        return readRaftState(leader, task);
    }

    public static RaftNodeStatus getStatus(final RaftNodeImpl node) {
        Callable<RaftNodeStatus> task = new Callable<RaftNodeStatus>() {
            @Override
            public RaftNodeStatus call() {
                return node.getStatus();
            }
        };

        return readRaftState(node, task);
    }

    public static RaftGroupMembers getLastGroupMembers(final RaftNodeImpl node) {
        Callable<RaftGroupMembers> task = new Callable<RaftGroupMembers>() {
            @Override
            public RaftGroupMembers call() {
                return node.state().lastGroupMembers();
            }
        };

        return readRaftState(node, task);
    }

    public static RaftGroupMembers getCommittedGroupMembers(final RaftNodeImpl node) {
        Callable<RaftGroupMembers> task = new Callable<RaftGroupMembers>() {
            @Override
            public RaftGroupMembers call() {
                return node.state().committedGroupMembers();
            }
        };

        return readRaftState(node, task);
    }

    public static void waitUntilLeaderElected(final RaftNodeImpl node) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(getLeaderMember(node));
            }
        });
    }

    private static <T> T readRaftState(RaftNodeImpl node, Callable<T> task) {
        FutureTask<T> futureTask = new FutureTask<T>(task);
        node.execute(futureTask);
        try {
            return futureTask.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static TestRaftMember newRaftMember(int port) {
        return new TestRaftMember(randomString(), port);
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

    public static LocalRaftGroup newGroupWithService(int nodeCount, RaftAlgorithmConfig raftAlgorithmConfig) {
        return newGroupWithService(nodeCount, raftAlgorithmConfig, false);
    }

    public static LocalRaftGroup newGroupWithService(int nodeCount, RaftAlgorithmConfig raftAlgorithmConfig, boolean appendNopEntryOnLeaderElection) {
        return new LocalRaftGroup(nodeCount, raftAlgorithmConfig, SERVICE_NAME, RaftDataService.class, appendNopEntryOnLeaderElection);
    }
}
