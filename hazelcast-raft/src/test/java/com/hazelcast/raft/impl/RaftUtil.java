package com.hazelcast.raft.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.service.RaftDataService;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.state.RaftGroupMembers;
import com.hazelcast.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.raft.impl.testing.TestRaftEndpoint;
import com.hazelcast.util.ExceptionUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.raft.impl.service.RaftDataService.SERVICE_NAME;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
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

    public static <T extends RaftEndpoint> T getLeaderEndpoint(final RaftNodeImpl node) {
        Callable<RaftEndpoint> task = new Callable<RaftEndpoint>() {
            @Override
            public RaftEndpoint call() {
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

    public static long getNextIndex(final RaftNodeImpl leader, final RaftEndpoint follower) {
        Callable<Long> task = new Callable<Long>() {
            @Override
            public Long call() {
                LeaderState leaderState = leader.state().leaderState();
                return leaderState.getNextIndex(follower);
            }
        };

        return readRaftState(leader, task);
    }

    public static long getMatchIndex(final RaftNodeImpl leader, final RaftEndpoint follower) {
        Callable<Long> task = new Callable<Long>() {
            @Override
            public Long call() {
                LeaderState leaderState = leader.state().leaderState();
                return leaderState.getMatchIndex(follower);
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

    public static void waitUntilLeaderElected(RaftNodeImpl node) {
        while (getLeaderEndpoint(node) == null) {
            sleepSeconds(1);
        }
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

    public static TestRaftEndpoint newRaftEndpoint(int port) {
        return new TestRaftEndpoint(randomString(), port);
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

    public static LocalRaftGroup newGroupWithService(int nodeCount, RaftConfig raftConfig) {
        return new LocalRaftGroup(nodeCount, raftConfig, SERVICE_NAME, RaftDataService.class);
    }
}
