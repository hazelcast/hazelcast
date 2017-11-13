package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.raft.impl.RaftUtil.getRole;
import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.raft.impl.service.RaftServiceUtil.getRaftNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HazelcastRaftTest extends HazelcastRaftTestSupport {

    @Test
    public void crashedLeader_cannotRecoverAndRejoinRaftGroup() throws Exception {
        Address[] raftAddresses = createAddresses(2);
        HazelcastInstance[] instances = newInstances(raftAddresses);

        RaftNodeImpl leader = waitAllForLeaderElection(instances, METADATA_GROUP_ID);

        HazelcastInstance leaderInstance = factory.getInstance(leader.getLocalEndpoint().getAddress());
        final HazelcastInstance followerInstance = getRandomFollowerInstance(instances, leader);

        leaderInstance.shutdown();

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNodeImpl raftNode = getRaftNode(followerInstance, METADATA_GROUP_ID);
                Assert.assertEquals(RaftRole.FOLLOWER, getRole(raftNode));
            }
        }, 3);

        final HazelcastInstance newInstance = factory.newHazelcastInstance(leader.getLocalEndpoint().getAddress(),
                createConfig(raftAddresses, raftAddresses.length));
        assertClusterSizeEventually(2, followerInstance);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNodeImpl raftNode = getRaftNode(followerInstance, METADATA_GROUP_ID);
                assertEquals(RaftRole.FOLLOWER, getRole(raftNode));

                raftNode = getRaftNode(newInstance, METADATA_GROUP_ID);
                assertEquals(RaftRole.FOLLOWER, getRole(raftNode));
            }
        }, 5);
    }

    @Test
    public void crashedFollower_cannotRecoverAndRejoinRaftGroup() throws Exception {
        Address[] raftAddresses = createAddresses(2);
        HazelcastInstance[] instances = newInstances(raftAddresses);

        final RaftNodeImpl leader = waitAllForLeaderElection(instances, METADATA_GROUP_ID);

        final HazelcastInstance leaderInstance = factory.getInstance(leader.getLocalEndpoint().getAddress());
        HazelcastInstance followerInstance = getRandomFollowerInstance(instances, leader);

        Address restartingAddress = getAddress(followerInstance);
        followerInstance.shutdown();

        final HazelcastInstance newInstance = factory.newHazelcastInstance(restartingAddress, createConfig(raftAddresses, raftAddresses.length));
        assertClusterSize(2, leaderInstance);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNodeImpl raftNode = getRaftNode(leaderInstance, METADATA_GROUP_ID);
                assertEquals(RaftRole.LEADER, getRole(raftNode));

                raftNode = getRaftNode(newInstance, METADATA_GROUP_ID);
                assertEquals(RaftRole.FOLLOWER, getRole(raftNode));
            }
        }, 5);
    }
}
