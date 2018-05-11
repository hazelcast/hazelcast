package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.config.raft.RaftMetadataGroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;

import static com.hazelcast.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.raft.impl.RaftUtil.waitUntilLeaderElected;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class HazelcastRaftTestSupport extends HazelcastTestSupport {

    protected TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createTestFactory();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected TestHazelcastInstanceFactory createTestFactory() {
        return createHazelcastInstanceFactory();
    }

    protected RaftNodeImpl waitAllForLeaderElection(final HazelcastInstance[] instances, final RaftGroupId groupId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftNodeImpl leaderNode = getLeaderNode(instances, groupId);
                int leaderTerm = getTerm(leaderNode);

                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    assertNotNull(raftNode);
                    assertEquals(leaderNode.getLocalMember(), getLeaderMember(raftNode));
                    assertEquals(leaderTerm, getTerm(raftNode));
                }
            }
        });

        return getLeaderNode(instances, groupId);
    }

    protected HazelcastInstance getRandomFollowerInstance(HazelcastInstance[] instances, RaftNodeImpl leader) {
        Address address = ((RaftMemberImpl) leader.getLocalMember()).getAddress();
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(address)) {
                return instance;
            }
        }
        throw new AssertionError("Cannot find non-leader instance!");
    }

    protected HazelcastInstance[] newInstances(int raftGroupSize) {
        return newInstances(raftGroupSize, raftGroupSize, 0);
    }

    protected HazelcastInstance[] newInstances(int raftGroupSize, int metadataGroupSize, int nonCpNodeCount) {
        if (nonCpNodeCount < 0) {
            throw new IllegalArgumentException("non-cp node count: " + nonCpNodeCount + " must be non-negative");
        }
        if (raftGroupSize < metadataGroupSize) {
            throw new IllegalArgumentException("Metadata group size cannot be bigger than raft group size");
        }

        int nodeCount = raftGroupSize + nonCpNodeCount;
        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            Config config = createConfig(raftGroupSize, metadataGroupSize);
            if (i < raftGroupSize) {
                config.getRaftConfig().getMetadataGroupConfig().setInitialRaftMember(true);
                instances[i] = factory.newHazelcastInstance(config);
            } else {
                instances[i] = factory.newHazelcastInstance(config);
            }
        }

        assertClusterSizeEventually(nodeCount, instances);

        return instances;
    }

    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = new Config();
        configureSplitBrainDelay(config);

        RaftMetadataGroupConfig metadataGroupConfig = new RaftMetadataGroupConfig()
                .setGroupSize(groupSize)
                .setMetadataGroupSize(metadataGroupSize);
        RaftConfig raftConfig = new RaftConfig().setMetadataGroupConfig(metadataGroupConfig);
        config.setRaftConfig(raftConfig);
        return config;
    }

    protected void configureSplitBrainDelay(Config config) {
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "15")
              .setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
    }

    protected RaftNodeImpl getLeaderNode(final HazelcastInstance[] instances, final RaftGroupId groupId) {
        return getRaftNode(getLeaderInstance(instances, groupId), groupId);
    }

    protected HazelcastInstance getLeaderInstance(final HazelcastInstance[] instances, final RaftGroupId groupId) {
        final RaftNodeImpl[] raftNodeRef = new RaftNodeImpl[1];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    if (raftNode != null) {
                        raftNodeRef[0] = raftNode;
                        return;
                    }
                }
                fail();
            }
        });

        RaftNodeImpl raftNode = raftNodeRef[0];
        waitUntilLeaderElected(raftNode);
        RaftMemberImpl leaderEndpoint = getLeaderMember(raftNode);

        for (HazelcastInstance instance : instances) {
            if (getAddress(instance).equals(leaderEndpoint.getAddress())) {
                return instance;
            }
        }

        throw new AssertionError();
    }

    protected HazelcastInstance getRandomFollowerInstance(final HazelcastInstance[] instances, final RaftGroupId groupId) {
        final RaftNodeImpl[] raftNodeRef = new RaftNodeImpl[1];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    if (raftNode != null) {
                        raftNodeRef[0] = raftNode;
                        return;
                    }
                }
                fail();
            }
        });

        RaftNodeImpl raftNode = raftNodeRef[0];
        waitUntilLeaderElected(raftNode);
        RaftMemberImpl leaderEndpoint = getLeaderMember(raftNode);

        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(leaderEndpoint.getAddress())) {
                return instance;
            }
        }

        throw new AssertionError();
    }

    protected RaftInvocationManager getRaftInvocationManager(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        return service.getInvocationManager();
    }

    public static RaftService getRaftService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
    }

    public static RaftNodeImpl getRaftNode(HazelcastInstance instance, RaftGroupId groupId) {
        return (RaftNodeImpl) getRaftService(instance).getRaftNode(groupId);
    }

    public static RaftGroupInfo getRaftGroupLocally(HazelcastInstance instance, RaftGroupId groupId) {
        return getRaftService(instance).getMetadataManager().getRaftGroup(groupId);
    }
}
