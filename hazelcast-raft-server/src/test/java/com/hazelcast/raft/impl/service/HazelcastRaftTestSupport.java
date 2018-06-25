package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftConfig;
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
import static org.junit.Assert.assertTrue;
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

    protected void waitUntilCPDiscoveryCompleted(final HazelcastInstance[] instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftService service = getRaftService(instance);
                    assertTrue(service.getMetadataGroupManager().isDiscoveryCompleted());
                }
            }
        });
    }

    protected HazelcastInstance[] newInstances(int cpNodeCount) {
        return newInstances(cpNodeCount, cpNodeCount, 0);
    }

    protected HazelcastInstance[] newInstances(int cpNodeCount, int groupSize, int nonCpNodeCount) {
        if (nonCpNodeCount < 0) {
            throw new IllegalArgumentException("non-cp node count: " + nonCpNodeCount + " must be non-negative");
        }
        if (cpNodeCount < groupSize) {
            throw new IllegalArgumentException("Group size cannot be bigger than cp node count");
        }

        int nodeCount = cpNodeCount + nonCpNodeCount;
        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            Config config = createConfig(cpNodeCount, groupSize);
            if (i < cpNodeCount) {
                instances[i] = factory.newHazelcastInstance(config);
            } else {
                instances[i] = factory.newHazelcastInstance(config);
            }
        }

        assertClusterSizeEventually(nodeCount, instances);

        return instances;
    }

    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = new Config();
        configureSplitBrainDelay(config);

        RaftConfig raftConfig = new RaftConfig();
        config.setRaftConfig(raftConfig);

        if (cpNodeCount > 0) {
            raftConfig.setCpNodeCount(cpNodeCount).setGroupSize(groupSize);
        }

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
        return getRaftService(instance).getMetadataGroupManager().getRaftGroup(groupId);
    }
}
