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

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.waitUntilLeaderElected;
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

    protected RaftNodeImpl waitAllForLeaderElection(HazelcastInstance[] instances, CPGroupId groupId) {
        assertTrueEventually(() -> {
            RaftNodeImpl leaderNode = getLeaderNode(instances, groupId);
            int leaderTerm = getTerm(leaderNode);

            for (HazelcastInstance instance : instances) {
                RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                assertNotNull(raftNode);
                assertEquals(leaderNode.getLocalMember(), getLeaderMember(raftNode));
                assertEquals(leaderTerm, getTerm(raftNode));
            }
        });

        return getLeaderNode(instances, groupId);
    }

    protected HazelcastInstance getRandomFollowerInstance(HazelcastInstance[] instances, RaftNodeImpl leader) {
        Address address = ((CPMemberInfo) leader.getLocalMember()).getAddress();
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(address)) {
                return instance;
            }
        }
        throw new AssertionError("Cannot find non-leader instance!");
    }

    public static void waitUntilCPDiscoveryCompleted(HazelcastInstance... instances) {
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                assertTrue(getRaftService(instance).isDiscoveryCompleted());
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
            instances[i] = factory.newHazelcastInstance(config);
        }

        assertClusterSizeEventually(nodeCount, instances);
        waitUntilCPDiscoveryCompleted(instances);

        return instances;
    }

    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = new Config();
        configureSplitBrainDelay(config);

        CPSubsystemConfig cpSubsystemConfig = new CPSubsystemConfig();
        config.setCPSubsystemConfig(cpSubsystemConfig);

        if (cpNodeCount > 0) {
            cpSubsystemConfig.setCPMemberCount(cpNodeCount).setGroupSize(groupSize);
        }

        return config;
    }

    protected void configureSplitBrainDelay(Config config) {
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "15")
              .setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
    }

    protected RaftNodeImpl getLeaderNode(HazelcastInstance[] instances, CPGroupId groupId) {
        return getRaftNode(getLeaderInstance(instances, groupId), groupId);
    }

    protected HazelcastInstance getLeaderInstance(HazelcastInstance[] instances, CPGroupId groupId) {
        RaftNodeImpl[] raftNodeRef = new RaftNodeImpl[1];
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                if (raftNode != null) {
                    raftNodeRef[0] = raftNode;
                    return;
                }
            }
            fail();
        });

        RaftNodeImpl raftNode = raftNodeRef[0];
        waitUntilLeaderElected(raftNode);
        CPMemberInfo leaderEndpoint = getLeaderMember(raftNode);

        for (HazelcastInstance instance : instances) {
            if (getAddress(instance).equals(leaderEndpoint.getAddress())) {
                return instance;
            }
        }

        throw new AssertionError();
    }

    protected HazelcastInstance getRandomFollowerInstance(HazelcastInstance[] instances, CPGroupId groupId) {
        RaftNodeImpl[] raftNodeRef = new RaftNodeImpl[1];
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                if (raftNode != null) {
                    raftNodeRef[0] = raftNode;
                    return;
                }
            }
            fail();
        });

        RaftNodeImpl raftNode = raftNodeRef[0];
        waitUntilLeaderElected(raftNode);
        CPMemberInfo leaderEndpoint = getLeaderMember(raftNode);

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

    public static RaftNodeImpl getRaftNode(HazelcastInstance instance, CPGroupId groupId) {
        return (RaftNodeImpl) getRaftService(instance).getRaftNode(groupId);
    }

    public static CPGroupInfo getRaftGroupLocally(HazelcastInstance instance, CPGroupId groupId) {
        return getRaftService(instance).getMetadataGroupManager().getGroup(groupId);
    }

    public static CPGroupId getMetadataGroupId(HazelcastInstance instance) {
        return getRaftService(instance).getMetadataGroupId();
    }
}
