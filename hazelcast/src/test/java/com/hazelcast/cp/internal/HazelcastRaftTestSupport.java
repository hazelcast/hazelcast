/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.waitUntilLeaderElected;
import static com.hazelcast.internal.util.Clock.currentTimeMillis;
import static com.hazelcast.spi.properties.ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.test.Accessors.getAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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

    protected static RaftNodeImpl waitAllForLeaderElection(HazelcastInstance[] instances, CPGroupId groupId) {
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
        return newInstances(cpNodeCount, groupSize, nonCpNodeCount, () -> createConfig(cpNodeCount, groupSize));
    }

    protected HazelcastInstance[] newInstances(int cpNodeCount, int groupSize, int nonCpNodeCount,
                                               Supplier<Config> configSupplier) {
        if (nonCpNodeCount < 0) {
            throw new IllegalArgumentException("non-cp node count: " + nonCpNodeCount + " must be non-negative");
        }
        if (cpNodeCount < groupSize) {
            throw new IllegalArgumentException("Group size cannot be bigger than cp node count");
        }

        int nodeCount = cpNodeCount + nonCpNodeCount;
        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            Config config = configSupplier.get();
            instances[i] = newInstance(config);
        }

        assertClusterSizeEventually(nodeCount, instances);
        waitUntilCPDiscoveryCompleted(instances);

        return instances;
    }

    protected HazelcastInstance newInstance(Config config) {
        return factory.newHazelcastInstance(config);
    }

    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = new Config();
        config.getMetricsConfig().setEnabled(false);
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

    protected static RaftNodeImpl getLeaderNode(HazelcastInstance[] instances, CPGroupId groupId) {
        return getRaftNode(getLeaderInstance(instances, groupId), groupId);
    }

    protected static HazelcastInstance getLeaderInstance(HazelcastInstance[] instances, CPGroupId groupId) {
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
        RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
        assertNotNull(leaderEndpoint);

        for (HazelcastInstance instance : instances) {
            CPMember cpMember = instance.getCPSubsystem().getLocalCPMember();
            if (cpMember != null && leaderEndpoint.getUuid().equals(cpMember.getUuid())) {
                return instance;
            }
        }

        throw new AssertionError();
    }

    protected static HazelcastInstance getRandomFollowerInstance(HazelcastInstance[] instances, CPGroupId groupId) {
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
        RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
        assertNotNull(leaderEndpoint);

        for (HazelcastInstance instance : instances) {
            CPMember cpMember = instance.getCPSubsystem().getLocalCPMember();
            if (cpMember != null && !cpMember.getUuid().equals(leaderEndpoint.getUuid())) {
                return instance;
            }
        }

        throw new AssertionError();
    }

    protected HazelcastInstance getInstance(RaftEndpoint endpoint) {
        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            CPMember cpMember = instance.getCPSubsystem().getLocalCPMember();
            if (cpMember != null && cpMember.getUuid().equals(endpoint.getUuid())) {
                return instance;
            }
        }
        return null;
    }

    protected RaftInvocationManager getRaftInvocationManager(HazelcastInstance instance) {
        RaftService service = getRaftService(instance);
        return service.getInvocationManager();
    }

    public static RaftService getRaftService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
    }

    public static RaftNodeImpl getRaftNode(HazelcastInstance instance, CPGroupId groupId) {
        return (RaftNodeImpl) getRaftService(instance).getRaftNode(groupId);
    }

    public static CPGroupSummary queryRaftGroupLocally(HazelcastInstance instance, CPGroupId groupId) {
        RaftNodeImpl raftNode = getRaftNode(instance, getMetadataGroupId(instance));
        if (raftNode == null) {
            return null;
        }

        return (CPGroupSummary) raftNode.query(new GetRaftGroupOp(groupId), QueryPolicy.ANY_LOCAL).joinInternal();
    }

    public static CPGroupId getGroupId(HazelcastInstance instance, String name) {
        for (CPGroupId groupId : instance.getCPSubsystem().getCPGroupIds()) {
            if (groupId.getName().equals(name)) {
                return groupId;
            }
        }
        throw new IllegalStateException("No CPGroupID found with name " + name);
    }

    public static RaftGroupId getMetadataGroupId(HazelcastInstance instance) {
        return getRaftService(instance).getMetadataGroupId();
    }

    public static NodeEngineImpl getNodeEngineImpl(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.getNodeEngine();
    }

    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl hazelcastInstanceImpl = getHazelcastInstanceImpl(hz);
        return hazelcastInstanceImpl.node;
    }

    static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        if (hz instanceof HazelcastInstanceImpl impl) {
            return impl;
        } else if (hz instanceof HazelcastInstanceProxy proxy) {
            try {
                return proxy.getOriginal();
            } catch (HazelcastInstanceNotActiveException e) {
                // fall through
            }
        }
        throw new IllegalArgumentException("The given HazelcastInstance is not an active HazelcastInstanceImpl: " + hz.getClass());
    }

    /**
     * Leadership rebalancing utilizes RaftNode::LeadershipTransfer under the hood.
     * There is a small chance that it may not transfer to the target leader. This is
     * very rare and a result of requiring an election process. In such a scenario, our tests
     * may operate on the incorrect assumption that our highest priority node has become leader.
     * <p>
     * This method applies a deterministic retry to assert leadership is correct.
     *
     * @param instances all members
     * @param group the groupId
     * @param expectedLeader the target leader after rebalancing.
     */
    protected void rebalanceLeadershipUntilLeader(HazelcastInstance[] instances,
                                                  CPGroupId groupId,
                                                  HazelcastInstance expectedLeader) {
        UUID expectedUuid = cpUuid(expectedLeader);

        assertTrueEventually(() -> {
            HazelcastInstance currentLeader = getLeaderInstance(instances, groupId);
            if (expectedUuid.equals(cpUuid(currentLeader))) {
                return; // done
            }

            HazelcastInstance metadataLeader = getLeaderInstance(instances, getMetadataGroupId(instances[0]));
            CompletableFuture<Void> f = getRaftService(metadataLeader)
                    .getMetadataGroupManager()
                    .rebalanceGroupLeadershipsAsync();

            try {
                // Must be bounded; otherwise a stuck rebalance can hang the whole assertion iteration.
                f.get(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (java.util.concurrent.TimeoutException ignored) {
                // allow retry
            }
        });
    }

    /**
     * Attempts to transfer Raft leadership to the given member and verifies that
     * it actually becomes leader.
     * <p>
     * {@code transferLeadership(..)} is asynchronous and best-effort: completion
     * does not strictly guarantee that the target will win the election. Due to
     * election timing or concurrent state changes, leadership may temporarily
     * settle on another member.
     * <p>
     * This helper:
     * <ul>
     *   <li>Triggers a transfer attempt,</li>
     *   <li>Waits up to 10s for leadership to converge to the expected member,</li>
     *   <li>Retries via {@code assertTrueEventually} if convergence is not observed.</li>
     * </ul>
     *
     * @param invokerLeader current leader initiating the transfer
     * @param groupId CP group id
     * @param expectedLeader target leader
     * @param instances all cluster members
     */
    protected void transferUntilLeaderEventually(HazelcastInstance invokerLeader,
                                                 CPGroupId groupId,
                                                 HazelcastInstance expectedLeader,
                                                 HazelcastInstance[] instances) {
        UUID expectedUuid = cpUuid(expectedLeader);

        assertTrueEventually(() -> {
            // initiate transfer
            getRaftService(invokerLeader)
                    .transferLeadership(groupId, getRaftService(expectedLeader).getLocalCPMember());
            // 2) wait up to 10s for expected leader to take place, it is not guranteed it will
            // succeed first time. See LeadershipTransfer documentation for more details.
            long deadline = currentTimeMillis() + 10_000;
            AssertionError last = null;

            while (currentTimeMillis() < deadline) {
                try {
                    HazelcastInstance leader = getLeaderInstance(instances, groupId);
                    if (expectedUuid.equals(cpUuid(leader))) {
                        return;
                    }
                } catch (AssertionError e) {
                    last = e;
                }

                // small backoff
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted while waiting for leader", e);
                }
            }

            // If we didn't observe it within the window, fail this iteration so assertTrueEventually retries (and re-transfers)
            if (last != null) {
                throw last;
            }
            throw new AssertionError("Leader did not become expected member " + expectedUuid + " for group " + groupId);
        });
    }

    protected UUID cpUuid(HazelcastInstance instance) {
        return instance.getCPSubsystem().getLocalCPMember().getUuid();
    }

    /**
     * Verifies that the given member does not become (or remain) leader.
     * <p>
     * Leadership transfer and rebalancing rely on elections and are therefore
     * not strictly deterministic. In rare timing scenarios, a lower-priority
     * member may temporarily win leadership. Instead of asserting a specific
     * leader, this helper asserts that a particular member is eventually
     * not the leader of the given CP group.
     *
     * @param notExpectedLeader member that must not be leader
     * @param instances all cluster members
     * @param groupId CP group id
     */
    protected void assertNotLeaderEventually(HazelcastInstance notExpectedLeader,
                                             HazelcastInstance[] instances,
                                             CPGroupId groupId) {
        UUID notExpectedUuid = cpUuid(notExpectedLeader);
        assertTrueEventually(() -> {
            HazelcastInstance leader = getLeaderInstance(instances, groupId);
            assertNotEquals(notExpectedUuid, cpUuid(leader));
        });
    }
}
