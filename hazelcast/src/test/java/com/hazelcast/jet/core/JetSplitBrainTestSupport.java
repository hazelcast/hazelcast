/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.server.FirewallingServer.FirewallingServerConnectionManager;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.util.stream.Collectors.toList;

/**
 * A support class for high-level split-brain tests. It will form a
 * cluster, create a split-brain situation, and then heal the cluster
 * again.
 * <p>
 * Tests are supposed to extend this class and use
 * {@link JetSplitBrainTestSupport#testSplitBrain(int, int, Consumer, BiConsumer, Consumer)} by providing
 * hooks to be notified about state transitions. All hooks are optional.
 * <p>
 * See {@link SplitBrainTest} for examples.
 */
public abstract class JetSplitBrainTestSupport extends JetTestSupport {

    static final int PARALLELISM = 4;

    private static final ILogger LOGGER = Logger.getLogger(JetSplitBrainTestSupport.class);

    /**
     * If new nodes have been created during split brain via
     * {@link #createHazelcastInstanceInBrain(JetInstance[], JetInstance[], boolean)}, then their joiners
     * are initialized with the other brain's addresses being blacklisted.
     */
    private boolean unblacklistHint;

    @Before
    public final void setUpInternals() {
        onBeforeSetup();
    }

    /**
     * Override this method to execute initialization that may be required
     * before instantiating the cluster. This is the
     * first method executed by {@code @Before SplitBrainTestSupport.setupInternals}.
     */
    protected void onBeforeSetup() {

    }

    private Config createConfig() {
        Config config = new Config();
        config.getJetConfig().getInstanceConfig().setCooperativeThreadCount(PARALLELISM);
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
        onConfigCreated(config);
        return config;
    }

    /**
     * Override this for custom Jet configuration
     */
    protected void onConfigCreated(Config config) {

    }

    final void testSplitBrain(int firstSubClusterSize, int secondSubClusterSize,
                              Consumer<JetInstance[]> beforeSplit,
                              BiConsumer<JetInstance[], JetInstance[]> onSplit,
                              Consumer<JetInstance[]> afterMerge) {
        checkPositive(firstSubClusterSize, "invalid first sub cluster size: " + firstSubClusterSize);
        checkPositive(secondSubClusterSize, "invalid second sub cluster size: " + secondSubClusterSize);

        Config config = createConfig();
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        JetInstance[] instances = startInitialCluster(config, clusterSize);

        if (beforeSplit != null) {
            beforeSplit.accept(instances);
        }

        LOGGER.info("Going to create split-brain...");
        createSplitBrain(instances, firstSubClusterSize, secondSubClusterSize);
        Brains brains = getBrains(instances, firstSubClusterSize, secondSubClusterSize);
        LOGGER.info("Split-brain created");

        if (onSplit != null) {
            onSplit.accept(brains.firstSubCluster, brains.secondSubCluster);
        }

        LOGGER.info("Going to heal split-brain...");
        healSplitBrain(instances, firstSubClusterSize);
        LOGGER.info("Split-brain healed");

        if (afterMerge != null) {
            afterMerge.accept(instances);
        }
    }

    private JetInstance[] startInitialCluster(Config config, int clusterSize) {
        JetInstance[] instances = new JetInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = createJetMember(config);
        }
        return instances;
    }

    /**
     * Starts a new {@code JetInstance} which is only able to communicate
     * with members on one of the two brains.
     * @param firstSubCluster jet instances in the first sub cluster
     * @param secondSubCluster jet instances in the first sub cluster
     * @param createOnFirstSubCluster if true, new instance is created on the first sub cluster.
     * @return a HazelcastInstance whose {@code MockJoiner} has blacklisted the other brain's
     *         members and its connection manager blocks connections to other brain's members
     * @see TestHazelcastInstanceFactory#newHazelcastInstance(Address, com.hazelcast.config.Config, Address[])
     */
    protected final JetInstance createHazelcastInstanceInBrain(JetInstance[] firstSubCluster,
                                                               JetInstance[] secondSubCluster,
                                                               boolean createOnFirstSubCluster) {
        Address newMemberAddress = nextAddress();
        JetInstance[] instancesToBlock = createOnFirstSubCluster ? secondSubCluster : firstSubCluster;

        List<Address> addressesToBlock = new ArrayList<>(instancesToBlock.length);
        for (JetInstance anInstancesToBlock : instancesToBlock) {
            if (isInstanceActive(anInstancesToBlock)) {
                addressesToBlock.add(getAddress(anInstancesToBlock));
                // block communication from these instances to the new address
                FirewallingServerConnectionManager connectionManager = getFireWalledEndpointManager(anInstancesToBlock);
                connectionManager.blockNewConnection(newMemberAddress);
                connectionManager.closeActiveConnection(newMemberAddress);
            }
        }
        // indicate we need to unblacklist addresses from joiner when split-brain will be healed
        unblacklistHint = true;
        // create a new Hazelcast instance which has blocked addresses blacklisted in its joiner
        return createJetMember(createConfig(), addressesToBlock.toArray(new Address[addressesToBlock.size()]));
    }

    private void createSplitBrain(JetInstance[] instances, int firstSubClusterSize, int secondSubClusterSize) {
        applyOnBrains(instances, firstSubClusterSize, SplitBrainTestSupport::blockCommunicationBetween);
        applyOnBrains(instances, firstSubClusterSize, HazelcastTestSupport::closeConnectionBetween);
        assertSplitBrainCreated(instances, firstSubClusterSize, secondSubClusterSize);
    }

    private void assertSplitBrainCreated(JetInstance[] instances, int firstSubClusterSize, int secondSubClusterSize) {
        for (int isolatedIndex = 0; isolatedIndex < firstSubClusterSize; isolatedIndex++) {
            JetInstance isolatedInstance = instances[isolatedIndex];
            assertClusterSizeEventually(firstSubClusterSize, isolatedInstance.getHazelcastInstance());
        }
        for (int i = firstSubClusterSize; i < instances.length; i++) {
            JetInstance currentInstance = instances[i];
            assertClusterSizeEventually(secondSubClusterSize, currentInstance.getHazelcastInstance());
        }
    }

    private void healSplitBrain(JetInstance[] instances, int firstSubClusterSize) {
        applyOnBrains(instances, firstSubClusterSize, SplitBrainTestSupport::unblockCommunicationBetween);
        if (unblacklistHint) {
            applyOnBrains(instances, firstSubClusterSize, JetSplitBrainTestSupport::unblacklistJoinerBetween);
        }
        for (JetInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance.getHazelcastInstance());
        }
        waitAllForSafeState(Stream.of(instances).map(JetInstance::getHazelcastInstance).collect(toList()));
    }

    private static FirewallingServerConnectionManager getFireWalledEndpointManager(JetInstance hz) {
        ServerConnectionManager cm = getNode(hz).getServer().getConnectionManager(EndpointQualifier.MEMBER);
        return (FirewallingServerConnectionManager) cm;
    }

    private Brains getBrains(JetInstance[] instances, int firstSubClusterSize, int secondSubClusterSize) {
        JetInstance[] firstSubCluster = new JetInstance[firstSubClusterSize];
        JetInstance[] secondSubCluster = new JetInstance[secondSubClusterSize];
        for (int i = 0; i < instances.length; i++) {
            if (i < firstSubClusterSize) {
                firstSubCluster[i] = instances[i];
            } else {
                secondSubCluster[i - firstSubClusterSize] = instances[i];
            }
        }
        return new Brains(firstSubCluster, secondSubCluster);
    }

    private void applyOnBrains(JetInstance[] instances, int firstSubClusterSize,
                               BiConsumer<HazelcastInstance, HazelcastInstance> action) {
        for (int i = 0; i < firstSubClusterSize; i++) {
            JetInstance isolatedInstance = instances[i];
            // do not take into account instances which have been shutdown
            if (!isInstanceActive(isolatedInstance)) {
                continue;
            }
            for (int j = firstSubClusterSize; j < instances.length; j++) {
                JetInstance currentInstance = instances[j];
                if (!isInstanceActive(currentInstance)) {
                    continue;
                }
                action.accept(isolatedInstance.getHazelcastInstance(), currentInstance.getHazelcastInstance());
            }
        }
    }

    private static boolean isInstanceActive(JetInstance instance) {
        if (instance.getHazelcastInstance() instanceof HazelcastInstanceProxy) {
            try {
                ((HazelcastInstanceProxy) instance.getHazelcastInstance()).getOriginal();
                return true;
            } catch (HazelcastInstanceNotActiveException exception) {
                return false;
            }
        } else if (instance.getHazelcastInstance() instanceof HazelcastInstanceImpl) {
            return getNode(instance).getState() == NodeState.ACTIVE;
        } else {
            throw new AssertionError("Unsupported HazelcastInstance type");
        }
    }

    private static void unblacklistJoinerBetween(HazelcastInstance h1, HazelcastInstance h2) {
        Node h1Node = Accessors.getNode(h1);
        Node h2Node = Accessors.getNode(h2);
        h1Node.getJoiner().unblacklist(h2Node.getThisAddress());
        h2Node.getJoiner().unblacklist(h1Node.getThisAddress());
    }

    private static final class Brains {
        private final JetInstance[] firstSubCluster;
        private final JetInstance[] secondSubCluster;

        private Brains(JetInstance[] firstSubCluster, JetInstance[] secondSubCluster) {
            this.firstSubCluster = firstSubCluster;
            this.secondSubCluster = secondSubCluster;
        }
    }
}

