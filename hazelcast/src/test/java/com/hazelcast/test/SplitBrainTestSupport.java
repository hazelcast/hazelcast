/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A support class for high-level split-brain tests.
 * It will form a cluster, create a split-brain situation and then heal the cluster again.
 *
 * Tests are supposed to subclass this class and use its hooks to be notified about state transitions.
 * See {@link #onBeforeSplitBrainCreated(HazelcastInstance[])},
 * {@link #onAfterSplitBrainCreated(HazelcastInstance[], HazelcastInstance[])}
 * and {@link #onAfterSplitBrainHealed(HazelcastInstance[])}
 *
 * The current implementation always isolate the 1st member of the cluster, but it should be simple to customize this
 * class to support mode advanced split-brain scenarios.
 *
 * See {@link SplitBrainTestSupportTest} for an example test.
 */
public abstract class SplitBrainTestSupport extends HazelcastTestSupport {

    protected TestHazelcastInstanceFactory factory;

    private static final int[] DEFAULT_BRAINS = new int[]{1, 2};
    private static final int DEFAULT_ITERATION_COUNT = 1;
    private HazelcastInstance[] instances;
    private int[] brains;
    /**
     * If new nodes have been created during split brain via {@link #createHazelcastInstanceInBrain(int)}, then their joiners
     * are initialized with the other brain's addresses being blacklisted.
     */
    private boolean unblacklistHint = false;

    private static final SplitBrainAction BLOCK_COMMUNICATION = new SplitBrainAction() {
        @Override
        public void apply(HazelcastInstance h1, HazelcastInstance h2) {
            blockCommunicationBetween(h1, h2);
        }
    };

    private static final SplitBrainAction UNBLOCK_COMMUNICATION = new SplitBrainAction() {
        @Override
        public void apply(HazelcastInstance h1, HazelcastInstance h2) {
            unblockCommunicationBetween(h1, h2);
        }
    };

    private static final SplitBrainAction CLOSE_CONNECTION = new SplitBrainAction() {
        @Override
        public void apply(HazelcastInstance h1, HazelcastInstance h2) {
            closeConnectionBetween(h1, h2);
        }
    };

    private static final SplitBrainAction UNBLACKLIST_MEMBERS = new SplitBrainAction() {
        @Override
        public void apply(HazelcastInstance h1, HazelcastInstance h2) {
            unblacklistJoinerBetween(h1, h2);
        }
    };

    @Before
    public final void setUpInternals() {
        onBeforeSetup();
        final Config config = config();

        brains = brains();
        validateBrainsConfig(brains);
        int clusterSize = getClusterSize();
        instances = startInitialCluster(config, clusterSize);
    }

    /**
     * Override this for custom Hazelcast configuration
     *
     * @return
     */
    protected Config config() {
        final Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
        return config;
    }

    /**
     * Override this to create a custom brain sizes
     *
     * @return
     */
    protected int[] brains() {
        return DEFAULT_BRAINS;
    }


    /**
     * Override this to create the split-brain situation multiple-times. The test will use
     * the same members for all iterations.
     *
     * @return
     */
    protected int iterations() {
        return DEFAULT_ITERATION_COUNT;
    }

    /**
     * Override this method to execute initialization that may be required before instantiating the cluster. This is the
     * first method executed by {@code @Before SplitBrainTestSupport.setupInternals}.
     */
    protected void onBeforeSetup() {

    }

    /**
     * Called when a cluster is fully formed. You can use this method for test initialization, data load, etc.
     *
     * @param instances all Hazelcast instances in your cluster
     */
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {

    }

    /**
     * Called just after a split brain situation was created
     */
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) throws Exception {

    }

    /**
     * Called just after the original cluster was healed again. This is likely the place for various asserts.
     *
     * @param instances all Hazelcast instances in your cluster
     */
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {

    }

    /**
     * Indicates whether test should fail when cluster does not include all original members after communications are unblocked.
     *
     * Override this method when it is expected that after communications are unblocked some members will not rejoin the cluster.
     * When overriding this method, it may be desirable to add some wait time to allow the split brain handler to execute.
     *
     * @return {@code true} if the test should fail when not all original members rejoin after split brain is
     * healed, otherwise {@code false}.
     */
    protected boolean shouldAssertAllNodesRejoined() {
        return true;
    }

    @Test
    public void testSplitBrain() throws Exception {
        for (int i = 0; i < iterations(); i++) {
            doIteration();
        }
    }

    private void doIteration() throws Exception {
        onBeforeSplitBrainCreated(instances);

        createSplitBrain();
        Brains brains = getBrains();
        onAfterSplitBrainCreated(brains.getFirstHalf(), brains.getSecondHalf());

        healSplitBrain();
        onAfterSplitBrainHealed(instances);
    }

    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[clusterSize];
        factory = createHazelcastInstanceFactory(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            HazelcastInstance hz = factory.newHazelcastInstance(config);
            hazelcastInstances[i] = hz;
        }
        return hazelcastInstances;
    }

    /**
     * Starts a new {@code HazelcastInstance} which is only able to communicate with members on one of the two brains.
     *
     * @param brain index of brain to start a new instance in (0 to start instance in first brain or 1 to start instance in
     *              second brain)
     * @return a HazelcastInstance whose {@code MockJoiner} has blacklisted the other brain's members and its connection
     * manager blocks connections to other brain's members
     * @see TestHazelcastInstanceFactory#newHazelcastInstance(Address, Config, Address[])
     */
    protected HazelcastInstance createHazelcastInstanceInBrain(int brain) {
        Address newMemberAddress = factory.nextAddress();
        Brains brains = getBrains();
        HazelcastInstance[] instancesToBlock = brain == 1 ? brains.firstHalf : brains.secondHalf;

        List<Address> addressesToBlock = new ArrayList<Address>(instancesToBlock.length);
        for (int i = 0; i < instancesToBlock.length; i++) {
            if (isInstanceActive(instancesToBlock[i])) {
                addressesToBlock.add(getAddress(instancesToBlock[i]));
                // block communication from these instances to the new address

                FirewallingConnectionManager connectionManager = getFireWalledConnectionManager(instancesToBlock[i]);
                connectionManager.blockNewConnection(newMemberAddress);
                connectionManager.closeActiveConnection(newMemberAddress);
            }
        }
        // indicate we need to unblacklist addresses from joiner when split-brain will be healed
        unblacklistHint = true;
        // create a new Hazelcast instance which has blocked addresses blacklisted in its joiner
        return factory.newHazelcastInstance(config(), addressesToBlock.toArray(new Address[addressesToBlock.size()]));
    }

    private void validateBrainsConfig(int[] clusterTopology) {
        if (clusterTopology.length != 2) {
            throw new AssertionError("Only simple topologies with 2 brains are supported. Current setup: "
                    + Arrays.toString(clusterTopology));
        }
    }

    private int getClusterSize() {
        int clusterSize = 0;
        for (int brainSize : brains) {
            clusterSize += brainSize;
        }
        return clusterSize;
    }

    private void createSplitBrain() {
        blockCommunications();
        closeExistingConnections();
        assertSplitBrainCreated();
    }

    private void assertSplitBrainCreated() {
        int firstHalfSize = brains[0];
        for (int isolatedIndex = 0; isolatedIndex < firstHalfSize; isolatedIndex++) {
            HazelcastInstance isolatedInstance = instances[isolatedIndex];
            assertClusterSizeEventually(firstHalfSize, isolatedInstance);
        }
        for (int i = firstHalfSize; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            assertClusterSizeEventually(instances.length - firstHalfSize, currentInstance);
        }
    }

    private void closeExistingConnections() {
        applyOnBrains(CLOSE_CONNECTION);
    }

    private void blockCommunications() {
        applyOnBrains(BLOCK_COMMUNICATION);
    }

    private void healSplitBrain() {
        MergeBarrier mergeBarrier = new MergeBarrier(instances);
        try {
            unblockCommunication();
            if (unblacklistHint) {
                unblacklistMembers();
            }
            if (shouldAssertAllNodesRejoined()) {
                for (HazelcastInstance hz : instances) {
                    assertClusterSizeEventually(instances.length, hz);
                }
            }
            waitAllForSafeState(instances);
        } finally {
            mergeBarrier.awaitNoMergeInProgressAndClose();
        }
    }

    private void unblockCommunication() {
        applyOnBrains(UNBLOCK_COMMUNICATION);
    }

    private void unblacklistMembers() {
        applyOnBrains(UNBLACKLIST_MEMBERS);
    }

    private static FirewallingConnectionManager getFireWalledConnectionManager(HazelcastInstance hz) {
        return (FirewallingConnectionManager) getNode(hz).getConnectionManager();
    }

    protected Brains getBrains() {
        int firstHalfSize = brains[0];
        int secondHalfSize = brains[1];
        HazelcastInstance[] firstHalf = new HazelcastInstance[firstHalfSize];
        HazelcastInstance[] secondHalf = new HazelcastInstance[secondHalfSize];
        for (int i = 0; i < instances.length; i++) {
            if (i < firstHalfSize) {
                firstHalf[i] = instances[i];
            } else {
                secondHalf[i - firstHalfSize] = instances[i];
            }
        }
        return new Brains(firstHalf, secondHalf);
    }

    private void applyOnBrains(SplitBrainAction action) {
        int firstHalfSize = brains[0];
        for (int isolatedIndex = 0; isolatedIndex < firstHalfSize; isolatedIndex++) {
            HazelcastInstance isolatedInstance = instances[isolatedIndex];
            // do not take into account instances which have been shutdown
            if (!isInstanceActive(isolatedInstance)) {
                continue;
            }
            for (int i = firstHalfSize; i < instances.length; i++) {
                HazelcastInstance currentInstance = instances[i];
                if (!isInstanceActive(currentInstance)) {
                    continue;
                }
                action.apply(isolatedInstance, currentInstance);
            }
        }
    }

    private static boolean isInstanceActive(HazelcastInstance instance) {
        if (instance instanceof HazelcastInstanceProxy) {
            try {
                ((HazelcastInstanceProxy) instance).getOriginal();
                return true;
            } catch (HazelcastInstanceNotActiveException exception) {
                return false;
            }
        } else if (instance instanceof HazelcastInstanceImpl) {
            return getNode(instance).getState() == NodeState.ACTIVE;
        } else {
            throw new AssertionError("Unsupported HazelcastInstance type");
        }
    }

    public static void blockCommunicationBetween(HazelcastInstance h1, HazelcastInstance h2) {
        FirewallingConnectionManager cm1 = getFireWalledConnectionManager(h1);
        FirewallingConnectionManager cm2 = getFireWalledConnectionManager(h2);
        Node node1 = getNode(h1);
        Node node2 = getNode(h2);
        cm1.blockNewConnection(node2.getThisAddress());
        cm2.blockNewConnection(node1.getThisAddress());
        cm1.closeActiveConnection(node2.getThisAddress());
        cm2.closeActiveConnection(node1.getThisAddress());
    }

    public static void unblockCommunicationBetween(HazelcastInstance h1, HazelcastInstance h2) {
        FirewallingConnectionManager cm1 = getFireWalledConnectionManager(h1);
        FirewallingConnectionManager cm2 = getFireWalledConnectionManager(h2);
        Node node1 = getNode(h1);
        Node node2 = getNode(h2);
        cm1.unblock(node2.getThisAddress());
        cm2.unblock(node1.getThisAddress());
    }

    private static void unblacklistJoinerBetween(HazelcastInstance h1, HazelcastInstance h2) {
        Node h1Node = getNode(h1);
        Node h2Node = getNode(h2);
        h1Node.getJoiner().unblacklist(h2Node.getThisAddress());
        h2Node.getJoiner().unblacklist(h1Node.getThisAddress());
    }

    private interface SplitBrainAction {
        void apply(HazelcastInstance h1, HazelcastInstance h2);
    }

    protected class Brains {
        private final HazelcastInstance[] firstHalf;
        private final HazelcastInstance[] secondHalf;

        private Brains(HazelcastInstance[] firstHalf, HazelcastInstance[] secondHalf) {
            this.firstHalf = firstHalf;
            this.secondHalf = secondHalf;
        }

        public HazelcastInstance[] getFirstHalf() {
            return firstHalf;
        }

        public HazelcastInstance[] getSecondHalf() {
            return secondHalf;
        }
    }
}
