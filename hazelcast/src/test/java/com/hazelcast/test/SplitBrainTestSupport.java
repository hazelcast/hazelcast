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

package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.server.FirewallingServer;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.properties.ClusterProperty;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static com.hazelcast.test.starter.ReflectionUtils.isInstanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A support class for high-level split-brain tests.
 * <p>
 * Forms a cluster, creates a split-brain situation and then heals the cluster again.
 * <p>
 * Implementing tests are supposed to subclass this class and use its hooks to be notified about state transitions.
 * See {@link #onBeforeSplitBrainCreated(HazelcastInstance[])},
 * {@link #onAfterSplitBrainCreated(HazelcastInstance[], HazelcastInstance[])}
 * and {@link #onAfterSplitBrainHealed(HazelcastInstance[])}
 * <p>
 * The current implementation always isolates the first member of the cluster, but it should be simple to customize this
 * class to support mode advanced split-brain scenarios.
 * <p>
 * See {@link SplitBrainTestSupportTest} for an example test.
 */
@SuppressWarnings({"RedundantThrows", "WeakerAccess"})
public abstract class SplitBrainTestSupport extends HazelcastTestSupport {

    protected TestHazelcastInstanceFactory factory;

    // per default the second half should merge into the first half
    private static final int[] DEFAULT_BRAINS = new int[]{2, 1};
    private static final int DEFAULT_ITERATION_COUNT = 1;
    private HazelcastInstance[] instances;
    private int[] brains;
    /**
     * If new nodes have been created during split brain via {@link #createHazelcastInstanceInBrain(int)}, then their joiners
     * are initialized with the other brain's addresses being blacklisted.
     */
    private boolean unblacklistHint = false;

    private static final SplitBrainAction BLOCK_COMMUNICATION = SplitBrainTestSupport::blockCommunicationBetween;

    private static final SplitBrainAction UNBLOCK_COMMUNICATION = SplitBrainTestSupport::unblockCommunicationBetween;

    private static final SplitBrainAction CLOSE_CONNECTION = HazelcastTestSupport::closeConnectionBetween;

    private static final SplitBrainAction UNBLACKLIST_MEMBERS = SplitBrainTestSupport::unblacklistJoinerBetween;

    @Before
    public final void setUpInternals() {
        onBeforeSetup();

        brains = brains();
        validateBrainsConfig(brains);

        Config config = config();
        int clusterSize = getClusterSize();
        instances = startInitialCluster(config, clusterSize);
    }

    @After
    public final void tearDown() {
        onTearDown();
    }

    /**
     * Override this method to execute initialization that may be required before instantiating the cluster. This is the
     * first method executed by {@code @Before SplitBrainTestSupport.setupInternals}.
     */
    protected void onBeforeSetup() {
    }

    /**
     * Override this method to execute clean up that may be required after finishing the test. This is the
     * first method executed by {@code @After SplitBrainTestSupport.tearDown}.
     */
    protected void onTearDown() {
    }

    /**
     * Override this method to create a custom brain sizes
     *
     * @return the default number of brains
     */
    protected int[] brains() {
        return DEFAULT_BRAINS;
    }

    /**
     * Override this method to create a custom Hazelcast configuration.
     *
     * @return the default Hazelcast configuration
     */
    protected Config config() {
        return smallInstanceConfig()
                .setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
                .setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
    }

    protected final Config getConfig() {
        return super.getConfig();
    }

    /**
     * Override this method to create the split-brain situation multiple-times.
     * <p>
     * The test will use the same members for all iterations.
     *
     * @return the default number of iterations
     */
    protected int iterations() {
        return DEFAULT_ITERATION_COUNT;
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
     * <p>
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

        List<Address> addressesToBlock = new ArrayList<>(instancesToBlock.length);
        for (HazelcastInstance hz : instancesToBlock) {
            if (isInstanceActive(hz)) {
                addressesToBlock.add(Accessors.getAddress(hz));
                // block communication from these instances to the new address
                FirewallingServer networkingService = getFireWalledNetworkingService(hz);
                FirewallingServer.FirewallingServerConnectionManager endpointManager =
                        (FirewallingServer.FirewallingServerConnectionManager) networkingService.getConnectionManager(MEMBER);
                endpointManager.blockNewConnection(newMemberAddress);
                endpointManager.closeActiveConnection(newMemberAddress);
            }
        }
        // indicate that we need to unblacklist addresses from the joiner when split-brain will be healed
        unblacklistHint = true;
        // create a new Hazelcast instance which has blocked addresses blacklisted in its joiner
        return factory.newHazelcastInstance(newMemberAddress, config(), addressesToBlock.toArray(new Address[0]));
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

    private static FirewallingServer getFireWalledNetworkingService(HazelcastInstance hz) {
        Node node = Accessors.getNode(hz);
        return (FirewallingServer) node.getServer();
    }

    private static FirewallingServer.FirewallingServerConnectionManager getFireWalledEndpointManager(HazelcastInstance hz) {
        Node node = Accessors.getNode(hz);
        return (FirewallingServer.FirewallingServerConnectionManager)
                node.getServer().getConnectionManager(MEMBER);
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
        if (isInstanceOf(instance, HazelcastInstanceProxy.class)) {
            try {
                return getFieldValueReflectively(instance, "original") != null;
            } catch (IllegalAccessException e) {
                throw new AssertionError("Could not get original field from HazelcastInstanceProxy: " + e.getMessage());
            }
        } else if (isInstanceOf(instance, HazelcastInstanceImpl.class)) {
            return Accessors.getNode(instance).getState() == NodeState.ACTIVE;
        }
        throw new AssertionError("Unsupported HazelcastInstance type: " + instance.getClass().getName());
    }

    public static void blockCommunicationBetween(HazelcastInstance h1, HazelcastInstance h2) {
        FirewallingServer.FirewallingServerConnectionManager cm1 = getFireWalledEndpointManager(h1);
        FirewallingServer.FirewallingServerConnectionManager cm2 = getFireWalledEndpointManager(h2);
        Node node1 = Accessors.getNode(h1);
        Node node2 = Accessors.getNode(h2);
        cm1.blockNewConnection(node2.getThisAddress());
        cm2.blockNewConnection(node1.getThisAddress());
        cm1.closeActiveConnection(node2.getThisAddress());
        cm2.closeActiveConnection(node1.getThisAddress());
    }

    public static void unblockCommunicationBetween(HazelcastInstance h1, HazelcastInstance h2) {
        FirewallingServer.FirewallingServerConnectionManager cm1 = getFireWalledEndpointManager(h1);
        FirewallingServer.FirewallingServerConnectionManager cm2 = getFireWalledEndpointManager(h2);
        Node node1 = Accessors.getNode(h1);
        Node node2 = Accessors.getNode(h2);
        cm1.unblock(node2.getThisAddress());
        cm2.unblock(node1.getThisAddress());
    }

    private static void unblacklistJoinerBetween(HazelcastInstance h1, HazelcastInstance h2) {
        Node h1Node = Accessors.getNode(h1);
        Node h2Node = Accessors.getNode(h2);
        h1Node.getJoiner().unblacklist(h2Node.getThisAddress());
        h2Node.getJoiner().unblacklist(h1Node.getThisAddress());
    }

    public static String toString(Collection collection) {
        StringBuilder sb = new StringBuilder("[");
        String delimiter = "";
        for (Object item : collection) {
            sb.append(delimiter).append(item);
            delimiter = ", ";
        }
        sb.append("]");
        return sb.toString();
    }

    public static void assertPi(Object value) {
        assertInstanceOf(Double.class, value);
        assertEquals("Expected the value to be PI", Math.PI, (Double) value, 0.00000001d);
    }

    public static void assertPiCollection(Collection<Object> collection) {
        assertEquals("Expected the collection to be a PI collection",
                collection.size(), ReturnPiCollectionMergePolicy.PI_COLLECTION.size());
        assertTrue("Expected the collection to be a PI collection",
                collection.containsAll(ReturnPiCollectionMergePolicy.PI_COLLECTION));
    }

    public static void assertPiSet(Collection<Object> collection) {
        assertEquals("Expected the collection to be a PI set", collection.size(), ReturnPiCollectionMergePolicy.PI_SET.size());
        assertTrue("Expected the collection to be a PI set", collection.containsAll(ReturnPiCollectionMergePolicy.PI_SET));
    }

    private interface SplitBrainAction {
        void apply(HazelcastInstance h1, HazelcastInstance h2);
    }

    /**
     * Contains the {@link HazelcastInstance} from the both sub-clusters (first and second brain).
     */
    protected static class Brains {

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

    /**
     * Listener to wait for the split-brain healing to be finished.
     */
    protected static class MergeLifecycleListener implements LifecycleListener {

        private final CountDownLatch latch;

        public MergeLifecycleListener(int mergingClusterSize) {
            latch = new CountDownLatch(mergingClusterSize);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                latch.countDown();
            }
        }

        public void await() {
            assertOpenEventually(latch);
        }
    }

    /**
     * Always returns {@code null} as merged value.
     * <p>
     * Used to test the removal of all values from a data structure.
     */
    protected static class RemoveValuesMergePolicy implements SplitBrainMergePolicy<Object, MergingValue<Object>, Object> {

        @Override
        public Object merge(MergingValue<Object> mergingValue, MergingValue<Object> existingValue) {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    /**
     * Always returns {@link Math#PI} as merged value.
     * <p>
     * Used to test that data structures can deal with user created data in OBJECT format.
     */
    protected static class ReturnPiMergePolicy implements SplitBrainMergePolicy<Object, MergingValue<Object>, Object> {

        @Override
        public Object merge(MergingValue<Object> mergingValue, MergingValue<Object> existingValue) {
            return Math.PI;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    /**
     * Always returns a collection of {@link Math#PI} digits as merged value.
     * <p>
     * Used to test that data structures can deal with user created data in OBJECT format.
     */
    protected static class ReturnPiCollectionMergePolicy
            implements SplitBrainMergePolicy<Collection<Object>, MergingValue<Collection<Object>>, Collection<Object>> {

        private static final Collection<Object> PI_COLLECTION;
        private static final Set<Object> PI_SET;

        static {
            PI_COLLECTION = new ArrayList<>(5);
            PI_COLLECTION.add(3);
            PI_COLLECTION.add(1);
            PI_COLLECTION.add(4);
            PI_COLLECTION.add(1);
            PI_COLLECTION.add(5);
            PI_SET = new HashSet<>(PI_COLLECTION);
        }

        @Override
        public Collection<Object> merge(MergingValue<Collection<Object>> mergingValue,
                                        MergingValue<Collection<Object>> existingValue) {
            return PI_COLLECTION;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    /**
     * Merges only {@link Integer} values of the given values (preferring the merging value).
     * <p>
     * Used to test the deserialization of values.
     */
    protected static class MergeIntegerValuesMergePolicy<V, T extends MergingValue<V>> implements SplitBrainMergePolicy<V, T, Object> {

        @Override
        public Object merge(T mergingValue, T existingValue) {
            if (mergingValue.getValue() instanceof Integer) {
                return mergingValue.getRawValue();
            }
            if (existingValue != null && existingValue.getValue() instanceof Integer) {
                return existingValue.getRawValue();
            }
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    /**
     * Merges only {@link Integer} values of the given collections.
     * <p>
     * Used to test the deserialization of values.
     */
    protected static class MergeCollectionOfIntegerValuesMergePolicy
            implements SplitBrainMergePolicy<Collection<Object>, MergingValue<Collection<Object>>, Collection<Object>> {

        @Override
        public Collection<Object> merge(MergingValue<Collection<Object>> mergingValue,
                                        MergingValue<Collection<Object>> existingValue) {
            Collection<Object> result = new ArrayList<>();
            for (Object value : mergingValue.getValue()) {
                if (value instanceof Integer) {
                    result.add(value);
                }
            }
            if (existingValue != null) {
                for (Object value : existingValue.getValue()) {
                    if (value instanceof Integer) {
                        result.add(value);
                    }
                }
            }
            return result;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }
}
