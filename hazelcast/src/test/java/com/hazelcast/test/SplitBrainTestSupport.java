package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

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
 *
 */
public abstract class SplitBrainTestSupport extends HazelcastTestSupport {

    private static final int[] DEFAULT_BRAINS = new int[]{1, 2};
    private static final int DEFAULT_ITERATION_COUNT = 1;
    private HazelcastInstance[] instances;
    private int[] brains;

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



    @Before
    public final void setUpInternals() {
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
     * Called when a cluster is fully formed. You can use this method for test initialization, data load, etc.
     *
     * @param instances all Hazelcast instances in your cluster
     */
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {

    }

    /**
     * Called just after a split brain situation was created
     *
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

    private HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[clusterSize];
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            HazelcastInstance hz = factory.newHazelcastInstance(config);
            hazelcastInstances[i] = hz;
        }
        return hazelcastInstances;
    }

    private void validateBrainsConfig(int[] clusterTopology) {
        if (clusterTopology.length != 2) {
            throw new AssertionError("Only a simple topologies with 2 brains are supported. Current setup: "
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
        unblockCommunication();
        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(instances.length, hz);
        }
        waitAllForSafeState(instances);
    }

    private void unblockCommunication() {
        applyOnBrains(UNBLOCK_COMMUNICATION);
    }

    private static FirewallingMockConnectionManager getFireWalledConnectionManager(HazelcastInstance hz) {
        return (FirewallingMockConnectionManager) getNode(hz).getConnectionManager();
    }

    private Brains getBrains() {
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
            for (int i = firstHalfSize; i < instances.length; i++) {
                HazelcastInstance currentInstance = instances[i];
                action.apply(isolatedInstance, currentInstance);
            }
        }
    }

    private static void blockCommunicationBetween(HazelcastInstance h1, HazelcastInstance h2) {
        FirewallingMockConnectionManager h1CM = getFireWalledConnectionManager(h1);
        FirewallingMockConnectionManager h2CM = getFireWalledConnectionManager(h2);
        Node h1Node = getNode(h1);
        Node h2Node = getNode(h2);
        h1CM.block(h2Node.getThisAddress());
        h2CM.block(h1Node.getThisAddress());
    }

    private static void unblockCommunicationBetween(HazelcastInstance h1, HazelcastInstance h2) {
        FirewallingMockConnectionManager h1CM = getFireWalledConnectionManager(h1);
        FirewallingMockConnectionManager h2CM = getFireWalledConnectionManager(h2);
        Node h1Node = getNode(h1);
        Node h2Node = getNode(h2);
        h1CM.unblock(h2Node.getThisAddress());
        h2CM.unblock(h1Node.getThisAddress());
    }

    private interface SplitBrainAction {
        void apply(HazelcastInstance h1, HazelcastInstance h2);
    }

    private class Brains {
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
