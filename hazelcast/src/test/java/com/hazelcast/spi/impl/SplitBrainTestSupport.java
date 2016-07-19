package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * A support class for high-level split-brain tests.
 * It will form a cluster, create a split-brain situation and then heal the cluster again.
 *
 * Tests are supposed to subclass this class and use its hooks to be notified about state transitions.
 * See {@link #onBeforeSplitBrainCreated()}, {@link #onAfterSplitBrainCreated()} and {@link #onAfterSplitBrainHealed()}
 *
 * The current implementation always isolate the 1st member of the cluster, but it should be simple to customize this
 * class to support mode advanced split-brain scenarios.
 *
 */
public abstract class SplitBrainTestSupport extends HazelcastTestSupport {

    private static final int DEFAULT_CLUSTER_SIZE = 3;
    private HazelcastInstance[] instances;

    @Before
    public final void setUp() {
        final Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");

        int clusterSize = clusterSize();
        instances = new HazelcastInstance[clusterSize];
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            HazelcastInstance hz = factory.newHazelcastInstance(config);
            instances[i] = hz;
        }
    }

    protected int clusterSize() {
        return DEFAULT_CLUSTER_SIZE;
    }

    /**
     * Called when a cluster is fully formed. You can use this method for test initialization, data load, etc.
     *
     */
    protected void onBeforeSplitBrainCreated() {

    }

    /**
     * Called just after a split brain situation was created
     *
     */
    protected void onAfterSplitBrainCreated() {

    }

    /**
     * Called just after the original cluster was healed again. This is likely the place for various asserts.
     *
     */
    protected void onAfterSplitBrainHealed() {

    }

    protected HazelcastInstance[] getAllInstances() {
        return instances;
    }

    @Test
    public void testSplitBrain() {
        onBeforeSplitBrainCreated();
        createSplitBrain();
        onAfterSplitBrainCreated();
        healSplitBrain();
        onAfterSplitBrainHealed();
    }

    private void createSplitBrain() {
        HazelcastInstance isolatedInstance = instances[0];
        FirewallingMockConnectionManager isolatedCM = getFireWalledConnectionManager(isolatedInstance);
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            Node currentNode = getNode(currentInstance);
            isolatedCM.block(currentNode.getThisAddress());
        }

        Node isolatedNode = getNode(isolatedInstance);
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            FirewallingMockConnectionManager currentCM = getFireWalledConnectionManager(currentInstance);
            currentCM.block(isolatedNode.getThisAddress());
        }

        for (int i = 1 ; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            closeConnectionBetween(isolatedInstance, currentInstance);
        }

        assertClusterSizeEventually(1, isolatedInstance);
        for (int i = 1 ; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            assertClusterSizeEventually(instances.length - 1, currentInstance);
        }

    }

    private void healSplitBrain() {
        HazelcastInstance isolatedInstance = instances[0];
        FirewallingMockConnectionManager isolatedCM = getFireWalledConnectionManager(isolatedInstance);
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            Node currentNode = getNode(currentInstance);
            isolatedCM.unblock(currentNode.getThisAddress());
        }

        Node isolatedNode = getNode(isolatedInstance);
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            FirewallingMockConnectionManager currentCM = getFireWalledConnectionManager(currentInstance);
            currentCM.unblock(isolatedNode.getThisAddress());
        }

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(instances.length, hz);
        }
        waitAllForSafeState(instances);
    }

    private static FirewallingMockConnectionManager getFireWalledConnectionManager(HazelcastInstance hz) {
        return (FirewallingMockConnectionManager) getNode(hz).getConnectionManager();
    }

}
