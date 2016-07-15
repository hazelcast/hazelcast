package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.DefaultNodeContext;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.tcp.FirewallingTcpIpConnectionManager;
import com.hazelcast.spi.impl.waitnotifyservice.impl.TestRecoveryAfterSplitBrain;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.ServerSocketChannel;

import static com.hazelcast.instance.TestUtil.getNode;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.closeConnectionBetween;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;

/**
 * A support class for high-level split-brain tests.
 * It will form a cluster, create a split-brain situation and then heal the cluster again.
 *
 * Tests are supposed to subclass this class and use its hooks to be notified about state transitions.
 * See {@link #onBeforeSplitBrainCreated()}, {@link #onAfterSplitBrainCreated()} and {@link #onAfterSplitBrainHealed()}
 *
 * The current implementation always isolate the 1st member of the cluster, but it should be simple to customer this
 * class to support additional scenarios too.
 *
 */
public abstract class SplitBrainTestSupport {

    private static final int DEFAULT_CLUSTER_SIZE = 3;
    private HazelcastInstance[] instances;

    @Before
    public final void setUp() {
        final Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "30");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "30");

        int clusterSize = clusterSize();
        instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            HazelcastInstance hz = HazelcastInstanceFactory.newHazelcastInstance(config, "test-" + i,
                    new TestRecoveryAfterSplitBrain.FirewallingNodeContext());
            instances[i] = hz;
        }
    }

    @After
    public final void tearDown() {
        for (HazelcastInstance hz : instances) {
            hz.getLifecycleService().terminate();
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
        FirewallingTcpIpConnectionManager isolatedCM = getFireWalledConnectionManager(isolatedInstance);
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            Node currentNode = getNode(currentInstance);
            isolatedCM.block(currentNode.getThisAddress());
        }

        Node isolatedNode = getNode(isolatedInstance);
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            FirewallingTcpIpConnectionManager currentCM = getFireWalledConnectionManager(currentInstance);
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
        FirewallingTcpIpConnectionManager isolatedCM = getFireWalledConnectionManager(isolatedInstance);
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            Node currentNode = getNode(currentInstance);
            isolatedCM.unblock(currentNode.getThisAddress());
        }

        Node isolatedNode = getNode(isolatedInstance);
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance currentInstance = instances[i];
            FirewallingTcpIpConnectionManager currentCM = getFireWalledConnectionManager(currentInstance);
            currentCM.unblock(isolatedNode.getThisAddress());
        }

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(instances.length, hz);
        }
        waitAllForSafeState(instances);
    }

    private static FirewallingTcpIpConnectionManager getFireWalledConnectionManager(HazelcastInstance hz) {
        return (FirewallingTcpIpConnectionManager) getNode(hz).getConnectionManager();
    }

    public static class FirewallingNodeContext extends DefaultNodeContext {
        @Override
        public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
            NodeIOService ioService = new NodeIOService(node, node.nodeEngine);
            return new FirewallingTcpIpConnectionManager(
                    node.loggingService,
                    node.getHazelcastThreadGroup(),
                    ioService,
                    node.nodeEngine.getMetricsRegistry(),
                    serverSocketChannel);
        }
    }

}
