package com.hazelcast.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterShutdownTest extends HazelcastTestSupport {

    @Before
    public void setUp() {
        setLoggingLog4j();
        setLogLevel(Level.TRACE);
    }

    @After
    public void tearDown() {
        resetLogLevel();
    }

    @Test
    public void cluster_mustBeShutDown_by_singleMember_when_clusterState_ACTIVE() {
        testClusterShutdownWithSingleMember(ClusterState.ACTIVE);
    }

    @Test
    public void cluster_mustBeShutDown_by_singleMember_when_clusterState_FROZEN() {
        testClusterShutdownWithSingleMember(ClusterState.FROZEN);
    }

    @Test
    public void cluster_mustBeShutDown_by_singleMember_when_clusterState_PASSIVE() {
        testClusterShutdownWithSingleMember(ClusterState.PASSIVE);
    }

    @Test
    public void cluster_mustBeShutDown_by_multipleMembers_when_clusterState_PASSIVE() {
        testClusterShutdownWithMultipleMembers(6, 3);
    }

    @Test
    public void cluster_mustBeShutDown_by_allMembers_when_clusterState_PASSIVE() {
        testClusterShutdownWithMultipleMembers(6, 6);
    }

    @Test
    public void whenClusterIsAlreadyShutdown_thenLifecycleServiceShutdownShouldDoNothing() {
        HazelcastInstance instance = testClusterShutdownWithSingleMember(ClusterState.ACTIVE);

        instance.getLifecycleService().shutdown();
    }

    private HazelcastInstance testClusterShutdownWithSingleMember(ClusterState clusterState) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz1 = instances[0];
        Node[] nodes = getNodes(instances);

        hz1.getCluster().changeClusterState(clusterState);
        hz1.getCluster().shutdown();

        assertNodesShutDownEventually(nodes);

        return hz1;
    }

    private void testClusterShutdownWithMultipleMembers(int clusterSize, int nodeCountToTriggerShutdown) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances();

        instances[0].getCluster().changeClusterState(ClusterState.PASSIVE);
        Node[] nodes = getNodes(instances);

        final CountDownLatch latch = new CountDownLatch(1);

        for (int i = 0; i < nodeCountToTriggerShutdown; i++) {
            final HazelcastInstance instance = instances[i];
            final Runnable shutdownRunnable = new Runnable() {
                @Override
                public void run() {
                    assertOpenEventually(latch);
                    instance.getCluster().shutdown();
                }
            };

            new Thread(shutdownRunnable).start();
        }

        latch.countDown();
        assertNodesShutDownEventually(nodes);
    }

    private static Node[] getNodes(HazelcastInstance[] instances) {
        Node[] nodes = new Node[instances.length];
        for (int i = 0; i < instances.length; i++) {
            nodes[i] = getNode(instances[i]);
        }
        return nodes;
    }

    private static void assertNodesShutDownEventually(Node[] nodes) {
        for (final Node node : nodes) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {
                    assertEquals(NodeState.SHUT_DOWN, node.getState());
                }
            });
        }
    }
}
