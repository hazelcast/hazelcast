package com.hazelcast.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeState;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterShutdownTest
        extends HazelcastTestSupport {

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

    private void testClusterShutdownWithSingleMember(final ClusterState clusterState) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance[] instances = factory.newInstances();

        final HazelcastInstance hz1 = instances[0];

        hz1.getCluster().changeClusterState(clusterState);
        hz1.getCluster().shutdown();

        assertNodesShutDownEventually(instances);
    }

    private void testClusterShutdownWithMultipleMembers(final int clusterSize, final int nodeCountToTriggerShutdown) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        final HazelcastInstance[] instances = factory.newInstances();

        instances[0].getCluster().changeClusterState(ClusterState.PASSIVE);

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
        assertNodesShutDownEventually(instances);
    }

    private void assertNodesShutDownEventually(HazelcastInstance[] instances) {
        for (final HazelcastInstance instance : instances) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {
                    assertEquals(NodeState.SHUT_DOWN, getNode(instance).getState());
                }
            });
        }
    }

}
