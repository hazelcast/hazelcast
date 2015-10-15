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

import static com.hazelcast.cluster.AdvancedClusterStateTest.changeClusterStateEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterShutdownTest extends HazelcastTestSupport {

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
        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], clusterState);

        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                instances[0].getCluster().shutdown();
            }
        });

        thread.start();

        assertNodesShutDownEventually(instances);
    }

    private void testClusterShutdownWithMultipleMembers(final int clusterSize, final int nodeCountToTriggerShutdown) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        final HazelcastInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);
        final CountDownLatch latch = new CountDownLatch(1);
        final Runnable shutdownRunnable = new Runnable() {
            @Override
            public void run() {
                assertOpenEventually(latch);
                instances[0].getCluster().shutdown();
            }
        };

        for (int i = 0; i < nodeCountToTriggerShutdown; i++) {
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
