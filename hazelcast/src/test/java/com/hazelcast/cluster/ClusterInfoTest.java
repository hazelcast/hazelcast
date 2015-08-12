package com.hazelcast.cluster;


import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterInfoTest extends HazelcastTestSupport {


    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(4);
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void test_start_time_single_node_cluster() throws Exception {
        HazelcastInstance h1 = factory.newHazelcastInstance();
        Node node1 = TestUtil.getNode(h1);
        assertNotEquals(Long.MIN_VALUE, node1.getClusterService().getClusterClock().getClusterStartTime());
    }

    @Test
    public void all_nodes_should_have_the_same_cluster_start_time_and_cluster_id() throws Exception {

        HazelcastInstance h1 = factory.newHazelcastInstance();
        HazelcastInstance h2 = factory.newHazelcastInstance();
        HazelcastInstance h3 = factory.newHazelcastInstance();

        assertSizeEventually(3, h1.getCluster().getMembers());
        assertSizeEventually(3, h2.getCluster().getMembers());
        assertSizeEventually(3, h3.getCluster().getMembers());

        Node node1 = TestUtil.getNode(h1);
        Node node2 = TestUtil.getNode(h2);
        Node node3 = TestUtil.getNode(h3);

        //All nodes should have same startTime
        final ClusterServiceImpl clusterService = node1.getClusterService();
        long node1ClusterStartTime = clusterService.getClusterClock().getClusterStartTime();
        long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        String node1ClusterId = clusterService.getClusterId();

        assertTrue(clusterUpTime > 0);
        assertNotEquals(node1ClusterStartTime, Long.MIN_VALUE);
        assertEquals(node1ClusterStartTime, node2.getClusterService().getClusterClock().getClusterStartTime());
        assertEquals(node1ClusterStartTime, node3.getClusterService().getClusterClock().getClusterStartTime());

        //All nodes should have same clusterId
        assertNotNull(node1ClusterId);
        assertEquals(node1ClusterId, node2.getClusterService().getClusterId());
        assertEquals(node1ClusterId, node3.getClusterService().getClusterId());


    }

    @Test
    public void all_nodes_should_have_the_same_cluster_start_time_and_id_after_master_shutdown_and_new_node_join() {

        HazelcastInstance h1 = factory.newHazelcastInstance();
        HazelcastInstance h2 = factory.newHazelcastInstance();
        HazelcastInstance h3 = factory.newHazelcastInstance();

        assertSizeEventually(3, h1.getCluster().getMembers());
        assertSizeEventually(3, h2.getCluster().getMembers());
        assertSizeEventually(3, h3.getCluster().getMembers());

        Node node1 = TestUtil.getNode(h1);
        final ClusterServiceImpl clusterService = node1.getClusterService();
        long node1ClusterStartTime = clusterService.getClusterClock().getClusterStartTime();
        long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        String node1ClusterId = clusterService.getClusterId();

        assertTrue(clusterUpTime > 0);
        assertTrue(node1.isMaster());
        h1.shutdown();
        assertSizeEventually(2, h2.getCluster().getMembers());

        HazelcastInstance h4 = factory.newHazelcastInstance();

        Node node2 = TestUtil.getNode(h2);
        Node node3 = TestUtil.getNode(h3);
        Node node4 = TestUtil.getNode(h4);

        //All nodes should have the same cluster start time
        assertNotEquals(node1ClusterStartTime, Long.MIN_VALUE);
        assertEquals(node1ClusterStartTime, node2.getClusterService().getClusterClock().getClusterStartTime());
        assertEquals(node1ClusterStartTime, node3.getClusterService().getClusterClock().getClusterStartTime());
        assertEquals(node1ClusterStartTime, node4.getClusterService().getClusterClock().getClusterStartTime());

        //All nodes should have the same clusterId
        assertEquals(node1ClusterId, node2.getClusterService().getClusterId());
        assertEquals(node1ClusterId, node3.getClusterService().getClusterId());
        assertEquals(node1ClusterId, node4.getClusterService().getClusterId());

    }
}
