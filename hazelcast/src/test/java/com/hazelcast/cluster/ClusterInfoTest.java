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

package com.hazelcast.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
    public void test_start_time_single_node_cluster() {
        HazelcastInstance h1 = factory.newHazelcastInstance();
        Node node1 = getNode(h1);
        assertNotEquals(Long.MIN_VALUE, node1.getClusterService().getClusterClock().getClusterStartTime());
    }

    @Test
    public void all_nodes_should_have_the_same_cluster_start_time_and_cluster_id() {
        HazelcastInstance h1 = factory.newHazelcastInstance();
        HazelcastInstance h2 = factory.newHazelcastInstance();
        HazelcastInstance h3 = factory.newHazelcastInstance();

        assertClusterSize(3, h1, h3);
        assertClusterSizeEventually(3, h2);

        Node node1 = getNode(h1);
        Node node2 = getNode(h2);
        Node node3 = getNode(h3);

        //All nodes should have same startTime
        final ClusterServiceImpl clusterService = node1.getClusterService();
        long node1ClusterStartTime = clusterService.getClusterClock().getClusterStartTime();
        long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        UUID node1ClusterId = clusterService.getClusterId();

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

        assertClusterSize(3, h1, h3);
        assertClusterSizeEventually(3, h2);

        Node node1 = getNode(h1);
        final ClusterServiceImpl clusterService = node1.getClusterService();
        long node1ClusterStartTime = clusterService.getClusterClock().getClusterStartTime();
        long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        UUID node1ClusterId = clusterService.getClusterId();

        assertTrue(clusterUpTime > 0);
        assertTrue(node1.isMaster());
        h1.shutdown();
        assertClusterSizeEventually(2, h2);

        HazelcastInstance h4 = factory.newHazelcastInstance();

        Node node2 = getNode(h2);
        Node node3 = getNode(h3);
        Node node4 = getNode(h4);

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
