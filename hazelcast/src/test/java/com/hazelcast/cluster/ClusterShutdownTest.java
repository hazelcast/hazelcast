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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

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

    public static Node[] getNodes(HazelcastInstance[] instances) {
        Node[] nodes = new Node[instances.length];
        for (int i = 0; i < instances.length; i++) {
            nodes[i] = getNode(instances[i]);
        }
        return nodes;
    }

    public static void assertNodesShutDownEventually(Node[] nodes) {
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
