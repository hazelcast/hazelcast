/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractWaitNotifyKey;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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

    @Test
    public void clusterShutdown_shouldNotBeRejected_byBackpressure() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.toString(), "1");
        config.setProperty(GroupProperty.BACKPRESSURE_ENABLED.toString(), "true");
        config.setProperty(GroupProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS.toString(), "100");
        config.setProperty(GroupProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION.toString(), "3");

        HazelcastInstance hz = createHazelcastInstance(config);
        final InternalOperationService operationService = getOperationService(hz);
        final Address address = getAddress(hz);

        for (int i = 0; i < 10; i++) {
            Future<Object> future = spawn(new Callable<Object>() {
                @Override
                public Object call() {
                    operationService.invokeOnTarget(null, new AlwaysBlockingOperation(), address);
                    return null;
                }
            });
            try {
                future.get();
            } catch (ExecutionException e) {
                assertInstanceOf(HazelcastOverloadException.class, e.getCause());
            }
        }

        Node node = getNode(hz);
        hz.getCluster().shutdown();

        assertFalse(hz.getLifecycleService().isRunning());
        assertEquals(NodeState.SHUT_DOWN, node.getState());
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
                public void run() {
                    assertEquals(NodeState.SHUT_DOWN, node.getState());
                }
            });
        }
    }

    private static class AlwaysBlockingOperation extends Operation implements BlockingOperation {

        @Override
        public void run() throws Exception {
        }

        @Override
        public WaitNotifyKey getWaitKey() {
            return new AbstractWaitNotifyKey(getServiceName(), "test") {
            };
        }

        @Override
        public boolean shouldWait() {
            return true;
        }

        @Override
        public void onWaitExpire() {
            sendResponse(new TimeoutException());
        }

        @Override
        public String getServiceName() {
            return "AlwaysBlockingOperationService";
        }
    }
}
