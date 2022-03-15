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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.spi.impl.operationparker.impl.OperationParkerImpl;
import com.hazelcast.spi.impl.operationservice.AbstractWaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_NetworkSplitTest extends HazelcastTestSupport {

    @Test
    public void testWaitingInvocations_whenNodeSplitFromCluster() {
        SplitAction action = new FullSplitAction();
        testWaitingInvocations_whenNodeSplitFromCluster(action);
    }

    @Test
    public void testWaitingInvocations_whenNodePartiallySplitFromCluster() {
        SplitAction action = new PartialSplitAction();
        testWaitingInvocations_whenNodeSplitFromCluster(action);
    }

    private void testWaitingInvocations_whenNodeSplitFromCluster(SplitAction action) {
        Config config = createConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        warmUpPartitions(hz1, hz2, hz3);
        int partitionId = getPartitionId(hz2);

        Operation op = new AlwaysBlockingOperation();
        Future<Object> future = getNodeEngineImpl(hz3).getOperationService().invokeOnPartition("", op, partitionId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                final OperationParkerImpl waitNotifyService3 = (OperationParkerImpl) getNodeEngineImpl(hz2).getOperationParker();
                assertEquals(1, waitNotifyService3.getTotalParkedOperationCount());
            }
        });

        // execute the given split action
        action.run(hz1, hz2, hz3);

        unblock(hz1, hz2, hz3);

        // Let node3 detect the split and merge it back to other two.
        ClusterServiceImpl clusterService3 = (ClusterServiceImpl) getClusterService(hz3);
        clusterService3.merge(getAddress(hz1));

        assertClusterSizeEventually(3, hz1, hz2, hz3);

        try {
            future.get(1, TimeUnit.MINUTES);
            fail("Future.get() should fail with a MemberLeftException!");
        } catch (MemberLeftException expected) {
            ignore(expected);
        } catch (Exception e) {
            fail(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    @Test
    public void testWaitNotifyService_whenNodeSplitFromCluster() {
        SplitAction action = new FullSplitAction();
        testWaitNotifyService_whenNodeSplitFromCluster(action);
    }

    @Test
    public void testWaitNotifyService_whenNodePartiallySplitFromCluster() {
        SplitAction action = new PartialSplitAction();
        testWaitNotifyService_whenNodeSplitFromCluster(action);
    }

    private void testWaitNotifyService_whenNodeSplitFromCluster(SplitAction action) {
        Config config = createConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        warmUpPartitions(hz1, hz2, hz3);
        int partitionId = getPartitionId(hz3);

        getNodeEngineImpl(hz1).getOperationService().invokeOnPartition("", new AlwaysBlockingOperation(), partitionId);

        final OperationParkerImpl waitNotifyService3 = (OperationParkerImpl) getNodeEngineImpl(hz3).getOperationParker();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, waitNotifyService3.getTotalParkedOperationCount());
            }
        });

        action.run(hz1, hz2, hz3);

        // create a new node to prevent same partition assignments
        // after node3 rejoins
        factory.newHazelcastInstance(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, getPartitionService(hz1).getMigrationQueueSize());
            }
        });

        unblock(hz1, hz2, hz3);

        // Let node3 detect the split and merge it back to other two.
        ClusterServiceImpl clusterService3 = (ClusterServiceImpl) getClusterService(hz3);
        clusterService3.merge(getAddress(hz1));

        assertClusterSizeEventually(4, hz1, hz2, hz3);

        assertEquals(0, waitNotifyService3.getTotalParkedOperationCount());
    }

    private Config createConfig() {
        Config config = new Config();
        config.setClusterName(generateRandomString(10));
        config.setProperty(ClusterProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "10");
        return config;
    }

    private void unblock(HazelcastInstance instance1, HazelcastInstance instance2, HazelcastInstance instance3) {
        unblockCommunicationBetween(instance1, instance2);
        unblockCommunicationBetween(instance1, instance3);
        unblockCommunicationBetween(instance2, instance3);
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

        @Override
        public ExceptionAction onInvocationException(Throwable throwable) {
            return ExceptionAction.THROW_EXCEPTION;
        }
    }

    private interface SplitAction {
        void run(HazelcastInstance instance1, HazelcastInstance instance2, HazelcastInstance instance3);
    }

    private static class FullSplitAction implements SplitAction {

        @Override
        public void run(final HazelcastInstance instance1, final HazelcastInstance instance2, final HazelcastInstance instance3) {
            // Artificially create a network-split
            blockCommunicationBetween(instance1, instance3);
            blockCommunicationBetween(instance2, instance3);

            closeConnectionBetween(instance1, instance3);
            closeConnectionBetween(instance2, instance3);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(2, getNodeEngineImpl(instance1).getClusterService().getSize());
                    assertEquals(2, getNodeEngineImpl(instance2).getClusterService().getSize());
                    assertEquals(1, getNodeEngineImpl(instance3).getClusterService().getSize());
                }
            });
        }
    }

    private static class PartialSplitAction implements SplitAction {

        @Override
        public void run(final HazelcastInstance instance1, final HazelcastInstance instance2, final HazelcastInstance instance3) {
            // Artificially create a partial network-split;
            // node1 and node2 will be split from node3
            // but node 3 will not be able to detect that.
            blockCommunicationBetween(instance1, instance3);
            blockCommunicationBetween(instance2, instance3);
            suspectMember(getNode(instance1), getNode(instance3));

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(2, getNodeEngineImpl(instance1).getClusterService().getSize());
                    assertEquals(2, getNodeEngineImpl(instance2).getClusterService().getSize());
                    assertEquals(3, getNodeEngineImpl(instance3).getClusterService().getSize());
                }
            });
        }
    }
}
