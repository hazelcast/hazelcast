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

package com.hazelcast.instance;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl_asyncInvokeOnPartitionTest.InvocationEntryProcessor.latch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NodeStateTest extends HazelcastTestSupport {

    @Test
    public void nodeState_isActive_whenInstanceStarted() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        assertEquals(NodeState.ACTIVE, getNode(hz).getState());
    }

    @Test
    public void nodeState_isShutdown_whenInstanceShutdown() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        Node node = getNode(hz);
        hz.shutdown();
        assertEquals(NodeState.SHUT_DOWN, node.getState());
    }

    @Test
    public void nodeState_isShutdown_whenInstanceTerminated() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        Node node = getNode(hz);
        hz.shutdown();
        assertEquals(NodeState.SHUT_DOWN, node.getState());
    }

    @Test
    public void multipleShutdowns_Allowed() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        Node node = getNode(hz);

        for (int i = 0; i < 3; i++) {
            node.shutdown(false);
        }
    }

    @Test
    public void concurrentShutdowns_Allowed() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        final Node node = getNode(hz);

        Thread[] shutdownThreads = new Thread[3];
        for (int i = 0; i < shutdownThreads.length; i++) {
            Thread thread = new Thread() {
                public void run() {
                    node.shutdown(false);
                }
            };
            thread.start();
            shutdownThreads[i] = thread;
        }

        for (Thread thread : shutdownThreads) {
            thread.join(TimeUnit.MINUTES.toMillis(1));
        }
    }

    @Test
    public void shouldReject_NormalOperationInvocation_whilePassive() throws Exception {
        InvocationTask task = new InvocationTask() {
            @Override
            public void invoke(NodeEngine nodeEngine) throws Exception {
                Future<Object> future = nodeEngine.getOperationService()
                        .invokeOnPartition(null, new DummyOperation(), 1);
                try {
                    future.get();
                    fail("Invocation should fail while node is passive!");
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    assertTrue("Cause: " + cause, cause instanceof HazelcastInstanceNotActiveException);
                }
            }
        };

        testInvocation_whilePassive(task);
    }

    @Test
    public void shouldReject_NormalOperationExecution_whilePassive() throws Exception {
        InvocationTask task = new InvocationTask() {
            @Override
            public void invoke(NodeEngine nodeEngine) throws Exception {
                final CountDownLatch latch = new CountDownLatch(1);
                Operation op = new DummyOperation() {
                    @Override
                    public void onExecutionFailure(Throwable e) {
                        latch.countDown();
                    }

                    @Override
                    public boolean returnsResponse() {
                        return false;
                    }
                };

                nodeEngine.getOperationService().run(op);
                assertOpenEventually(latch);
            }
        };

        testInvocation_whilePassive(task);
    }

    @Test
    public void shouldAllow_AllowedOperationInvocation_whilePassive() throws Exception {
        InvocationTask task = new InvocationTask() {
            @Override
            public void invoke(NodeEngine nodeEngine) throws Exception {
                Future<Object> future = nodeEngine.getOperationService()
                        .invokeOnTarget(null, new DummyAllowedDuringPassiveStateOperation(), nodeEngine.getThisAddress());
                future.get(1, TimeUnit.MINUTES);
            }
        };

        testInvocation_whilePassive(task);
    }

    @Test
    public void shouldAllow_AllowedOperationExecution_whilePassive() throws Exception {
        InvocationTask task = new InvocationTask() {
            @Override
            public void invoke(NodeEngine nodeEngine) throws Exception {
                final CountDownLatch latch = new CountDownLatch(1);
                Operation op = new DummyAllowedDuringPassiveStateOperation() {
                    @Override
                    public void afterRun() throws Exception {
                        latch.countDown();
                    }

                    @Override
                    public boolean returnsResponse() {
                        return false;
                    }
                };

                nodeEngine.getOperationService().run(op);
                assertOpenEventually(latch);
            }
        };

        testInvocation_whilePassive(task);
    }

    private void testInvocation_whilePassive(InvocationTask invocationTask) throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance hz = factory.newHazelcastInstance();
        final Node node = getNode(hz);

        changeClusterStateEventually(hz, ClusterState.PASSIVE);

        assertEquals(NodeState.PASSIVE, node.getState());

        try {
            invocationTask.invoke(getNodeEngineImpl(hz));
        } catch (Throwable e) {
            // countdown-latch on failure
            latch.countDown();
            throw ExceptionUtil.rethrow(e);
        }
    }

    private interface InvocationTask {
        void invoke(NodeEngine nodeEngine) throws Exception;
    }

    private static class DummyOperation extends Operation {
        @Override
        public void run() throws Exception {
        }
    }

    private static class DummyAllowedDuringPassiveStateOperation extends Operation implements AllowedDuringPassiveState {
        @Override
        public void run() throws Exception {
        }
    }
}
