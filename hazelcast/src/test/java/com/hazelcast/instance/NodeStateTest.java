/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.AllowedDuringShutdown;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    public void shouldReject_NormalOperationInvocation_whileShuttingDown() throws Exception {
        InvocationTask task = new InvocationTask() {
            @Override
            public void invoke(NodeEngine nodeEngine) throws Exception {
                Future<Object> future = nodeEngine.getOperationService()
                        .invokeOnPartition(null, new DummyOperation(), 1);
                try {
                    future.get();
                    fail("Invocation should fail while node is shutting down!");
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    assertTrue("Cause: " + cause, cause instanceof HazelcastInstanceNotActiveException);
                }
            }
        };

        testInvocation_whileShuttingDown(task);
    }

    @Test
    public void shouldReject_NormalOperationExecution_whileShuttingDown() throws Exception {
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

                nodeEngine.getOperationService().runOperationOnCallingThread(op);
                assertOpenEventually(latch);
            }
        };

        testInvocation_whileShuttingDown(task);
    }

    @Test
    public void shouldAllow_AllowedOperationInvocation_whileShuttingDown() throws Exception {
        InvocationTask task = new InvocationTask() {
            @Override
            public void invoke(NodeEngine nodeEngine) throws Exception {
                Future<Object> future = nodeEngine.getOperationService()
                        .invokeOnTarget(null, new DummyAllowedDuringShutdownOperation(), nodeEngine.getThisAddress());
                future.get(1, TimeUnit.MINUTES);
            }
        };

        testInvocation_whileShuttingDown(task);
    }

    @Test
    public void shouldAllow_AllowedOperationExecution_whileShuttingDown() throws Exception {
        InvocationTask task = new InvocationTask() {
            @Override
            public void invoke(NodeEngine nodeEngine) throws Exception {
                final CountDownLatch latch = new CountDownLatch(1);
                Operation op = new DummyAllowedDuringShutdownOperation() {
                    @Override
                    public void afterRun() throws Exception {
                        latch.countDown();
                    }

                    @Override
                    public boolean returnsResponse() {
                        return false;
                    }
                };

                nodeEngine.getOperationService().runOperationOnCallingThread(op);
                assertOpenEventually(latch);
            }
        };

        testInvocation_whileShuttingDown(task);
    }

    private void testInvocation_whileShuttingDown(InvocationTask invocationTask) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Config config = new Config();
        config.getServicesConfig().addServiceConfig(
                new ServiceConfig().setEnabled(true)
                        .setName("bs").setServiceImpl(new BlockingService(latch)));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance hz = factory.newHazelcastInstance(config);
        final Node node = getNode(hz);

        final Thread shutdownThread = new Thread() {
            public void run() {
                hz.shutdown();
            }
        };
        shutdownThread.start();

        waitNodeStateToBeShuttingDown(node);

        try {
            invocationTask.invoke(getNodeEngineImpl(hz));
        } catch (Throwable e) {
            // countdown-latch on failure
            latch.countDown();
            throw ExceptionUtil.rethrow(e);
        }

        assertEquals(NodeState.SHUTTING_DOWN, node.getState());
        latch.countDown();
        shutdownThread.join(TimeUnit.MINUTES.toMillis(1));

        assertEquals(NodeState.SHUT_DOWN, node.getState());
    }

    private interface InvocationTask {
        void invoke(NodeEngine nodeEngine) throws Exception;
    }

    private static void waitNodeStateToBeShuttingDown(final Node node) {
        assertEqualsEventually(new Callable<NodeState>() {
            @Override
            public NodeState call() throws Exception {
                return node.getState();
            }
        }, NodeState.SHUTTING_DOWN);
    }

    private static class BlockingService implements ManagedService {

        private final CountDownLatch latch;

        BlockingService(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
            try {
                latch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class DummyOperation extends AbstractOperation {
        @Override
        public void run() throws Exception {
        }
    }

    private static class DummyAllowedDuringShutdownOperation
            extends AbstractOperation implements AllowedDuringShutdown {
        @Override
        public void run() throws Exception {
        }
    }

}
