/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.worker;

import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.io.InboundBatch;
import com.hazelcast.sql.impl.exec.io.InboundHandler;
import com.hazelcast.sql.impl.exec.io.OutboundHandler;
import com.hazelcast.sql.impl.operation.QueryAbstractExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryFlowControlExchangeOperation;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.state.QueryStateCallback;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryFragmentExecutableTest extends HazelcastTestSupport {

    private QueryFragmentWorkerPool pool;

    @After
    public void after() {
        if (pool != null) {
            pool.stop();

            pool = null;
        }
    }

    @Test
    public void testSetupIsCalledOnlyOnce() {
        pool = createPool();

        TestStateCallback stateCallback = new TestStateCallback();
        TestExec exec = new TestExec().setPayload(new ResultExecPayload(IterationResult.FETCHED));

        QueryFragmentExecutable fragmentExecutable = new QueryFragmentExecutable(
            stateCallback,
            Collections.emptyList(),
            exec,
            Collections.emptyMap(),
            Collections.emptyMap(),
            pool
        );

        fragmentExecutable.run();
        assertEquals(1, exec.getSetupInvocationCount());

        fragmentExecutable.run();
        assertEquals(1, exec.getSetupInvocationCount());
    }

    @Test
    public void testAdvanceIsCalledUntilCompletion() {
        pool = createPool();

        TestStateCallback stateCallback = new TestStateCallback();
        TestExec exec = new TestExec();

        QueryFragmentExecutable fragmentExecutable = new QueryFragmentExecutable(
            stateCallback,
            Collections.emptyList(),
            exec,
            Collections.emptyMap(),
            Collections.emptyMap(),
            pool
        );

        exec.setPayload(new ResultExecPayload(IterationResult.WAIT));
        fragmentExecutable.run();
        fragmentExecutable.run();
        assertEquals(2, exec.getAdvanceInvocationCount());

        exec.setPayload(new ResultExecPayload(IterationResult.FETCHED));
        fragmentExecutable.run();
        fragmentExecutable.run();
        assertEquals(4, exec.getAdvanceInvocationCount());
        assertEquals(0, stateCallback.getFragmentFinishedInvocationCount());

        exec.setPayload(new ResultExecPayload(IterationResult.FETCHED_DONE));
        fragmentExecutable.run();
        fragmentExecutable.run();
        assertEquals(5, exec.getAdvanceInvocationCount());
        assertEquals(1, stateCallback.getFragmentFinishedInvocationCount());
    }

    @Test
    public void testExceptionDuringExecution() {
        pool = createPool();

        TestStateCallback stateCallback = new TestStateCallback();
        TestExec exec = new TestExec();

        QueryFragmentExecutable fragmentExecutable = new QueryFragmentExecutable(
            stateCallback,
            Collections.emptyList(),
            exec,
            Collections.emptyMap(),
            Collections.emptyMap(),
            pool
        );

        // Throw an exception.
        exec.setPayload(new ExceptionExecPayload());
        fragmentExecutable.run();
        assertEquals(1, exec.getAdvanceInvocationCount());
        assertSame(ExceptionExecPayload.ERROR, stateCallback.getCancelException());

        // Make sure that no advance is possible after that.
        fragmentExecutable.run();
        assertEquals(1, exec.getAdvanceInvocationCount());
        assertEquals(0, stateCallback.getFragmentFinishedInvocationCount());
    }

    @Test
    public void testSchedule() throws Exception {
        pool = createPool();

        TestStateCallback stateCallback = new TestStateCallback();
        TestExec exec = new TestExec();

        AtomicBoolean flowControlNotified = new AtomicBoolean();

        InboundHandler inboundHandler = new InboundHandler() {
            @Override
            public void onBatch(InboundBatch batch, long remainingMemory) {
                // No-op.
            }

            @Override
            public void onFragmentExecutionCompleted() {
                flowControlNotified.set(true);
            }
        };

        QueryFragmentExecutable fragmentExecutable = new QueryFragmentExecutable(
            stateCallback,
            Collections.emptyList(),
            exec,
            Collections.singletonMap(1, inboundHandler),
            Collections.emptyMap(),
            pool
        );

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(1);

        exec.setPayload(() -> {
            startLatch.countDown();
            stopLatch.await();

            return IterationResult.FETCHED;
        });

        assertTrue(fragmentExecutable.schedule());
        startLatch.await();

        assertFalse(fragmentExecutable.schedule());
        stopLatch.countDown();

        assertTrueEventually(() -> assertTrue(flowControlNotified.get()));
    }

    /**
     * Concurrent test which submit messages from different threads and see if all of them are processed.
     */
    @Test
    public void testMessages() throws Exception {
        int repeatCount = 10;

        for (int i = 0; i < repeatCount; i++) {
            // Prepare data structures for messages.
            ConcurrentLinkedQueue<Long> inboundQueue = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<Long> outboundQueue = new ConcurrentLinkedQueue<>();

            Set<Long> inboundSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
            Set<Long> outboundSet = Collections.newSetFromMap(new ConcurrentHashMap<>());

            InboundHandler inboundHandler = new InboundHandler() {
                @Override
                public void onBatch(InboundBatch batch, long remainingMemory) {
                    inboundQueue.add(remainingMemory);
                }

                @Override
                public void onFragmentExecutionCompleted() {
                    // No-op.
                }
            };

            OutboundHandler outboundHandler = outboundQueue::add;

            // Prepare executable.
            QueryId queryId = QueryId.create(UUID.randomUUID());
            UUID callerId = UUID.randomUUID();
            int edgeId = 1;

            pool = createPool();

            TestStateCallback stateCallback = new TestStateCallback();

            TestExec exec = new TestExec().setPayload(() -> {
                while (!inboundQueue.isEmpty()) {
                    inboundSet.add(inboundQueue.poll());
                }

                while (!outboundQueue.isEmpty()) {
                    outboundSet.add(outboundQueue.poll());
                }

                return IterationResult.FETCHED;
            });

            QueryFragmentExecutable fragmentExecutable = new QueryFragmentExecutable(
                stateCallback,
                Collections.emptyList(),
                exec,
                Collections.singletonMap(edgeId, inboundHandler),
                Collections.singletonMap(edgeId, Collections.singletonMap(callerId, outboundHandler)),
                pool
            );

            assertEquals(1, fragmentExecutable.getInboxEdgeIds().size());
            assertEquals(1, fragmentExecutable.getOutboxEdgeIds().size());
            assertEquals(edgeId, (int) fragmentExecutable.getInboxEdgeIds().iterator().next());
            assertEquals(edgeId, (int) fragmentExecutable.getOutboxEdgeIds().iterator().next());

            // Start threads and wait for completion.
            int operationCount = 10_000;
            int threadCount = 8;

            AtomicInteger doneOperationCount = new AtomicInteger();
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            Runnable run = () -> {
                try {
                    while (true) {
                        int counter = doneOperationCount.getAndIncrement();

                        if (counter >= operationCount) {
                            break;
                        }

                        QueryAbstractExchangeOperation operation;

                        if (ThreadLocalRandom.current().nextBoolean()) {
                            operation = new QueryBatchExchangeOperation(
                                queryId,
                                edgeId,
                                UUID.randomUUID(),
                                EmptyRowBatch.INSTANCE,
                                false,
                                counter
                            );
                        } else {
                            operation = new QueryFlowControlExchangeOperation(queryId, edgeId, counter);
                        }

                        operation.setCallerId(callerId);

                        fragmentExecutable.addOperation(operation);
                        fragmentExecutable.schedule();
                    }
                } finally {
                    doneLatch.countDown();
                }
            };

            for (int j = 0; j < threadCount; j++) {
                new Thread(run).start();
            }

            doneLatch.await();

            // Make sure that all batches were drained.
            assertTrueEventually(() -> assertTrue(inboundQueue.isEmpty()));
            assertTrueEventually(() -> assertTrue(outboundQueue.isEmpty()));

            // Make sure that the executor observed all batches.
            assertTrueEventually(() -> assertEquals(operationCount, inboundSet.size() + outboundSet.size()));
        }
    }

    /**
     * Test that ensures propagation of cancel.
     */
    @Test
    public void testShutdown() throws Exception {
        pool = createPool();

        TestStateCallback stateCallback = new TestStateCallback();
        TestExec exec = new TestExec();

        QueryFragmentExecutable fragmentExecutable = new QueryFragmentExecutable(
            stateCallback,
            Collections.emptyList(),
            exec,
            Collections.emptyMap(),
            Collections.emptyMap(),
            pool
        );

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch blockLatch = new CountDownLatch(1);
        AtomicBoolean interruptCaught = new AtomicBoolean();

        exec.setPayload(() -> {
            startLatch.countDown();

            try {
                blockLatch.await();
            } catch (InterruptedException e) {
                interruptCaught.set(true);

                Thread.currentThread().interrupt();

                throw e;
            }

            return IterationResult.FETCHED;
        });

        // Await exec is reached.
        assertTrue(fragmentExecutable.schedule());
        startLatch.await();

        // Shutdown.
        pool.stop();

        assertTrueEventually(() -> assertTrue(interruptCaught.get()));
    }

    private QueryFragmentWorkerPool createPool() {
        return new QueryFragmentWorkerPool("instance", 4, new NoLogFactory().getLogger("logger"));
    }

    private interface ExecPayload {
        IterationResult run() throws Exception;
    }

    private static class ResultExecPayload implements ExecPayload {

        private final IterationResult result;

        private ResultExecPayload(IterationResult result) {
            this.result = result;
        }

        @Override
        public IterationResult run() throws Exception {
            return result;
        }
    }

    private static class ExceptionExecPayload implements ExecPayload {

        private static final Exception ERROR = new RuntimeException("Failed");

        @Override
        public IterationResult run() throws Exception {
            throw ERROR;
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private static class TestExec extends AbstractExec {

        private volatile ExecPayload payload;
        private volatile int setupInvocationCount;
        private volatile int advanceInvocationCount;

        private TestExec() {
            super(1);
        }

        @Override
        protected IterationResult advance0() {
            advanceInvocationCount++;

            try {
                return payload.run();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected RowBatch currentBatch0() {
            return null;
        }

        @Override
        protected void setup0(QueryFragmentContext ctx) {
            setupInvocationCount++;
        }

        private int getSetupInvocationCount() {
            return setupInvocationCount;
        }

        public int getAdvanceInvocationCount() {
            return advanceInvocationCount;
        }

        public TestExec setPayload(ExecPayload payload) {
            this.payload = payload;

            return this;
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private static class TestStateCallback implements QueryStateCallback {

        private volatile Exception cancelException;
        private volatile int fragmentFinishedInvocationCount;

        @Override
        public void onFragmentFinished() {
            fragmentFinishedInvocationCount++;
        }

        @Override
        public void cancel(Exception e) {
            cancelException = e;
        }

        @Override
        public void checkCancelled() {
            // No-op.
        }

        public Exception getCancelException() {
            return cancelException;
        }

        public int getFragmentFinishedInvocationCount() {
            return fragmentFinishedInvocationCount;
        }
    }
}
