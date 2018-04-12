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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
public class TaskletExecutionServiceTest extends JetTestSupport {

    private static final int THREAD_COUNT = 4;

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();

    private TaskletExecutionService es;
    private ClassLoader classLoaderMock;

    @Before
    public void before() {
        HazelcastInstance hzMock = mock(HazelcastInstance.class);
        LoggingService loggingService = mock(LoggingService.class);
        Mockito.when(hzMock.getName()).thenReturn("test-hz-instance");
        Mockito.when(hzMock.getLoggingService()).thenReturn(loggingService);
        Mockito.when(loggingService.getLogger(TaskletExecutionService.class))
               .thenReturn(Logger.getLogger(TaskletExecutionService.class));
        es = new TaskletExecutionService(hzMock, THREAD_COUNT);
        classLoaderMock = mock(ClassLoader.class);
    }

    @After
    public void after() {
        es.shutdown();
    }

    @Test
    public void when_blockingTask_then_executed() {
        // Given
        final MockTasklet t = new MockTasklet().blocking();

        // When
        executeAndJoin(singletonList(t));

        // Then
        t.assertDone();
    }

    @Test
    public void when_nonBlockingTask_then_executed() {
        // Given
        final MockTasklet t = new MockTasklet();

        // When
        executeAndJoin(singletonList(t));

        // Then
        t.assertDone();
    }

    @Test(expected = CompletionException.class)
    public void when_nonBlockingAndInitFails_then_futureFails() {
        // Given
        final MockTasklet t = new MockTasklet().initFails();

        // When
        executeAndJoin(singletonList(t));

        // Then
        t.assertDone();
    }

    @Test(expected = CompletionException.class)
    public void when_blockingAndInitFails_then_futureFails() {
        // Given
        final MockTasklet t = new MockTasklet().blocking().initFails();

        // When - Then
        executeAndJoin(singletonList(t));
    }

    @Test(expected = CompletionException.class)
    public void when_nonBlockingAndCallFails_then_futureFails() {
        // Given
        final MockTasklet t = new MockTasklet().callFails();

        // When - Then
        executeAndJoin(singletonList(t));
    }

    @Test(expected = CompletionException.class)
    public void when_blockingAndCallFails_then_futureFails() {
        // Given
        final MockTasklet t = new MockTasklet().blocking().callFails();

        // When - Then
        executeAndJoin(singletonList(t));
    }

    @Test
    public void when_shutdown_then_submitFails() {
        // Given
        es.beginExecute(singletonList(new MockTasklet()), new CompletableFuture<>(), classLoaderMock);
        es.beginExecute(singletonList(new MockTasklet()), new CompletableFuture<>(), classLoaderMock);

        // When
        es.shutdown();

        // Then
        exceptionRule.expect(IllegalStateException.class);
        es.beginExecute(singletonList(new MockTasklet()), new CompletableFuture<>(), classLoaderMock);
    }

    @Test
    public void when_manyCallsWithSomeStalling_then_eventuallyDone() {
        // Given
        final List<MockTasklet> tasklets = asList(
                new MockTasklet().blocking().callsBeforeDone(10),
                new MockTasklet().callsBeforeDone(10));

        // When
        executeAndJoin(tasklets);

        // Then
        tasklets.forEach(MockTasklet::assertDone);
    }

    @Test
    public void when_workStealing_then_allComplete() {
        // Given
        final List<MockTasklet> tasklets =
                Stream.generate(() -> new MockTasklet().callsBeforeDone(1000))
                      .limit(100).collect(toList());

        // When
        executeAndJoin(tasklets);

        // Then
        tasklets.forEach(MockTasklet::assertDone);
    }

    @Test
    public void when_nonBlockingTaskletIsCancelled_then_completesEarly() throws Exception {
        // Given
        final List<MockTasklet> tasklets =
                Stream.generate(() -> new MockTasklet().callsBeforeDone(Integer.MAX_VALUE))
                      .limit(100).collect(toList());

        // When
        CompletableFuture<Void> f = es.beginExecute(tasklets, cancellationFuture, classLoaderMock);
        cancellationFuture.cancel(true);

        // Then
        tasklets.forEach(MockTasklet::assertNotDone);

        exceptionRule.expect(CancellationException.class);
        f.get();
    }

    @Test
    public void when_blockingTaskletIsCancelled_then_completeEarly() throws ExecutionException, InterruptedException {
        // Given
        final List<MockTasklet> tasklets =
                Stream.generate(() -> new MockTasklet().blocking().callsBeforeDone(Integer.MAX_VALUE))
                      .limit(100).collect(toList());

        // When
        CompletableFuture<Void> f = es.beginExecute(tasklets, cancellationFuture, classLoaderMock);
        cancellationFuture.cancel(true);

        // Then
        tasklets.forEach(MockTasklet::assertNotDone);

        exceptionRule.expect(CancellationException.class);
        f.get();
    }

    @Test
    public void when_blockingSleepingTaskletIsCancelled_then_completeEarly() throws Exception {
        // Given
        final List<MockTasklet> tasklets =
                Stream.generate(() -> new MockTasklet().sleeping().callsBeforeDone(Integer.MAX_VALUE))
                      .limit(100).collect(toList());

        // When
        CompletableFuture<Void> f = es.beginExecute(tasklets, cancellationFuture, classLoaderMock);
        cancellationFuture.cancel(true);

        // Then
        tasklets.forEach(MockTasklet::assertNotDone);
        assertTrueEventually(f::isDone);

        exceptionRule.expect(CancellationException.class);
        cancellationFuture.get();
    }

    @Test
    public void when_nonBlockingCancelled_then_doneCallBackFiredAfterActualDone() throws Exception {
        // Given
        CountDownLatch proceedLatch = new CountDownLatch(1);
        final List<MockTasklet> tasklets =
                Stream.generate(() -> new MockTasklet().waitOnLatch(proceedLatch).callsBeforeDone(Integer.MAX_VALUE))
                      .limit(100).collect(toList());

        // When
        CompletableFuture<Void> f = es.beginExecute(tasklets, cancellationFuture, classLoaderMock);

        cancellationFuture.cancel(true);

        // Then
        assertFalse("future should not be completed until tasklets are completed.", f.isDone());

        proceedLatch.countDown();

        assertTrueEventually(() -> {
            assertTrue("future should be completed eventually", f.isDone());
        });

        exceptionRule.expect(CancellationException.class);
        cancellationFuture.get();
    }

    @Test
    public void when_twoNonBlockingTasklets_then_differentWorker() throws Exception {
        // Given
        TaskletAssertingThreadLocal t1 = new TaskletAssertingThreadLocal();
        TaskletAssertingThreadLocal t2 = new TaskletAssertingThreadLocal();
        assertTrue(t1.isCooperative());

        // When
        CompletableFuture<Void> f1 = es.beginExecute(singletonList(t1), new CompletableFuture<>(), classLoaderMock);
        CompletableFuture<Void> f2 = es.beginExecute(singletonList(t2), new CompletableFuture<>(), classLoaderMock);
        f1.join();
        f2.join();

        // Then
        // -- assertions are inside TaskletAssertingThreadLocal and will fail, if t1 and t2 are running on the same thread
    }

    @Test
    public void when_tryCompleteOnReturnedFuture_then_fails() {
        // Given
        final MockTasklet t = new MockTasklet().callsBeforeDone(Integer.MAX_VALUE);
        CompletableFuture<Void> f = es.beginExecute(singletonList(t), cancellationFuture, classLoaderMock);

        // When - Then
        exceptionRule.expect(UnsupportedOperationException.class);
        f.complete(null);
    }

    @Test
    public void when_tryCompleteExceptionallyOnReturnedFuture_then_fails() {
        // Given
        final MockTasklet t = new MockTasklet().callsBeforeDone(Integer.MAX_VALUE);
        CompletableFuture<Void> f = es.beginExecute(singletonList(t), cancellationFuture, classLoaderMock);

        // When - Then
        exceptionRule.expect(UnsupportedOperationException.class);
        f.completeExceptionally(new RuntimeException());
    }

    @Test
    public void when_tryCancelOnReturnedFuture_then_fails() {
        // Given
        final MockTasklet t = new MockTasklet().callsBeforeDone(Integer.MAX_VALUE);
        CompletableFuture<Void> f = es.beginExecute(singletonList(t), cancellationFuture, classLoaderMock);

        // When - Then
        exceptionRule.expect(UnsupportedOperationException.class);
        f.cancel(true);
    }

    @Test
    public void when_cancellationFutureCompleted_then_fails() throws Throwable {
        // Given
        final MockTasklet t = new MockTasklet().callsBeforeDone(Integer.MAX_VALUE);
        CompletableFuture<Void> f = es.beginExecute(singletonList(t), cancellationFuture, classLoaderMock);

        // When - Then
        cancellationFuture.complete(null);

        exceptionRule.expect(IllegalStateException.class);
        try {
            f.join();
        } catch (CompletionException e) {
            throw peel(e);
        }
    }

    @Test
    public void workStealing_stressTest() throws Exception {
        final List<SynchronizationTestTasklet> tasklets =
                IntStream.range(0, 10000)
                .mapToObj(SynchronizationTestTasklet::new)
                .collect(toList());

        CompletableFuture<Void> f = es.beginExecute(tasklets, cancellationFuture, classLoaderMock);
        Collections.shuffle(tasklets);
        long start = System.nanoTime();
        for (int i = 0; i < tasklets.size(); i++) {
            tasklets.get(i).terminated = true;
            LockSupport.parkNanos(MILLISECONDS.toNanos(i) - (System.nanoTime() - start));
        }
        // get the future to see eventual exception in the tasklets
        f.get();
        // when run locally it happened between 5000-5500 times
        assertGreaterOrEquals("tasklet on different thread count",
                tasklets.stream().filter(t -> t.sawDifferentThread).count(), 100);
    }

    /**
     * A tasklet that has a properly synchronized and a non-synchronized
     * counter. If tasklet is externally synchronized properly, both counters
     * should have same value all the time.
     */
    private static final class SynchronizationTestTasklet implements Tasklet {

        private final AtomicInteger counter1 = new AtomicInteger();
        private final int index;
        private int counter2;
        private volatile boolean terminated;
        private boolean sawDifferentThread;
        private long lastThreadId = -1;

        SynchronizationTestTasklet(int index) {
            this.index = index;
        }

        @Nonnull
        @Override
        public ProgressState call() {
            long currentThreadId = Thread.currentThread().getId();
            if (lastThreadId != -1) {
                sawDifferentThread |= currentThreadId != lastThreadId;
            }
            lastThreadId = currentThreadId;
            int v1 = counter1.getAndIncrement();
            int v2 = counter2++;
            assertEquals(v1, v2);
            return terminated ? ProgressState.DONE : ProgressState.MADE_PROGRESS;
        }

        @Override
        public boolean isCooperative() {
            return true;
        }

        @Override
        public String toString() {
            return "SynchronizationTestTasklet-" + index;
        }
    }

    private void executeAndJoin(List<MockTasklet> tasklets) {
        CompletableFuture<Void> f = es.beginExecute(tasklets, cancellationFuture, classLoaderMock);
        f.join();
    }

    static class MockTasklet implements Tasklet {

        boolean isBlocking;
        boolean initFails;
        boolean callFails;
        int callsBeforeDone;

        private boolean willMakeProgress = true;
        private boolean isSleeping;
        private CountDownLatch latch;

        @Override
        public boolean isCooperative() {
            return !isBlocking;
        }

        @Nonnull
        @Override
        public ProgressState call() {
            if (callFails) {
                throw new RuntimeException("mock call failure");
            }
            if (isSleeping) {
                try {
                    Thread.currentThread().join();
                } catch (InterruptedException e) {
                    return DONE;
                }
            }
            if (latch != null) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw sneakyThrow(e);
                }
            }
            willMakeProgress = !willMakeProgress;
            return callsBeforeDone-- == 0 ? DONE
                    : willMakeProgress ? MADE_PROGRESS
                    : NO_PROGRESS;
        }

        @Override
        public void init() {
            if (initFails) {
                throw new RuntimeException("mock init failure");
            }
        }

        MockTasklet blocking() {
            isBlocking = true;
            return this;
        }

        MockTasklet sleeping() {
            isSleeping = true;
            isBlocking = true;
            return this;
        }

        MockTasklet waitOnLatch(CountDownLatch latch) {
            this.latch = latch;
            return this;
        }

        MockTasklet initFails() {
            initFails = true;
            return this;
        }

        MockTasklet callFails() {
            callFails = true;
            return this;
        }

        MockTasklet callsBeforeDone(int count) {
            callsBeforeDone = count;
            return this;
        }

        void assertDone() {
            assertEquals("Tasklet wasn't done", -1, callsBeforeDone);
        }

        void assertNotDone() {
            assertNotEquals("Tasklet was done", -1, callsBeforeDone);
        }
    }

    private static class TaskletAssertingThreadLocal implements Tasklet {

        private static ThreadLocal<Integer> threadLocal = ThreadLocal.withInitial(() -> 0);

        private int callCount;

        @Nonnull
        @Override
        public ProgressState call() {
            assertEquals("the ThreadLocal was updated from multiple tasklets", callCount, threadLocal.get().intValue());
            threadLocal.set(threadLocal.get() + 1);
            callCount++;
            LockSupport.parkNanos(10_000_000);
            return callCount > 50 ? DONE : MADE_PROGRESS;
        }
    }
}
