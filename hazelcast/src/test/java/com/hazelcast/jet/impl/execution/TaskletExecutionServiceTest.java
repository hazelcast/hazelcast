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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.execution.TaskletExecutionService.TASKLET_INIT_CLOSE_EXECUTOR_NAME;
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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TaskletExecutionServiceTest extends JetTestSupport {

    private static final int THREAD_COUNT = 4;

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();

    private TaskletExecutionService tes;
    private ExecutorService executor;
    private ClassLoader classLoaderMock;

    @Before
    public void before() {
        executor = Executors.newCachedThreadPool();
        NodeEngineImpl neMock = mock(NodeEngineImpl.class);

        HazelcastInstance hzMock = mock(HazelcastInstance.class);
        when(neMock.getHazelcastInstance()).thenReturn(hzMock);
        when(hzMock.getName()).thenReturn("test-hz-instance");

        ExecutionService es = mock(ExecutionService.class);
        when(neMock.getExecutionService()).thenReturn(es);
        when(es.submit(eq(TASKLET_INIT_CLOSE_EXECUTOR_NAME), any(Runnable.class)))
                .then(invocationOnMock -> executor.submit(invocationOnMock.<Runnable>getArgument(1)));

        LoggingService loggingService = mock(LoggingService.class);
        when(neMock.getLoggingService()).thenReturn(loggingService);
        when(loggingService.getLogger(TaskletExecutionService.class))
               .thenReturn(Logger.getLogger(TaskletExecutionService.class));

        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(mock(ILogger.class), ProbeLevel.INFO);
        when(neMock.getMetricsRegistry()).thenReturn(metricsRegistry);

        HazelcastProperties properties = new HazelcastProperties(new Properties());
        tes = new TaskletExecutionService(neMock, THREAD_COUNT, properties);
        classLoaderMock = mock(ClassLoader.class);
    }

    @After
    public void after() {
        tes.shutdown();
        executor.shutdown();
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

    @Test
    public void when_manyNonBlockingAndSomeInitFails_then_allInitAwaited() {
        // Given
        List<MockTasklet> tasklets = asList(
           new MockTasklet().initLasts(100),
           new MockTasklet().initLasts(200).initFails(),
           new MockTasklet().initLasts(400),
           new MockTasklet().initLasts(800)
        );

        // When
        try {
            executeAndJoin(tasklets);
            fail();
        } catch (CompletionException e) {
            // expected
        }

        // Then
        tasklets.forEach(MockTasklet::assertInitDone);
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
        CompletableFuture<Void> f = tes.beginExecute(tasklets, cancellationFuture, classLoaderMock);
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
        CompletableFuture<Void> f = tes.beginExecute(tasklets, cancellationFuture, classLoaderMock);
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
        CompletableFuture<Void> f = tes.beginExecute(tasklets, cancellationFuture, classLoaderMock);
        cancellationFuture.cancel(true);

        // Then
        tasklets.forEach(MockTasklet::assertNotDone);
        assertTrueEventually(() -> assertTrue(f.isDone()), 10);

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
        CompletableFuture<Void> f = tes.beginExecute(tasklets, cancellationFuture, classLoaderMock);

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
    public void when_twoNonBlockingTasklets_then_differentWorker() {
        // Given
        TaskletAssertingThreadLocal t1 = new TaskletAssertingThreadLocal();
        TaskletAssertingThreadLocal t2 = new TaskletAssertingThreadLocal();
        assertTrue(t1.isCooperative());

        // When
        CompletableFuture<Void> f1 = tes.beginExecute(singletonList(t1), new CompletableFuture<>(), classLoaderMock);
        CompletableFuture<Void> f2 = tes.beginExecute(singletonList(t2), new CompletableFuture<>(), classLoaderMock);
        f1.join();
        f2.join();

        // Then
        // -- assertions are inside TaskletAssertingThreadLocal and will fail, if t1 and t2 are running on the same thread
    }

    @Test
    public void when_tryCompleteOnReturnedFuture_then_fails() {
        // Given
        final MockTasklet t = new MockTasklet().callsBeforeDone(Integer.MAX_VALUE);
        CompletableFuture<Void> f = tes.beginExecute(singletonList(t), cancellationFuture, classLoaderMock);

        // When - Then
        exceptionRule.expect(UnsupportedOperationException.class);
        f.complete(null);
    }

    @Test
    public void when_tryCompleteExceptionallyOnReturnedFuture_then_fails() {
        // Given
        final MockTasklet t = new MockTasklet().callsBeforeDone(Integer.MAX_VALUE);
        CompletableFuture<Void> f = tes.beginExecute(singletonList(t), cancellationFuture, classLoaderMock);

        // When - Then
        exceptionRule.expect(UnsupportedOperationException.class);
        f.completeExceptionally(new RuntimeException());
    }

    @Test
    public void when_tryCancelOnReturnedFuture_then_fails() {
        // Given
        final MockTasklet t = new MockTasklet().callsBeforeDone(Integer.MAX_VALUE);
        CompletableFuture<Void> f = tes.beginExecute(singletonList(t), cancellationFuture, classLoaderMock);

        // When - Then
        exceptionRule.expect(UnsupportedOperationException.class);
        f.cancel(true);
    }

    @Test
    public void when_cancellationFutureCompleted_then_fails() throws Throwable {
        // Given
        final MockTasklet t = new MockTasklet().callsBeforeDone(Integer.MAX_VALUE);
        CompletableFuture<Void> f = tes.beginExecute(singletonList(t), cancellationFuture, classLoaderMock);

        // When
        cancellationFuture.complete(null);

        // Then
        exceptionRule.expect(IllegalStateException.class);
        try {
            f.join();
        } catch (CompletionException e) {
            throw peel(e);
        }
    }

    private void executeAndJoin(List<MockTasklet> tasklets) {
        CompletableFuture<Void> f = tes.beginExecute(tasklets, cancellationFuture, classLoaderMock);
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
        private long initLastsMillis;
        private volatile boolean initDone;

        @Override
        public boolean isCooperative() {
            return !isBlocking;
        }

        @Nonnull @Override
        public ProgressState call() {
            if (callFails) {
                throw new RuntimeException("mock call failure");
            }
            if (isSleeping) {
                try {
                    Thread.sleep(500);
                    return NO_PROGRESS;
                } catch (InterruptedException e) {
                    throw new RuntimeException("Sleeping interrupted, this should not happen because " +
                            "we don't interrupt blocking workers", e);
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
            LockSupport.parkNanos(MILLISECONDS.toNanos(initLastsMillis));
            initDone = true;
            if (initFails) {
                throw new RuntimeException("mock init failure");
            }
        }

        boolean isInitDone() {
            return initDone;
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

        MockTasklet initLasts(long millis) {
            initLastsMillis = millis;
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

        void assertInitDone() {
            assertTrue(initDone);
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
