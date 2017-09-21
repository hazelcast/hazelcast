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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class TaskletExecutionServiceTest extends JetTestSupport {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final Consumer<CompletionStage<Void>> doneCallback = noopConsumer();

    private TaskletExecutionService es;
    private ClassLoader classLoaderMock;

    @Before
    public void before() {
        HazelcastInstance hzMock = mock(HazelcastInstance.class);
        LoggingService loggingService = mock(LoggingService.class);
        ILogger mockLogger = mock(ILogger.class);
        Mockito.when(hzMock.getName()).thenReturn("test-hz-instance");
        Mockito.when(hzMock.getLoggingService()).thenReturn(loggingService);
        Mockito.when(loggingService.getLogger(Mockito.<Class>any())).thenReturn(mockLogger);
        es = new TaskletExecutionService(hzMock, 4);
        classLoaderMock = mock(ClassLoader.class);
    }

    @Test
    public void when_blockingTask_then_executed() {
        // Given
        final MockTasklet t = new MockTasklet().blocking();

        // When
        es.execute(singletonList(t), doneCallback, classLoaderMock).toCompletableFuture().join();

        // Then
        t.assertDone();
    }

    @Test
    public void when_nonblockingTask_then_executed() {
        // Given
        final MockTasklet t = new MockTasklet();

        // When
        es.execute(singletonList(t), doneCallback, classLoaderMock).toCompletableFuture().join();

        // Then
        t.assertDone();
    }

    @Test(expected = CompletionException.class)
    public void when_nonblockingAndInitFails_then_futureFails() {
        // Given
        final MockTasklet t = new MockTasklet().initFails();

        // When
        es.execute(singletonList(t), doneCallback, classLoaderMock).toCompletableFuture().join();

        // Then
        t.assertDone();
    }

    @Test(expected = CompletionException.class)
    public void when_blockingAndInitFails_then_futureFails() {
        // Given
        final MockTasklet t = new MockTasklet().blocking().initFails();

        // When - Then
        es.execute(singletonList(t), doneCallback, classLoaderMock).toCompletableFuture().join();
    }

    @Test(expected = CompletionException.class)
    public void when_nonblockingAndCallFails_then_futureFails() {
        // Given
        final MockTasklet t = new MockTasklet().callFails();

        // When - Then
        es.execute(singletonList(t), doneCallback, classLoaderMock).toCompletableFuture().join();
    }

    @Test(expected = CompletionException.class)
    public void when_blockingAndCallFails_then_futureFails() {
        // Given
        final MockTasklet t = new MockTasklet().blocking().callFails();

        // When - Then
        es.execute(singletonList(t), doneCallback, classLoaderMock).toCompletableFuture().join();
    }

    @Test
    public void when_shutdown_then_submitFails() {
        // Given
        es.execute(singletonList(new MockTasklet()), doneCallback, classLoaderMock);
        es.execute(singletonList(new MockTasklet()), doneCallback, classLoaderMock);

        // When
        es.shutdown();

        // Then
        exceptionRule.expect(IllegalStateException.class);
        es.execute(singletonList(new MockTasklet()), doneCallback, classLoaderMock);
    }

    @Test
    public void when_manyCallsWithSomeStalling_then_eventuallyDone() {
        // Given
        final List<MockTasklet> tasklets = asList(
                new MockTasklet().blocking().callsBeforeDone(10),
                new MockTasklet().callsBeforeDone(10));

        // When
        es.execute(tasklets, doneCallback, classLoaderMock).toCompletableFuture().join();

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
        es.execute(tasklets, doneCallback, classLoaderMock).toCompletableFuture().join();

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
        CompletableFuture<Void> future = es.execute(tasklets, doneCallback, classLoaderMock).toCompletableFuture();
        future.cancel(true);

        // Then
        tasklets.forEach(MockTasklet::assertNotDone);

        exceptionRule.expect(CancellationException.class);
        future.get();
    }

    @Test
    public void when_blockingTaskletIsCancelled_then_completeEarly() throws ExecutionException, InterruptedException {
        // Given
        final List<MockTasklet> tasklets =
                Stream.generate(() -> new MockTasklet().blocking().callsBeforeDone(Integer.MAX_VALUE))
                      .limit(100).collect(toList());

        // When
        CompletableFuture<Void> future = es.execute(tasklets, doneCallback, classLoaderMock).toCompletableFuture();
        future.cancel(true);

        // Then
        tasklets.forEach(MockTasklet::assertNotDone);

        exceptionRule.expect(CancellationException.class);
        future.get();
    }

    @Test
    public void when_blockingSleepingTaskletIsCancelled_then_completeEarly() throws Exception {
        // Given
        final List<MockTasklet> tasklets =
                Stream.generate(() -> new MockTasklet().sleeping().callsBeforeDone(Integer.MAX_VALUE))
                      .limit(100).collect(toList());

        // When
        CompletableFuture<Void> future = es.execute(tasklets, doneCallback, classLoaderMock).toCompletableFuture();
        future.cancel(true);

        // Then
        tasklets.forEach(MockTasklet::assertNotDone);
        exceptionRule.expect(CancellationException.class);
        future.get();
    }

    @Test
    public void when_nonBlockingCancelled_then_doneCallBackFiredAfterActualDone() throws Exception {
        // Given
        CountDownLatch proceedLatch = new CountDownLatch(1);
        final List<MockTasklet> tasklets =
                Stream.generate(() -> new MockTasklet().waitOnLatch(proceedLatch).callsBeforeDone(Integer.MAX_VALUE))
                      .limit(100).collect(toList());

        CompletableFuture<Void> doneFuture = new CompletableFuture<Void>();

        // When
        CompletableFuture<Void> future = es.execute(tasklets, f -> doneFuture.complete(null), classLoaderMock)
                                           .toCompletableFuture();

        future.cancel(true);

        // Then
        assertTrue("future returned from .execute() should be done", future.isDone());
        assertFalse("doneFuture should not be completed until tasklets are completed.", doneFuture.isDone());

        proceedLatch.countDown();

        assertTrueEventually(() -> {
            assertTrue("doneFuture should be completed eventually", doneFuture.isDone());
        });

        exceptionRule.expect(CancellationException.class);
        future.get();
    }

    @Test
    public void when_twoNonBlockingTasklets_then_differentWorker() throws Exception {
        // Given
        TaskletAssertingThreadLocal t1 = new TaskletAssertingThreadLocal();
        TaskletAssertingThreadLocal t2 = new TaskletAssertingThreadLocal();
        assertTrue(t1.isCooperative());

        // When
        CompletableFuture<Void> f1 = es.execute(singletonList(t1), doneCallback, classLoaderMock).toCompletableFuture();
        CompletableFuture<Void> f2 = es.execute(singletonList(t2), doneCallback, classLoaderMock).toCompletableFuture();
        f1.join();
        f2.join();

        // Then
        // -- assertions are inside TaskletAssertingThreadLocal and will fail, if t1 and t2 are running on the same thread
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
        public void init(CompletableFuture<Void> jobFuture) {
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
