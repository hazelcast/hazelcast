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

package com.hazelcast.executor;

import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompletableFutureTest extends HazelcastTestSupport {

    private static final RuntimeException THROW_TEST_EXCEPTION = new RuntimeException("Test exception");
    private static final RuntimeException NO_EXCEPTION = null;

    private ExecutionService executionService;
    private CountDownLatch inExecutionLatch;
    private CountDownLatch startLogicLatch;
    private CountDownLatch executedLogic;
    private CountDownLatch callbacksDoneLatch;
    private AtomicReference<Object> reference1;
    private AtomicReference<Object> reference2;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setUp() {
        NodeEngine nodeEngine = getNode(createHazelcastInstance()).getNodeEngine();
        executionService = nodeEngine.getExecutionService();
        startLogicLatch = new CountDownLatch(1);
        executedLogic = new CountDownLatch(1);
        inExecutionLatch = new CountDownLatch(1);
        reference1 = new AtomicReference<Object>();
        reference2 = new AtomicReference<Object>();
    }

    @Test
    public void preregisterCallback() {
        InternalCompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(1), NO_EXCEPTION);
        f.whenCompleteAsync(storeTaskResponseToReference(reference1));

        releaseAwaitingTask();

        assertCallbacksExecutedEventually();
        assertEquals("success", reference1.get());
    }

    @Test
    public void preregisterTwoCallbacks() {
        InternalCompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(2), NO_EXCEPTION);
        f.whenCompleteAsync(storeTaskResponseToReference(reference1));
        f.whenCompleteAsync(storeTaskResponseToReference(reference2));

        releaseAwaitingTask();

        assertCallbacksExecutedEventually();
        assertEquals("success", reference1.get());
        assertEquals("success", reference2.get());
    }

    @Test
    public void preregisterTwoCallbacks_taskThrowsException() {
        InternalCompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(2), THROW_TEST_EXCEPTION);
        f.whenCompleteAsync(storeTaskResponseToReference(reference1));
        f.whenCompleteAsync(storeTaskResponseToReference(reference2));

        releaseAwaitingTask();

        assertCallbacksExecutedEventually();
        assertTestExceptionThrown(reference1, reference2);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/6020
    public void postregisterCallback() {
        InternalCompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(1), NO_EXCEPTION);
        releaseAwaitingTask();
        assertTaskFinishedEventually(f);

        f.whenCompleteAsync(storeTaskResponseToReference(reference1));

        assertCallbacksExecutedEventually();
        assertEquals("success", reference1.get());
    }

    @Test
    public void postregisterTwoCallbacks() {
        InternalCompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(2), NO_EXCEPTION);
        releaseAwaitingTask();
        assertTaskFinishedEventually(f);

        f.whenCompleteAsync(storeTaskResponseToReference(reference1));
        f.whenCompleteAsync(storeTaskResponseToReference(reference2));

        assertCallbacksExecutedEventually();
        assertEquals("success", reference1.get());
        assertEquals("success", reference2.get());
    }

    @Test
    public void postregisterTwoCallbacks_taskThrowsException() {
        InternalCompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(2), THROW_TEST_EXCEPTION);
        releaseAwaitingTask();
        assertTaskFinishedEventually(f);

        f.whenCompleteAsync(storeTaskResponseToReference(reference1));
        f.whenCompleteAsync(storeTaskResponseToReference(reference2));

        assertCallbacksExecutedEventually();
        assertTestExceptionThrown(reference1, reference2);
    }

    @Test(timeout = 60000)
    public void get_taskThrowsException() throws Exception {
        InternalCompletableFuture<String> f = submitAwaitingTaskNoCallbacks(THROW_TEST_EXCEPTION);
        submitReleasingTask(100);

        expected.expect(ExecutionException.class);
        f.get();
    }

    @Test(timeout = 60000)
    public void getWithTimeout_taskThrowsException() throws Exception {
        InternalCompletableFuture<String> f = submitAwaitingTaskNoCallbacks(THROW_TEST_EXCEPTION);
        submitReleasingTask(200);

        expected.expect(ExecutionException.class);
        f.get(30000, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 60000)
    public void getWithTimeout_finishesWithinTime() throws Exception {
        InternalCompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
        submitReleasingTask(200);
        String result = f.get(30000, TimeUnit.MILLISECONDS);

        assertEquals("success", result);
    }

    @Test(timeout = 60000)
    public void getWithTimeout_timesOut() throws Exception {
        InternalCompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);

        expected.expect(TimeoutException.class);
        f.get(1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void singleCancellation_beforeDone_succeeds() {
        InternalCompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
        assertTaskInExecution();

        boolean cancelResult = f.cancel(false);

        assertTrue("Task cancellation succeeded should succeed", cancelResult);
    }

    @Test
    public void cancellation_afterDone_taskNotCancelled_flagsSetCorrectly() throws Exception {
        final InternalCompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
        assertTaskInExecution();
        releaseAwaitingTask();
        assertTaskExecutedItsLogic();
        assertTaskFinishedEventually(f);

        boolean firstCancelResult = f.cancel(false);
        boolean secondCancelResult = f.cancel(false);

        assertFalse("Cancellation should not succeed after task is done", firstCancelResult);
        assertFalse("Cancellation should not succeed after task is done", secondCancelResult);

        assertFalse("Task should NOT be cancelled", f.isCancelled());
        assertEquals("success", f.get());
    }

    @Test
    public void noCancellation_afterDone_flagsSetCorrectly() throws Exception {
        InternalCompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
        assertTaskInExecution();
        releaseAwaitingTask();
        assertTaskExecutedItsLogic();
        assertTaskFinishedEventually(f);

        assertTrue("Task should be done", f.isDone());
        assertFalse("Task should NOT be cancelled", f.isCancelled());
        assertEquals("success", f.get());
    }

    @Test(timeout = 60000)
    public void cancelAndGet_taskCancelled_withoutInterruption_logicExecuted() throws Exception {
        InternalCompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
        assertTaskInExecution();

        boolean cancelResult = f.cancel(false);
        releaseAwaitingTask();
        assertTaskExecutedItsLogic(); // cancellation came, when task already awaiting, so logic executed
        assertTaskFinishedEventually(f);

        assertTrue("Task cancellation should succeed", cancelResult);
        assertTrue("Task should be done", f.isDone());
        assertTrue("Task should be cancelled", f.isCancelled());
        expected.expect(CancellationException.class);
        f.get();
    }

    @Test(timeout = 60000)
    public void cancelAndGet_taskCancelled_withInterruption_noLogicExecuted() throws Exception {
        InternalCompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
        assertTaskInExecution();

        boolean cancelResult = f.cancel(true);

        assertTaskInterruptedAndDidNotExecuteItsLogic();
        assertTaskFinishedEventually(f); // task did not have to be releases - interruption was enough
        assertTrue("Task cancellation should succeed", cancelResult);
        assertTrue("Task should be done", f.isDone());
        assertTrue("Task should be cancelled", f.isCancelled());
        expected.expect(CancellationException.class);
        f.get();
    }


    private static void assertTestExceptionThrown(AtomicReference<?>... refs) {
        for (AtomicReference<?> ref : refs) {
            assertThat("TEST_EXCEPTION expected", ref.get(),
                    Matchers.sameInstance(THROW_TEST_EXCEPTION));
        }
    }

    private InternalCompletableFuture<String> submitAwaitingTaskNoCallbacks(final Exception exception) {
        return submitAwaitingTask(0, exception);
    }

    private InternalCompletableFuture<String> submitAwaitingTask(Integer numberOfCallbacks, final Exception exception) {
        callbacksDoneLatch = new CountDownLatch(numberOfCallbacks);
        return submit(() -> {
            inExecutionLatch.countDown();
            assertOpenEventually(startLogicLatch);
            executedLogic.countDown();
            if (exception != null) {
                throw exception;
            }
            return "success";
        });
    }

    private void submitReleasingTask(final long millisToAwaitBeforeRelease) {
        submit(() -> {
            sleepAtLeastMillis(millisToAwaitBeforeRelease);
            releaseAwaitingTask();
        });
    }

    private InternalCompletableFuture<String> submit(final Callable<String> callable) {
        return executionService.asCompletableFuture(executionService.submit("default", callable));
    }

    private void submit(final Runnable runnable) {
        executionService.submit("default", runnable);
    }

    private Integer expectedNumberOfCallbacks(int number) {
        return number;
    }

    private void releaseAwaitingTask() {
        startLogicLatch.countDown();
    }

    private void assertCallbacksExecutedEventually() {
        assertOpenEventually(callbacksDoneLatch);
    }

    private void assertTaskExecutedItsLogic() {
        assertOpenEventually(executedLogic);
    }

    private void assertTaskInterruptedAndDidNotExecuteItsLogic() {
        assertEquals(1, executedLogic.getCount());
    }

    private void assertTaskFinishedEventually(final CompletableFuture future) {
        assertTrueEventually(() -> assertTrue(future.isDone()));
    }

    private void assertTaskInExecution() {
        assertOpenEventually(inExecutionLatch);
    }

    private BiConsumer<String, Throwable> storeTaskResponseToReference(final AtomicReference<Object> ref) {
        return new BiConsumer<String, Throwable>() {
            @Override
            public void accept(String s, Throwable throwable) {
                if (throwable == null) {
                    doit(s);
                } else {
                    doit(throwable);
                }
            }

            private void doit(Object response) {
                ref.set(response);
                callbacksDoneLatch.countDown();
            }
        };
    }
}
