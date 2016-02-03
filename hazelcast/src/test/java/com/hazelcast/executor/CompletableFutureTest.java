/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CompletableFutureTest extends HazelcastTestSupport {

    private static final RuntimeException THROW_TEST_EXCEPTION = new RuntimeException("Test exception");
    private static final RuntimeException NO_EXCEPTION = null;

    private ExecutionService executionService;
    private CountDownLatch inExecutionLatch, startLogicLatch, executedLogic, callbacksDoneLatch;
    private AtomicReference<Object> reference1, reference2;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        NodeEngine nodeEngine = getNode(createHazelcastInstance()).getNodeEngine();
        executionService = nodeEngine.getExecutionService();
        startLogicLatch = new CountDownLatch(1);
        executedLogic = new CountDownLatch(1);
        inExecutionLatch = new CountDownLatch(1);
        reference1 = new AtomicReference<Object>();
        reference2 = new AtomicReference<Object>();
    }

    @Test
    public void preregisterCallback() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(1), NO_EXCEPTION);
        f.andThen(storeTaskResponseToReference(reference1));

        releaseAwaitingTask();

        assertCallbacksExecutedEventually();
        assertEquals("success", reference1.get());
    }

    @Test
    public void preregisterTwoCallbacks() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(2), NO_EXCEPTION);
        f.andThen(storeTaskResponseToReference(reference1));
        f.andThen(storeTaskResponseToReference(reference2));

        releaseAwaitingTask();

        assertCallbacksExecutedEventually();
        assertEquals("success", reference1.get());
        assertEquals("success", reference2.get());
    }

    @Test
    public void preregisterTwoCallbacks_taskThrowsException() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(2), THROW_TEST_EXCEPTION);
        f.andThen(storeTaskResponseToReference(reference1));
        f.andThen(storeTaskResponseToReference(reference2));

        releaseAwaitingTask();

        assertCallbacksExecutedEventually();
        assertTestExceptionThrown(reference1, reference2);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/6020
    public void postregisterCallback() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(1), NO_EXCEPTION);
        releaseAwaitingTask();
        assertTaskFinishedEventually(f);

        f.andThen(storeTaskResponseToReference(reference1));

        assertCallbacksExecutedEventually();
        assertEquals("success", reference1.get());
    }

    @Test
    public void postregisterTwoCallbacks() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(2), NO_EXCEPTION);
        releaseAwaitingTask();
        assertTaskFinishedEventually(f);

        f.andThen(storeTaskResponseToReference(reference1));
        f.andThen(storeTaskResponseToReference(reference2));

        assertCallbacksExecutedEventually();
        assertEquals("success", reference1.get());
        assertEquals("success", reference2.get());
    }

    @Test
    public void postregisterTwoCallbacks_taskThrowsException() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTask(expectedNumberOfCallbacks(2), THROW_TEST_EXCEPTION);
        releaseAwaitingTask();
        assertTaskFinishedEventually(f);

        f.andThen(storeTaskResponseToReference(reference1));
        f.andThen(storeTaskResponseToReference(reference2));

        assertCallbacksExecutedEventually();
        assertTestExceptionThrown(reference1, reference2);
    }

    @Test(timeout = 60000)
    public void get_taskThrowsException() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(THROW_TEST_EXCEPTION);
        submitReleasingTask(100);

        expected.expect(ExecutionException.class);
        f.get();
    }

    @Test(timeout = 60000)
    public void getWithTimeout_taskThrowsException() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(THROW_TEST_EXCEPTION);
        submitReleasingTask(200);

        expected.expect(ExecutionException.class);
        f.get(30000, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 60000)
    public void getWithTimeout_finishesWithinTime() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
        submitReleasingTask(200);
        String result = f.get(30000, TimeUnit.MILLISECONDS);

        assertEquals("success", result);
    }

    @Test(timeout = 60000)
    public void getWithTimeout_timesOut() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);

        expected.expect(TimeoutException.class);
        f.get(1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void singleCancellation_beforeDone_succeeds() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
        assertTaskInExecution();

        boolean cancelResult = f.cancel(false);

        assertTrue("Task cancellation succeeded should succeed", cancelResult);
    }

    @Test
    public void doubleCancellation_beforeDone_firstSucceeds_secondFails() throws Exception {
        ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
        assertTaskInExecution(); // but never released to execute logic

        boolean firstCancelResult = f.cancel(false);
        boolean secondCancelResult = f.cancel(false);

        assertTrue("First task cancellation should succeed", firstCancelResult);
        assertFalse("Second task cancellation should failed", secondCancelResult);
    }

    @Test
    public void cancellation_afterDone_taskNotCancelled_flagsSetCorrectly() throws Exception {
        final ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
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
        ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
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
        ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
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
        ICompletableFuture<String> f = submitAwaitingTaskNoCallbacks(NO_EXCEPTION);
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
        for (AtomicReference<?> ref : refs)
            assertThat("ExecutionException expected", ref.get(), instanceOf(ExecutionException.class));
        for (AtomicReference<?> ref : refs)
            assertThat("TEST_EXCEPTION expected as cause", ((Throwable) ref.get()).getCause(),
                    Matchers.<Throwable>sameInstance(THROW_TEST_EXCEPTION));
    }

    private ICompletableFuture<String> submitAwaitingTaskNoCallbacks(final Exception exception) {
        return submitAwaitingTask(0, exception);
    }

    private ICompletableFuture<String> submitAwaitingTask(Integer numberOfCallbacks, final Exception exception) {
        callbacksDoneLatch = new CountDownLatch(numberOfCallbacks);
        return submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                inExecutionLatch.countDown();
                assertOpenEventually(startLogicLatch);
                executedLogic.countDown();
                if (exception != null) {
                    throw exception;
                }
                return "success";
            }
        });
    }

    private void submitReleasingTask(final long millisToAwaitBeforeRelease) {
        submit(new Runnable() {
            @Override
            public void run() {
                sleepAtLeastMillis(millisToAwaitBeforeRelease);
                releaseAwaitingTask();
            }
        });
    }

    private ICompletableFuture<String> submit(final Callable<String> callable) {
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

    private void assertTaskFinishedEventually(final ICompletableFuture future) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(future.isDone());
            }
        });
    }

    private void assertTaskInExecution() {
        assertOpenEventually(inExecutionLatch);
    }

    private ExecutionCallback<String> storeTaskResponseToReference(final AtomicReference<Object> ref) {
        return new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
                doit(response);
            }

            @Override
            public void onFailure(Throwable t) {
                doit(t);
            }

            private void doit(Object response) {
                ref.set(response);
                callbacksDoneLatch.countDown();
            }
        };
    }
}
