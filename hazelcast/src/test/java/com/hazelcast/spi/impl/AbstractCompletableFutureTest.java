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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractCompletableFutureTest extends HazelcastTestSupport {

    private static final Object RESULT = "foobar";

    private static final String EXCEPTION_MESSAGE = "You screwed buddy!";
    private static final Exception EXCEPTION = new RuntimeException(EXCEPTION_MESSAGE);

    private Executor executor = CALLER_RUNS;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void future_notExecuted_notDoneNotCancelled() {
        TestFutureImpl future = new TestFutureImpl();

        assertFalse("New future should not be done", future.isDone());
        assertFalse("New future should not be cancelled", future.isCancelled());
    }

    @Test
    public void future_notExecuted_callbackRegistered_notDoneNotCancelled() {
        TestFutureImpl future = new TestFutureImpl();
        future.whenCompleteAsync(mock(BiConsumer.class));

        assertFalse("New future should not be done", future.isDone());
        assertFalse("New future should not be cancelled", future.isCancelled());
    }

    @Test
    public void future_ordinaryResultSet_doneNotCancelled() {
        future_resultSet_doneNotCancelled(RESULT);
    }

    @Test
    public void future_nullResultSet_doneNotCancelled() {
        future_resultSet_doneNotCancelled(null);
    }

    @Test
    public void future_exceptionResultSet_doneNotCancelled() {
        future_resultSet_doneNotCancelled(EXCEPTION);
    }

    private void future_resultSet_doneNotCancelled(Object result) {
        TestFutureImpl future = new TestFutureImpl();
        future.complete(result);

        assertTrue("Future with result should be done", future.isDone());
        assertFalse("Done future should not be cancelled", future.isCancelled());
    }

    @Test
    public void future_resultNotSet_cancelled() {
        TestFutureImpl future = new TestFutureImpl();
        boolean cancelled = future.cancel(false);

        assertTrue(cancelled);
        assertTrue("Cancelled future should be done", future.isDone());
        assertTrue("Cancelled future should be cancelled", future.isCancelled());
    }

    @Test
    public void future_resultSetAndCancelled() {
        TestFutureImpl future = new TestFutureImpl();
        future.complete(RESULT);
        boolean cancelled = future.cancel(false);

        assertFalse(cancelled);
        assertTrue("Done future should be done", future.isDone());
        assertFalse("Done future should not be cancelled even if cancelled executed", future.isCancelled());
    }

    @Test
    public void future_cancelledAndResultSet() {
        TestFutureImpl future = new TestFutureImpl();
        boolean cancelled = future.cancel(false);
        future.complete(RESULT);

        assertTrue(cancelled);
        assertTrue("Cancelled future should be done", future.isDone());
        assertTrue("Cancelled future should be cancelled", future.isCancelled());
    }

    @Test(expected = CancellationException.class)
    public void get_cancelledFuture_exceptionThrown() throws Exception {
        TestFutureImpl future = new TestFutureImpl();

        future.cancel(false);

        future.get();
    }

    @Test(expected = CancellationException.class)
    public void getWithTimeout_cancelledFuture_exceptionThrown() throws Exception {
        TestFutureImpl future = new TestFutureImpl();

        future.cancel(false);

        future.get(10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void get_ordinaryResultSet_returnsResult() throws Exception {
        TestFutureImpl future = new TestFutureImpl();
        future.complete(RESULT);

        Object result = future.get();

        assertSame(RESULT, result);
    }

    @Test
    public void getWithTimeout_ordinaryResultSet_returnsResult() throws Exception {
        TestFutureImpl future = new TestFutureImpl();
        future.complete(RESULT);

        Object result = future.get(10, TimeUnit.MILLISECONDS);

        assertSame(RESULT, result);
    }

    @Test
    public void get_exceptionResultSet_exceptionThrown() throws Exception {
        TestFutureImpl future = new TestFutureImpl();
        future.completeExceptionally(EXCEPTION);

        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(EXCEPTION.getClass(), EXCEPTION_MESSAGE));
        future.get();
    }

    @Test
    public void getWithTimeout_exceptionResultSet_exceptionThrown_noTimeout() throws Exception {
        TestFutureImpl future = new TestFutureImpl();
        future.completeExceptionally(EXCEPTION);

        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(EXCEPTION.getClass(), EXCEPTION_MESSAGE));
        future.get(1, TimeUnit.NANOSECONDS);
    }

    @Test
    public void get_nullResultSet_returnsResult() throws Exception {
        TestFutureImpl future = new TestFutureImpl();
        future.complete(null);

        Object result = future.get();

        assertNull(result);
    }

    @Test
    public void getWithTimeout_nullResultSet_returnsResult() throws Exception {
        TestFutureImpl future = new TestFutureImpl();
        future.complete(null);

        Object result = future.get(10, TimeUnit.MILLISECONDS);

        assertNull(result);
    }

    @Test(expected = TimeoutException.class, timeout = 120000)
    public void getWithTimeout_resultNotSet_timesOut() throws Exception {
        TestFutureImpl future = new TestFutureImpl();

        future.get(10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = TimeoutException.class, timeout = 60000)
    public void getWithTimeout_zeroTimeout_resultNotSet_timesOut() throws Exception {
        TestFutureImpl future = new TestFutureImpl();

        future.get(0, TimeUnit.MILLISECONDS);
    }

    @Test(expected = TimeoutException.class, timeout = 60000)
    public void getWithTimeout_negativeTimeout_resultNotSet_timesOut() throws Exception {
        TestFutureImpl future = new TestFutureImpl();

        future.get(-1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = TimeoutException.class, timeout = 60000)
    public void getWithTimeout_lowerThanOneMilliTimeout_resultNotSet_timesOut() throws Exception {
        TestFutureImpl future = new TestFutureImpl();

        future.get(1, TimeUnit.NANOSECONDS);
    }

    @Test
    public void getWithTimeout_threadInterrupted_exceptionThrown() throws Exception {
        TestFutureImpl future = new TestFutureImpl();

        Thread.currentThread().interrupt();

        expected.expect(InterruptedException.class);
        future.get(100, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 60000)
    public void getWithTimeout_waited_notifiedOnSet() throws Exception {
        TestFutureImpl future = new TestFutureImpl();

        submitSetResultAfterTimeInMillis(future, RESULT, 200);
        Object result = future.get(30000, TimeUnit.MILLISECONDS);

        assertEquals(RESULT, result);
    }

    @Test(timeout = 60000)
    public void getWithTimeout_waited_waited_notifiedOnCancel() throws Exception {
        TestFutureImpl future = new TestFutureImpl();

        submitCancelAfterTimeInMillis(future, 200);

        expected.expect(CancellationException.class);
        future.get(30000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void setResult_ordinaryResultSet_futureDone() {
        setResult_resultSet_futureDone(RESULT);
    }

    @Test
    public void setResult_exceptionResultSet_futureDone() {
        setResult_resultSet_futureDone(EXCEPTION);
    }

    @Test
    public void setResult_nullResultSet_futureDone() {
        setResult_resultSet_futureDone(null);
    }

    private void setResult_resultSet_futureDone(Object result) {
        TestFutureImpl future = new TestFutureImpl();

        future.complete(result);

        assertTrue("Future should be done after result has been set", future.isDone());
    }

    @Test
    public void setResult_whenResultAlreadySet_secondResultDiscarded() throws Exception {
        TestFutureImpl future = new TestFutureImpl();
        Object initialResult = "firstresult";
        Object secondResult = "secondresult";

        future.complete(initialResult);
        future.complete(secondResult);

        assertSame(initialResult, future.get());
    }

    @Test
    public void setResult_whenPendingCallback_nullResult() {
        setResult_whenPendingCallback_callbacksExecutedCorrectly(null);
    }

    @Test
    public void setResult_whenPendingCallback_ordinaryResult() {
        setResult_whenPendingCallback_callbacksExecutedCorrectly("foo");
    }

    @Test
    public void setResult_whenPendingCallback_exceptionResult() {
        setResult_whenPendingCallback_callbacksExecutedCorrectly(new Exception());
    }

    public void setResult_whenPendingCallback_callbacksExecutedCorrectly(final Object result) {
        TestFutureImpl future = new TestFutureImpl();
        final BiConsumer callback1 = mock(BiConsumer.class);
        final BiConsumer callback2 = mock(BiConsumer.class);
        future.whenCompleteAsync(callback1);
        future.whenCompleteAsync(callback2);

        if (result instanceof Throwable) {
            future.completeExceptionally((Throwable) result);
        } else {
            future.complete(result);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                if (result instanceof Throwable) {
                    verify(callback1).accept(null, result);
                } else {
                    verify(callback1).accept(result, null);
                }
            }
        });
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                if (result instanceof Throwable) {
                    verify(callback2).accept(null, result);
                } else {
                    verify(callback2).accept(result, null);
                }
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void andThen_whenNullCallback_exceptionThrown() {
        TestFutureImpl future = new TestFutureImpl();
        future.whenCompleteAsync(null, executor);
    }

    @Test(expected = NullPointerException.class)
    public void andThen_whenNullExecutor_exceptionThrown() {
        TestFutureImpl future = new TestFutureImpl();
        future.whenCompleteAsync(mock(BiConsumer.class), null);
    }

    @Test
    public void andThen_whenInitialState() {
        TestFutureImpl future = new TestFutureImpl();
        BiConsumer callback = mock(BiConsumer.class);

        future.whenCompleteAsync(callback, executor);

        verifyZeroInteractions(callback);
    }

    @Test
    public void andThen_whenCancelled() {
        TestFutureImpl future = new TestFutureImpl();
        BiConsumer callback = mock(BiConsumer.class);

        future.cancel(false);
        future.whenCompleteAsync(callback, executor);

        verify(callback).accept(isNull(), isA(CancellationException.class));
    }

    @Test
    public void andThen_whenPendingCallback() {
        TestFutureImpl future = new TestFutureImpl();
        BiConsumer callback1 = mock(BiConsumer.class);
        BiConsumer callback2 = mock(BiConsumer.class);

        future.whenCompleteAsync(callback1, executor);
        future.whenCompleteAsync(callback2, executor);

        verifyZeroInteractions(callback1);
        verifyZeroInteractions(callback2);
    }

    @Test
    public void andThen_whenPendingCallback_andCancelled() {
        TestFutureImpl future = new TestFutureImpl();
        BiConsumer callback1 = mock(BiConsumer.class);
        BiConsumer callback2 = mock(BiConsumer.class);

        future.whenCompleteAsync(callback1, executor);
        future.cancel(false);
        future.whenCompleteAsync(callback2, executor);

        verify(callback1).accept(isNull(), isA(CancellationException.class));
        verify(callback2).accept(isNull(), isA(CancellationException.class));
    }

    @Test
    public void andThen_whenResultAvailable() throws Exception {
        TestFutureImpl future = new TestFutureImpl();
        final Object result = "result";
        final BiConsumer callback = mock(BiConsumer.class);

        future.complete(result);
        future.whenCompleteAsync(callback, executor);

        assertSame(result, future.get());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                verify(callback).accept(result, null);
            }
        });
    }

    private void submitCancelAfterTimeInMillis(final TestFutureImpl future, final int timeInMillis) {
        submit(new Runnable() {
            @Override
            public void run() {
                try {
                    sleepMillis(timeInMillis);
                } finally {
                    future.cancel(false);
                }
            }
        });
    }

    private void submitSetResultAfterTimeInMillis(final TestFutureImpl future, final Object result, final int timeInMillis) {
        submit(new Runnable() {
            @Override
            public void run() {
                try {
                    sleepMillis(timeInMillis);
                } finally {
                    future.complete(result);
                }
            }
        });
    }

    private void submit(Runnable runnable) {
        new Thread(runnable).start();
    }

    private class TestFutureImpl extends InternalCompletableFuture<Object> {
    }
}
