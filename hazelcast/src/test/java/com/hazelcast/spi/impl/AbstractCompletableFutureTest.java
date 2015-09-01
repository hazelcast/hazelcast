package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractCompletableFutureTest extends HazelcastTestSupport {

    private final static Object RESULT = "foobar";

    private final static String EXCEPTION_MESSAGE = "You screwed buddy!";
    private final static Exception EXCEPTION = new RuntimeException(EXCEPTION_MESSAGE);

    private HazelcastInstance hz;
    private ILogger logger;
    private NodeEngineImpl nodeEngine;
    private Executor executor;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        nodeEngine = getNodeEngineImpl(hz);
        logger = Logger.getLogger(AbstractCompletableFutureTest.class);
        executor = Executors.newFixedThreadPool(1);
    }

    @Test
    public void future_notExecuted_notDoneNotCancelled() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        assertFalse("New future should not be done", future.isDone());
        assertFalse("New future should not be cancelled", future.isCancelled());
    }

    @Test
    public void future_notExecuted_callbackRegistered_notDoneNotCancelled() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.andThen(mock(ExecutionCallback.class));

        assertFalse("New future should not be done", future.isDone());
        assertFalse("New future should not be cancelled", future.isCancelled());
    }

    @Test
    public void future_ordinaryResultSet_doneNotCancelled() throws Exception {
        future_resultSet_doneNotCancelled(RESULT);
    }

    @Test
    public void future_nullResultSet_doneNotCancelled() throws Exception {
        future_resultSet_doneNotCancelled(null);
    }

    @Test
    public void future_exceptionResultSet_doneNotCancelled() throws Exception {
        future_resultSet_doneNotCancelled(EXCEPTION);
    }

    private void future_resultSet_doneNotCancelled(Object result) {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(result);

        assertTrue("Future with result should be done", future.isDone());
        assertFalse("Done future should not be cancelled", future.isCancelled());
    }

    @Test
    public void future_resultNotSet_cancelled() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        boolean cancelled = future.cancel(false);

        assertTrue(cancelled);
        assertTrue("Cancelled future should be done", future.isDone());
        assertTrue("Cancelled future should be cancelled", future.isCancelled());
    }

    @Test
    public void future_resultSetAndCancelled() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(RESULT);
        boolean cancelled = future.cancel(false);

        assertFalse(cancelled);
        assertTrue("Done future should be done", future.isDone());
        assertFalse("Done future should not be cancelled even if cancelled executed", future.isCancelled());
    }

    @Test
    public void future_cancelledAndResultSet() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        boolean cancelled = future.cancel(false);
        future.setResult(RESULT);

        assertTrue(cancelled);
        assertTrue("Cancelled future should be done", future.isDone());
        assertTrue("Cancelled future should be cancelled", future.isCancelled());
        assertNull("Internal result should be null", future.getResult());
    }

    @Test(expected = CancellationException.class)
    public void get_cancelledFuture_exceptionThrown() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        future.cancel(false);

        future.get();
    }

    @Test(expected = CancellationException.class)
    public void getWithTimeout_cancelledFuture_exceptionThrown() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        future.cancel(false);

        future.get(10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void get_ordinaryResultSet_returnsResult() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(RESULT);

        Object result = future.get();

        assertSame(RESULT, result);
    }

    @Test
    public void getWithTimeout_ordinaryResultSet_returnsResult() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(RESULT);

        Object result = future.get(10, TimeUnit.MILLISECONDS);

        assertSame(RESULT, result);
    }

    @Test
    public void get_exceptionResultSet_exceptionThrown() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(EXCEPTION);

        expected.expect(EXCEPTION.getClass());
        expected.expectMessage(EXCEPTION_MESSAGE);

        future.get();
    }

    @Test
    public void getWithTimeout_exceptionResultSet_exceptionThrown_noTimeout() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(EXCEPTION);

        expected.expect(EXCEPTION.getClass());
        expected.expectMessage(EXCEPTION_MESSAGE);

        future.get(1, TimeUnit.NANOSECONDS);
    }

    @Test
    public void get_nullResultSet_returnsResult() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(null);

        Object result = future.get();

        assertNull(result);
    }

    @Test
    public void getWithTimeout_nullResultSet_returnsResult() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(null);

        Object result = future.get(10, TimeUnit.MILLISECONDS);

        assertNull(result);
    }

    @Test(expected = TimeoutException.class, timeout = 120000)
    public void getWithTimeout_resultNotSet_timesOut() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        future.get(10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = TimeoutException.class, timeout = 60000)
    public void getWithTimeout_zeroTimeout_resultNotSet_timesOut() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        future.get(0, TimeUnit.MILLISECONDS);
    }

    @Test(expected = TimeoutException.class, timeout = 60000)
    public void getWithTimeout_negativeTimeout_resultNotSet_timesOut() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        future.get(-1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = TimeoutException.class, timeout = 60000)
    public void getWithTimeout_lowerThanOneMilliTimeout_resultNotSet_timesOut() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        future.get(1, TimeUnit.NANOSECONDS);
    }

    @Test
    public void getWithTimeout_threadInterrupted_exceptionThrown() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        Thread.currentThread().interrupt();

        expected.expect(InterruptedException.class);
        future.get(100, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 60000)
    public void getWithTimeout_waited_notifiedOnSet() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        submitSetResultAfterTimeInMillis(future, RESULT, 200);
        Object result = future.get(30000, TimeUnit.MILLISECONDS);

        assertEquals(RESULT, result);
    }

    @Test(timeout = 60000)
    public void getWithTimeout_waited_waited_notifiedOnCancel() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        submitCancelAfterTimeInMillis(future, 200);

        expected.expect(CancellationException.class);
        future.get(30000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void setResult_ordinaryResultSet_futureDone() throws Exception {
        setResult_resultSet_futureDone(RESULT);
    }

    @Test
    public void setResult_exceptionResultSet_futureDone() throws Exception {
        setResult_resultSet_futureDone(EXCEPTION);
    }

    @Test
    public void setResult_nullResultSet_futureDone() throws Exception {
        setResult_resultSet_futureDone(null);
    }

    private void setResult_resultSet_futureDone(Object result) {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        future.setResult(result);

        assertTrue("Future should be done after result has been set", future.isDone());
    }

    @Test
    public void setResult_whenResultAlreadySet_secondResultDiscarded() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        Object initialResult = "firstresult", secondResult = "secondresult";

        future.setResult(initialResult);
        future.setResult(secondResult);

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
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        final ExecutionCallback callback1 = mock(ExecutionCallback.class);
        final ExecutionCallback callback2 = mock(ExecutionCallback.class);
        future.andThen(callback1);
        future.andThen(callback2);

        future.setResult(result);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                if (result instanceof Throwable) {
                    verify(callback1).onFailure((Throwable) result);
                } else {
                    verify(callback1).onResponse(result);
                }
            }
        });
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                if (result instanceof Throwable) {
                    verify(callback2).onFailure((Throwable) result);
                } else {
                    verify(callback2).onResponse(result);
                }
            }
        });
    }

    @Test
    public void getResult_whenInitialState() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);

        Object result = future.getResult();

        assertNull("Internal result should be null initially", result);
    }

    @Test
    public void getResult_whenPendingCallback() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.andThen(mock(ExecutionCallback.class));

        Object result = future.getResult();

        assertNull("Internal result should be null initially", result);
    }

    @Test
    public void getResult_whenNullResult() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.setResult(null);

        Object result = future.getResult();

        assertNull("Internal result should be null when set to null", result);
    }


    @Test(expected = IllegalArgumentException.class)
    public void andThen_whenNullCallback_exceptionThrown() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.andThen(null, executor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void andThen_whenNullExecutor_exceptionThrown() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        future.andThen(mock(ExecutionCallback.class), null);
    }

    @Test
    public void andThen_whenInitialState() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        ExecutionCallback callback = mock(ExecutionCallback.class);

        future.andThen(callback, executor);

        verifyZeroInteractions(callback);
    }

    @Test
    public void andThen_whenCancelled() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        ExecutionCallback callback = mock(ExecutionCallback.class);

        future.cancel(false);
        future.andThen(callback, executor);

        verifyZeroInteractions(callback);
    }

    @Test
    public void andThen_whenPendingCallback() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        ExecutionCallback callback1 = mock(ExecutionCallback.class);
        ExecutionCallback callback2 = mock(ExecutionCallback.class);

        future.andThen(callback1, executor);
        future.andThen(callback2, executor);

        verifyZeroInteractions(callback1);
        verifyZeroInteractions(callback2);
    }

    @Test
    public void andThen_whenPendingCallback_andCancelled() {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        ExecutionCallback callback1 = mock(ExecutionCallback.class);
        ExecutionCallback callback2 = mock(ExecutionCallback.class);

        future.andThen(callback1, executor);
        future.cancel(false);
        future.andThen(callback2, executor);

        verifyZeroInteractions(callback1);
        verifyZeroInteractions(callback2);
    }

    @Test
    public void andThen_whenResultAvailable() throws Exception {
        FutureImpl future = new FutureImpl(nodeEngine, logger);
        final Object result = "result";
        final ExecutionCallback callback = mock(ExecutionCallback.class);

        future.setResult(result);
        future.andThen(callback, executor);

        assertSame(result, future.get());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback).onResponse(result);
            }
        });
    }

    private void submitCancelAfterTimeInMillis(final FutureImpl future, final int timeInMillis) {
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

    private void submitSetResultAfterTimeInMillis(final FutureImpl future, final Object result, final int timeInMillis) {
        submit(new Runnable() {
            @Override
            public void run() {
                try {
                    sleepMillis(timeInMillis);
                } finally {
                    future.setResult(result);
                }
            }
        });
    }

    private void submit(Runnable runnable) {
        new Thread(runnable).start();
    }

    private class FutureImpl extends AbstractCompletableFuture<Object> {
        protected FutureImpl(NodeEngine nodeEngine, ILogger logger) {
            super(nodeEngine, logger);
        }
    }
}
