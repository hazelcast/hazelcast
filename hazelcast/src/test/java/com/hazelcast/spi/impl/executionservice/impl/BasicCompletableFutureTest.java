package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ManagedExecutorService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
public class BasicCompletableFutureTest {

    private static final String DELEGATE_RESULT = "DELEGATE_RESULT";
    private static final String OUTER_RESULT = "OUTER_RESULT";

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private FutureTask<String> delegateFuture;
    private boolean delegateThrowException;
    private BasicCompletableFuture<String> outerFuture;

    @Before
    public void given() {
        delegateThrowException = false;
        delegateFuture = future(DELEGATE_RESULT);
        outerFuture = basicCompletableFuture(delegateFuture);
    }

    @Test
    public void cancel_delegate_bothCancelled() {
        delegateFuture.cancel(false);

        assertTrue(delegateFuture.isCancelled());
        assertTrue(outerFuture.isCancelled());
    }

    @Test
    public void cancel_delegate_getOnDelegate() throws Exception {
        delegateFuture.cancel(false);

        expected.expect(CancellationException.class);
        delegateFuture.get();
    }

    @Test
    public void cancel_delegate_getOnOuter() throws Exception {
        delegateFuture.cancel(false);

        expected.expect(CancellationException.class);
        outerFuture.get();
    }

    @Test
    public void cancel_outer_bothCancelled() {
        outerFuture.cancel(false);

        assertTrue(delegateFuture.isCancelled());
        assertTrue(outerFuture.isCancelled());
    }

    @Test
    public void cancel_outer_getOnDelegate() throws Exception {
        outerFuture.cancel(false);

        expected.expect(CancellationException.class);
        delegateFuture.get();
    }

    @Test
    public void cancel_outer_getOnOuter() throws Exception {
        outerFuture.cancel(false);

        expected.expect(CancellationException.class);
        outerFuture.get();
    }

    @Test
    public void completeOuter_outerDoneDelegateNot_delegateAskedFirst() {
        outerFuture.setResult(OUTER_RESULT);

        assertFalse(delegateFuture.isDone());
        assertTrue(outerFuture.isDone());
    }

    @Test
    public void completeOuter_outerDoneDelegateNot_outerAskedFirst() {
        outerFuture.setResult(OUTER_RESULT);

        assertTrue(outerFuture.isDone());
        assertFalse(delegateFuture.isDone());
    }

    @Test
    public void completeDelegate_bothDone_delegateAskedFirst() {
        delegateFuture.run();

        assertTrue(delegateFuture.isDone());
        assertTrue(outerFuture.isDone());
    }

    @Test
    public void completeDelegate_bothDone_outerAskedFirst() {
        delegateFuture.run();

        assertTrue(outerFuture.isDone());
        assertTrue(delegateFuture.isDone());
    }

    @Test
    // PROBLEM NR.1
    // If you complete the BasicCompletableFuture through setResult it will return isDone() == true,
    // but get() may hang forever implying that the delegate future (the future that's enclosed in BCF) will not complete.
    // Infinite waiting path:
    // <pre>
    //     BasicCompletableFuture bcf = new ...();
    //     bcf.setResult("result"); (will "complete" the BCF but not the delegate future)
    //     if(bcf.isDone()) { // will return true
    //         bcf.get() // hangs forever on the delegate future get() assuming it never completes.
    //     }
    //</pre>
    public void completeOuter_getWithTimeout_delegateAsked() throws Exception {
        outerFuture.setResult(OUTER_RESULT);

        assertTrue(outerFuture.isDone());
        expected.expect(TimeoutException.class);
        delegateFuture.get(100, TimeUnit.MILLISECONDS);
    }

    @Test
    // PROBLEM NR.1
    public void completeOuter_getWithTimeout_outerAsked() throws Exception {
        outerFuture.setResult(OUTER_RESULT);

        assertTrue(outerFuture.isDone());
        expected.expect(TimeoutException.class);
        outerFuture.get(10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void completeDelegate_getWithTimeout_delegateAsked() throws Exception {
        delegateFuture.run();

        assertEquals(DELEGATE_RESULT, delegateFuture.get(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void completeDelegate_getWithTimeout_outerAsked() throws Exception {
        delegateFuture.run();

        assertEquals(DELEGATE_RESULT, outerFuture.get(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void completeDelegate_successfully_callbacksNeverRun() throws Exception {
        ExecutionCallback callback = mock(ExecutionCallback.class);

        delegateFuture.run();
        outerFuture.andThen(callback);

        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_withException_callbacksNeverRun() throws Exception {
        ExecutionCallback callback = mock(ExecutionCallback.class);
        delegateThrowException = true;

        delegateFuture.run();
        outerFuture.andThen(callback);

        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_successfully_callbackAfterGet_invokeIsDoneOnOuter_callbacksRun() throws Exception {
        ExecutionCallback callback = mock(ExecutionCallback.class);

        delegateFuture.run();
        outerFuture.isDone();
        outerFuture.andThen(callback);

        verify(callback, times(1)).onResponse(any(Throwable.class));
        verify(callback, times(0)).onFailure(any(Throwable.class));
    }

    @Test
    public void completeDelegate_successfully_callbackAfterGet_invokeGetOnOuter_callbacksRun() throws Exception {
        ExecutionCallback callback = mock(ExecutionCallback.class);

        delegateFuture.run();
        outerFuture.get();
        outerFuture.andThen(callback);

        verify(callback, times(1)).onResponse(any(Throwable.class));
        verify(callback, times(0)).onFailure(any(Throwable.class));
    }

    @Test
    public void completeDelegate_successfully_callbackBeforeGet_invokeIsDoneOnOuter_callbacksRun() throws Exception {
        ExecutionCallback callback = mock(ExecutionCallback.class);

        delegateFuture.run();
        outerFuture.andThen(callback);
        outerFuture.isDone();

        verify(callback, times(1)).onResponse(any(Throwable.class));
        verify(callback, times(0)).onFailure(any(Throwable.class));
    }

    @Test
    // PROBLEM NR.2 (this behavior is correct, but it takes part in the problematic scenario to show inconsistency)
    public void completeDelegate_successfully_callbackBeforeGet_invokeGetOnOuter_callbacksRun() throws Exception {
        ExecutionCallback callback = mock(ExecutionCallback.class);

        delegateFuture.run();
        outerFuture.andThen(callback);
        outerFuture.get();

        verify(callback, times(1)).onResponse(any(Throwable.class));
        verify(callback, times(0)).onFailure(any(Throwable.class));
    }

    @Test
    // PROBLEM NR.2 (this behavior is correct, but it takes part in the problematic scenario to show inconsistency)
    public void completeDelegate_withException_callbackBeforeGet_invokeIsDoneOnOuter_callbacksRun() throws Exception {
        ExecutionCallback callback = mock(ExecutionCallback.class);
        delegateThrowException = true;

        delegateFuture.run();
        outerFuture.andThen(callback);
        outerFuture.isDone();

        verify(callback, times(0)).onResponse(any(Throwable.class));
        verify(callback, times(1)).onFailure(any(Throwable.class));
    }

    @Test
    // PROBLEM NR.2 -> root cause of this problem -> callbacks are never run
    // which is not consistent with the other cases marked with PROBLEM NR.2
    public void completeDelegate_withException_callbackBeforeGet_invokeGetOnOuter_callbacksNeverReached() throws Exception {
        ExecutionCallback callback = mock(ExecutionCallback.class);
        delegateThrowException = true;

        delegateFuture.run();
        outerFuture.andThen(callback);

        try {
            outerFuture.get();
            fail();
        } catch(Throwable t) {
            assertEquals("Exception in execution", t.getCause().getMessage());
        }

        // potential callbacks never executed as opposed to test above
        verifyZeroInteractions(callback);
    }

    private <V> FutureTask<V> future(final V result) {
        return new FutureTask<V>(new Callable<V>() {
            @Override
            public V call() throws Exception {
                if (delegateThrowException) {
                    throw new RuntimeException("Exception in execution");
                }
                return result;
            }
        });
    }

    private static <V> BasicCompletableFuture<V> basicCompletableFuture(Future<V> future) {
        NodeEngine engine = mock(NodeEngine.class);
        when(engine.getLogger(BasicCompletableFuture.class)).thenReturn(mock(ILogger.class));
        ExecutionService executionService = mock(ExecutionService.class);
        when(engine.getExecutionService()).thenReturn(executionService);
        when(executionService.getExecutor(anyString())).thenReturn(new TestCurrentThreadExecutor());
        return new BasicCompletableFuture<V>(future, engine);
    }

    private static class TestCurrentThreadExecutor extends ThreadPoolExecutor implements ManagedExecutorService {
        public TestCurrentThreadExecutor() {
            super(1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
        }

        public String getName() {
            return "hz:test:current:thread:executor";
        }

        public int getQueueSize() {
            return Integer.MAX_VALUE;
        }

        public int getRemainingQueueCapacity() {
            return Integer.MAX_VALUE;
        }

        public void execute(Runnable r) {
            // run in current thread
            r.run();
        }
    }

}
