/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ManagedExecutorService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicCompletableFutureTest {

    private static final String DELEGATE_RESULT = "DELEGATE_RESULT";

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
    public void completeDelegate_successfully_callbacksNeverRun() {
        ExecutionCallback<String> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.andThen(callback);

        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_withException_callbacksNeverRun() {
        ExecutionCallback<String> callback = getStringExecutionCallback();
        delegateThrowException = true;

        delegateFuture.run();
        outerFuture.andThen(callback);

        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_successfully_callbackAfterGet_invokeIsDoneOnOuter_callbacksRun() {
        ExecutionCallback<String> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.isDone();
        outerFuture.andThen(callback);

        verify(callback, times(1)).onResponse(any(String.class));
        verify(callback, times(0)).onFailure(any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_successfully_callbackAfterGet_invokeGetOnOuter_callbacksRun() throws Exception {
        ExecutionCallback<String> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.get();
        outerFuture.andThen(callback);

        verify(callback, times(1)).onResponse(any(String.class));
        verify(callback, times(0)).onFailure(any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_successfully_callbackBeforeGet_invokeIsDoneOnOuter_callbacksRun() {
        ExecutionCallback<String> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.andThen(callback);
        outerFuture.isDone();

        verify(callback, times(1)).onResponse(any(String.class));
        verify(callback, times(0)).onFailure(any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_successfully_callbackBeforeGet_invokeGetOnOuter_callbacksRun() throws Exception {
        ExecutionCallback<String> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.andThen(callback);
        outerFuture.get();

        verify(callback, times(1)).onResponse(any(String.class));
        verify(callback, times(0)).onFailure(any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_withException_callbackBeforeGet_invokeIsDoneOnOuter_callbacksRun() {
        ExecutionCallback<String> callback = getStringExecutionCallback();
        delegateThrowException = true;

        delegateFuture.run();
        outerFuture.andThen(callback);
        outerFuture.isDone();

        verify(callback, times(0)).onResponse(any(String.class));
        verify(callback, times(1)).onFailure(any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_withException_callbackBeforeGet_invokeGetOnOuter_callbacksNeverReached() {
        ExecutionCallback<String> callback = getStringExecutionCallback();
        delegateThrowException = true;

        delegateFuture.run();
        outerFuture.andThen(callback);

        try {
            outerFuture.get();
            fail();
        } catch (Throwable t) {
            assertEquals("Exception in execution", t.getCause().getMessage());
        }

        verify(callback, times(0)).onResponse(any(String.class));
        verify(callback, times(1)).onFailure(any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    private <V> FutureTask<V> future(final V result) {
        return new FutureTask<V>(() -> {
            if (delegateThrowException) {
                throw new RuntimeException("Exception in execution");
            }
            return result;
        });
    }

    @SuppressWarnings("unchecked")
    private static ExecutionCallback<String> getStringExecutionCallback() {
        return mock(ExecutionCallback.class);
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
        TestCurrentThreadExecutor() {
            super(1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
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

        public void execute(Runnable runnable) {
            // run in current thread
            try {
                runnable.run();
            } catch (Exception ex) {
                sneakyThrow(new ExecutionException(ex));
            }
        }
    }
}
