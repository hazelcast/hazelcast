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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

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
        BiConsumer<String, Throwable> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.whenCompleteAsync(callback, CALLER_RUNS);

        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_withException_callbacksNeverRun() {
        BiConsumer<String, Throwable> callback = getStringExecutionCallback();
        delegateThrowException = true;

        delegateFuture.run();
        outerFuture.whenCompleteAsync(callback, CALLER_RUNS);

        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_successfully_callbackAfterGet_invokeIsDoneOnOuter_callbacksRun() {
        BiConsumer<String, Throwable> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.isDone();
        outerFuture.whenCompleteAsync(callback, CALLER_RUNS);

        verify(callback, times(1)).accept(any(String.class), isNull());
        verify(callback, times(0)).accept(isNull(), any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_successfully_callbackAfterGet_invokeGetOnOuter_callbacksRun() throws Exception {
        BiConsumer<String, Throwable> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.get();
        outerFuture.whenCompleteAsync(callback, CALLER_RUNS);

        verify(callback, times(1)).accept(any(String.class), isNull());
        verify(callback, times(0)).accept(isNull(), any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_successfully_callbackBeforeGet_invokeIsDoneOnOuter_callbacksRun() throws Exception {
        BiConsumer<String, Throwable> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.whenCompleteAsync(callback, CALLER_RUNS);
        outerFuture.isDone();

        verify(callback, times(1)).accept(any(String.class), isNull());
        verify(callback, times(0)).accept(isNull(), any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_successfully_callbackBeforeGet_invokeGetOnOuter_callbacksRun() throws Exception {
        BiConsumer<String, Throwable> callback = getStringExecutionCallback();

        delegateFuture.run();
        outerFuture.whenCompleteAsync(callback, CALLER_RUNS);
        outerFuture.get();

        verify(callback, times(1)).accept(any(String.class), isNull());
        verify(callback, times(0)).accept(isNull(), any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_withException_callbackBeforeGet_invokeIsDoneOnOuter_callbacksRun() {
        BiConsumer<String, Throwable> callback = getStringExecutionCallback();
        delegateThrowException = true;

        delegateFuture.run();
        outerFuture.whenCompleteAsync(callback, CALLER_RUNS);
        outerFuture.isDone();

        verify(callback, times(0)).accept(any(String.class), isNull());
        verify(callback, times(1)).accept(isNull(), any(Throwable.class));
        verifyZeroInteractions(callback);
    }

    @Test
    public void completeDelegate_withException_callbackBeforeGet_invokeGetOnOuter_callbacksNeverReached() {
        BiConsumer<String, Throwable> callback = getStringExecutionCallback();
        delegateThrowException = true;

        delegateFuture.run();
        outerFuture.whenCompleteAsync(callback, CALLER_RUNS);

        try {
            outerFuture.get();
            fail();
        } catch (Throwable t) {
            assertEquals("Exception in execution", t.getCause().getMessage());
        }

        verify(callback, times(0)).accept(any(String.class), isNull());
        verify(callback, times(1)).accept(isNull(), any(Throwable.class));
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
    private static BiConsumer<String, Throwable> getStringExecutionCallback() {
        return mock(BiConsumer.class);
    }

    private static <V> BasicCompletableFuture<V> basicCompletableFuture(Future<V> future) {
        return new BasicCompletableFuture<>(future);
    }
}
