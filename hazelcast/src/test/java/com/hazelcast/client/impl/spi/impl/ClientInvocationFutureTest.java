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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.sequence.CallIdSequence;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientInvocationFutureTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private ClientMessage request;
    private ClientMessage response;
    private ILogger logger;
    private SerializationService serializationService;
    private Data key;
    private Data value;
    private InternalCompletableFuture invocationFuture;
    private CallIdSequence callIdSequence;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        key = serializationService.toData("key");
        value = serializationService.toData("value");
        logger = mock(ILogger.class);
        request = MapGetCodec.encodeRequest("test", key, 1L);
        response = MapGetCodec.encodeResponse(value);
        callIdSequence = mock(CallIdSequence.class);
        invocationFuture = new ClientInvocationFuture(mock(ClientInvocation.class),
                request,
                logger,
                callIdSequence);
    }

    @Test
    public void test_normalCompletion()
            throws ExecutionException, InterruptedException {
        invocationFuture.complete(response);

        assertTrue(invocationFuture.isDone());
        assertFalse(invocationFuture.isCancelled());
        assertFalse(invocationFuture.isCompletedExceptionally());
        assertEquals(response, invocationFuture.get());
        assertEquals(response, invocationFuture.join());
        assertEquals(response, invocationFuture.joinInternal());
    }

    @Test
    public void test_exceptionalCompletion_withGet()
            throws ExecutionException, InterruptedException {
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        assertTrue(invocationFuture.isDone());
        assertFalse(invocationFuture.isCancelled());
        assertTrue(invocationFuture.isCompletedExceptionally());
        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        invocationFuture.get();
    }

    @Test
    public void test_exceptionalCompletion_withJoin() {
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        expected.expect(CompletionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        invocationFuture.join();
    }

    @Test
    public void test_exceptionalCompletion_withJoinInternal() {
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        expected.expect(IllegalArgumentException.class);
        invocationFuture.joinInternal();
    }

    @Test
    public void test_interruptionDuringGet()
            throws ExecutionException, InterruptedException {
        Thread thisThread = Thread.currentThread();
        Thread t = new Thread(() -> {
            sleepSeconds(2);
            thisThread.interrupt();
        });
        t.start();
        expected.expect(InterruptedException.class);
        invocationFuture.get();
    }

    @Test
    public void test_interruptionDuringGetWithTimeout()
            throws ExecutionException, InterruptedException, TimeoutException {
        Thread thisThread = Thread.currentThread();
        Thread t = new Thread(() -> {
            sleepSeconds(2);
            thisThread.interrupt();
        });
        t.start();
        expected.expect(InterruptedException.class);
        invocationFuture.get(30, TimeUnit.SECONDS);
    }

    @Test
    public void test_interruptionDuringJoin() {
        Thread thisThread = Thread.currentThread();
        Thread t = new Thread(() -> {
            sleepSeconds(2);
            thisThread.interrupt();
        });
        t.start();
        expected.expect(CompletionException.class);
        expected.expectCause(new RootCauseMatcher(InterruptedException.class));
        invocationFuture.join();
    }

    @Test
    public void test_cancellation()
            throws ExecutionException, InterruptedException {
        invocationFuture.cancel(true);

        assertTrue(invocationFuture.isDone());
        assertTrue(invocationFuture.isCancelled());
        expected.expect(CancellationException.class);
        invocationFuture.get();
    }

    @Test
    public void test_whenComplete() throws Exception {
        CompletableFuture nextStage = invocationFuture.whenComplete((value, throwable) -> {
            assertEquals(response, value);
        });
        invocationFuture.complete(response);

        assertEquals(response, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_thenRun() throws Exception {
        CompletableFuture nextStage = invocationFuture.thenRun(() -> {
            ignore(null);
        });
        invocationFuture.complete(response);

        assertEquals(null, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_thenCompose() throws Exception {
        CompletableFuture nextStage = invocationFuture.thenCompose((v) -> newCompletedFuture(v));
        invocationFuture.complete(response);

        assertEquals(response, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_thenApply() throws Exception {
        CompletableFuture nextStage = invocationFuture.thenApply((v) -> v);
        invocationFuture.complete(response);

        assertEquals(response, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_thenAccept() throws Exception {
        CompletableFuture nextStage = invocationFuture.thenAccept((v) -> ignore(null));
        invocationFuture.complete(response);

        assertEquals(null, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_thenAcceptBoth() throws Exception {
        CompletableFuture nextStage = invocationFuture.thenAcceptBoth(newCompletedFuture(null),
                (t, u) -> ignore(null));
        invocationFuture.complete(null);

        assertEquals(null, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_thenCombine() throws Exception {
        CompletableFuture nextStage = invocationFuture.thenCombine(newCompletedFuture(null),
                (t, u) -> t);
        invocationFuture.complete(response);

        assertEquals(response, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_exceptionally() throws Exception {
        CompletableFuture nextStage = invocationFuture.exceptionally((t) -> response);
        invocationFuture.completeExceptionally(new IllegalStateException());

        assertEquals(response, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_handle() throws Exception {
        CompletableFuture nextStage = invocationFuture.handle((t, u) -> t);
        invocationFuture.complete(response);

        assertEquals(response, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_acceptEither() throws Exception {
        CompletableFuture nextStage = invocationFuture.acceptEither(newCompletedFuture(null),
                t -> ignore(null));

        assertEquals(null, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(1)).complete();
    }

    @Test
    public void test_applyEither() throws Exception {
        CompletableFuture nextStage = invocationFuture.applyToEither(newCompletedFuture(null), (t) -> t);

        assertEquals(null, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(1)).complete();
    }

    @Test
    public void test_runAfterBoth() throws Exception {
        CompletableFuture<Void> nextStage = invocationFuture.runAfterBoth(newCompletedFuture(null), () -> ignore(null));
        invocationFuture.complete(null);

        assertEquals(null, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(2)).complete();
    }

    @Test
    public void test_runAfterEither() throws Exception {
        CompletableFuture<Void> nextStage = invocationFuture.runAfterEither(newCompletedFuture(null),
                () -> ignore(null));

        assertEquals(null, nextStage.get(10, TimeUnit.SECONDS));
        verify(callIdSequence).forceNext();
        verify(callIdSequence, times(1)).complete();
    }
}
