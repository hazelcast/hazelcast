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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDelegatingFutureTest {

    private static final String DESERIALIZED_VALUE = "value";

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private ClientMessage request;
    private ClientMessage response;
    private ILogger logger;
    private SerializationService serializationService;
    private Data key;
    private Data value;
    private ClientInvocationFuture invocationFuture;
    private InternalCompletableFuture<String> delegatingFuture;
    private CallIdSequence callIdSequence;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        key = serializationService.toData("key");
        value = serializationService.toData(DESERIALIZED_VALUE);
        logger = mock(ILogger.class);
        request = MapGetCodec.encodeRequest("test", key, 1L);
        response = MapGetCodec.encodeResponse(value);
        callIdSequence = mock(CallIdSequence.class);
        invocationFuture = new ClientInvocationFuture(mock(ClientInvocation.class),
                request,
                logger,
                callIdSequence);
        delegatingFuture = new ClientDelegatingFuture<>(invocationFuture, serializationService,
                clientMessage -> MapGetCodec.decodeResponse(clientMessage).response, true);
    }

    @Test
    public void get_whenCompletedNormally() throws Exception {
        invocationFuture.complete(response);

        assertTrue(delegatingFuture.isDone());
        assertEquals(DESERIALIZED_VALUE, delegatingFuture.get());
    }

    @Test
    public void get_whenCompletedExceptionally() throws Exception {
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        assertTrue(delegatingFuture.isDone());
        assertTrue(delegatingFuture.isCompletedExceptionally());
        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        delegatingFuture.get();
    }

    @Test
    public void join_whenCompletedExceptionally() {
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        expected.expect(CompletionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        delegatingFuture.join();
    }

    @Test
    public void joinInternal_whenCompletedExceptionally() {
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        expected.expect(IllegalArgumentException.class);
        expected.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        delegatingFuture.joinInternal();
    }

    @Test
    public void thenApply() {
        CompletableFuture<Long> nextStage = delegatingFuture.thenApply(v -> {
            assertEquals(DESERIALIZED_VALUE, v);
            return 1L;
        });

        invocationFuture.complete(response);
        assertEquals(1L, nextStage.join().longValue());
    }

    @Test
    public void thenAccept() {
        CompletableFuture<Void> nextStage = delegatingFuture.thenAccept(v -> {
            assertEquals(DESERIALIZED_VALUE, v);
        });
        invocationFuture.complete(response);

        nextStage.join();
    }

    @Test
    public void thenRun() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Void> nextStage = delegatingFuture.thenRun(latch::countDown);
        invocationFuture.complete(response);

        latch.await(10, TimeUnit.SECONDS);
        nextStage.get();
    }

    @Test
    public void thenRun_whenCompletedExceptionally() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Void> nextStage = delegatingFuture.thenRun(latch::countDown);
        invocationFuture.completeExceptionally(new IllegalStateException());

        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalStateException.class));
        nextStage.get();
    }

    @Test
    public void thenCombine() {
        CompletableFuture<String> nextStage = delegatingFuture.thenCombine(newCompletedFuture("value2"),
                (v1, v2) -> v1);
        invocationFuture.complete(response);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals(DESERIALIZED_VALUE, nextStage.join());
    }

    @Test
    public void thenAcceptBoth() {
        CompletableFuture<Void> nextStage = delegatingFuture.thenAcceptBoth(newCompletedFuture("otherValue"),
                (v1, v2) -> assertEquals(DESERIALIZED_VALUE, v1));
        invocationFuture.complete(response);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        nextStage.join();
    }

    @Test
    public void runAfterBoth() {
        CompletableFuture<Void> nextStage = delegatingFuture.runAfterBoth(newCompletedFuture("otherValue"),
                () -> ignore(null));
        invocationFuture.complete(response);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        nextStage.join();
    }

    @Test
    public void applyToEither() {
        CompletableFuture<Long> nextStage = delegatingFuture.applyToEither(new CompletableFuture<String>(),
                (v) -> {
            assertEquals(DESERIALIZED_VALUE, v);
            return 1L;
        });
        invocationFuture.complete(response);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals(1L, nextStage.join().longValue());
    }

    @Test
    public void acceptEither() {
        CompletableFuture<Void> nextStage = delegatingFuture.acceptEither(new CompletableFuture<>(),
                (v) -> assertEquals(DESERIALIZED_VALUE, v));
        invocationFuture.complete(response);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        nextStage.join();
    }

    @Test
    public void runAfterEither() {
        CompletableFuture<Void> nextStage = delegatingFuture.runAfterEither(new CompletableFuture<>(),
                () -> ignore(null));
        invocationFuture.complete(response);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
    }

    @Test
    public void thenCompose() {
        CompletableFuture<Long> nextStage = delegatingFuture.thenCompose(v -> {
            assertEquals(DESERIALIZED_VALUE, v);
            return newCompletedFuture(1L);
        });
        invocationFuture.complete(response);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals(1L, nextStage.join().longValue());
    }

    @Test
    public void whenComplete() {
        CompletableFuture<String> nextStage = delegatingFuture.whenComplete((v, t) -> {
            assertEquals(DESERIALIZED_VALUE, v);
        });
        invocationFuture.complete(response);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals(DESERIALIZED_VALUE, nextStage.join());
    }

    @Test
    public void whenComplete_whenExceptional() {
        CompletableFuture<String> nextStage = delegatingFuture.whenComplete((v, t) -> {
            assertInstanceOf(IllegalArgumentException.class, t);
        });
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        expected.expect(CompletionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        delegatingFuture.join();
    }

    @Test
    public void handle() {
        CompletableFuture<Long> nextStage = delegatingFuture.handle((v, t) -> {
            assertEquals(DESERIALIZED_VALUE, v);
            return 1L;
        });
        invocationFuture.complete(response);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals(1L, nextStage.join().longValue());
    }

    @Test
    public void exceptionally() {
        CompletableFuture<String> nextStage = delegatingFuture.exceptionally(t -> "value2");
        invocationFuture.completeExceptionally(new IllegalStateException());

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals("value2", nextStage.join());
    }

    @Test
    public void cancel_whenDelegateCancelled() {
        invocationFuture.cancel(true);

        assertTrue(invocationFuture.isCancelled());
        assertTrue(delegatingFuture.isCancelled());
    }

    @Test
    public void cancel_whenOuterFutureCancelled() {
        delegatingFuture.cancel(true);

        assertTrue(invocationFuture.isCancelled());
        assertTrue(delegatingFuture.isCancelled());
    }
}
