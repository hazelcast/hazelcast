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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.logging.ILogger;
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
import java.util.concurrent.ExecutionException;

import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDelegatingFutureTest {

    private static final String DESERIALIZED_VALUE = "value";
    private static final String DESERIALIZED_DEFAULT_VALUE = "default_value";

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private ClientMessage request;
    private ClientMessage response;
    private ILogger logger;
    private SerializationService serializationService;
    private Data key;
    private Data value;
    private ClientInvocationFuture invocationFuture;
    private ClientDelegatingFuture<String> delegatingFuture;
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
                MapGetCodec::decodeResponse, true);
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
        delegatingFuture.joinInternal();
    }

    @Test
    public void getNow_whenNotDoneShouldReturnDefaultValue() throws Exception {
        assertTrue(!delegatingFuture.isDone());
        assertEquals(DESERIALIZED_DEFAULT_VALUE, delegatingFuture.getNow(DESERIALIZED_DEFAULT_VALUE));
    }

    @Test
    public void getNow_whenDoneReturnValue() throws Exception {
        invocationFuture.complete(response);

        assertTrue(delegatingFuture.isDone());
        assertEquals(DESERIALIZED_VALUE, delegatingFuture.getNow(DESERIALIZED_DEFAULT_VALUE));
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
    }

    @Test
    public void cancel_whenOuterFutureCancelled() {
        delegatingFuture.cancel(true);

        assertTrue(invocationFuture.isCancelled());
        assertTrue(delegatingFuture.isCancelled());
    }

    @Test
    public void get_cachedValue() throws Exception {
        invocationFuture.complete(response);

        assertTrue(delegatingFuture.isDone());
        String cachedValue = delegatingFuture.get();
        assertEquals(DESERIALIZED_VALUE, cachedValue);
        assertSame(cachedValue, delegatingFuture.get());
    }

    @Test
    public void getNow_cachedValue() throws Exception {
        invocationFuture.complete(response);

        assertTrue(delegatingFuture.isDone());
        String cachedValue = delegatingFuture.get();
        assertEquals(DESERIALIZED_VALUE, cachedValue);
        assertSame(cachedValue, delegatingFuture.getNow(DESERIALIZED_DEFAULT_VALUE));
    }
}
