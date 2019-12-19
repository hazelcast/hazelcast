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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.FutureUtil;
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

import java.util.Arrays;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientInterceptingDelegatingFutureTest {

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
    private AtomicBoolean executed;

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
        executed = new AtomicBoolean();
    }

    @Test
    public void interceptorFromJoinInternal_whenCompletedNormally() {
        prepareDelegatingFuture(executedFlagConsumer());
        invocationFuture.complete(response);

        assertEquals(DESERIALIZED_VALUE, delegatingFuture.joinInternal());
        assertTrue(executed.get());
    }

    @Test
    public void interceptorFromGet_whenCompletedNormally()
            throws ExecutionException, InterruptedException {
        prepareDelegatingFuture(executedFlagConsumer());
        invocationFuture.complete(response);

        assertEquals(DESERIALIZED_VALUE, delegatingFuture.get());
        assertTrue(executed.get());
    }

    @Test
    public void interceptorFromJoin_whenCompletedNormally() {
        prepareDelegatingFuture(executedFlagConsumer());
        invocationFuture.complete(response);

        assertEquals(DESERIALIZED_VALUE, delegatingFuture.join());
        assertTrue(executed.get());
    }

    @Test
    public void interceptorFromJoin_whenCompletedExceptionally() {
        prepareDelegatingFuture(executedFlagConsumer());
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        try {
            delegatingFuture.join();
            fail();
        } catch (CompletionException e) {
            assertInstanceOf(IllegalArgumentException.class, e.getCause());
        }
        assertTrue(executed.get());
    }

    @Test
    public void interceptorFromJoinInternal_whenCompletedExceptionally() {
        prepareDelegatingFuture(executedFlagConsumer());
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        try {
            delegatingFuture.joinInternal();
            fail();
        } catch (IllegalArgumentException e) {
            ignore(e);
        }
        assertTrue(executed.get());
    }

    @Test
    public void interceptorFromGet_whenCompletedExceptionally()
            throws InterruptedException {
        prepareDelegatingFuture(executedFlagConsumer());
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        try {
            delegatingFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(IllegalArgumentException.class, e.getCause());
        }
        assertTrue(executed.get());
    }

    @Test
    public void callbackIsExecutedOnce_whenCompletedNormally() {
        callbackIsExecutedOnce(true);
    }

    @Test
    public void callbackIsExecutedOnce_whenCompletedExceptionally() {
        callbackIsExecutedOnce(false);
    }

    @Test
    public void getWithTimeout_whenDelegateIncomplete()
            throws InterruptedException, ExecutionException, TimeoutException {
        prepareDelegatingFuture(executedFlagConsumer());

        expected.expect(TimeoutException.class);
        delegatingFuture.get(50, TimeUnit.MILLISECONDS);
    }

    @Test
    public void getWithTimeout_whenDelegateCompletedButCallbackIncomplete()
            throws InterruptedException, ExecutionException, TimeoutException {
        CountDownLatch latch = new CountDownLatch(1);
        prepareDelegatingFuture((v, t) -> {
            latch.countDown();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                ignore(e);
            }
        });

        invocationFuture.complete(response);

        // spawn a separate thread to start execution of the callback
        spawn(delegatingFuture::joinInternal);
        // wait for other thread to start execution of long callback
        latch.await();
        expected.expect(TimeoutException.class);
        delegatingFuture.get(50, TimeUnit.MILLISECONDS);
    }

    @Test
    public void whenCompletedExceptionally_subsequentGetsThrowException() {
        prepareDelegatingFuture((v, t) -> ignore(t));

        invocationFuture.completeExceptionally(new IllegalStateException());

        for (int i = 0; i < 3; i++) {
            try {
                delegatingFuture.joinInternal();
                fail("Should have thrown IllegalStateException");
            } catch (IllegalStateException e) {
                ignore(e);
            }
        }
    }

    @Test
    public void whenCompletedNormally_subsequentGetsReturnSame() {
        prepareDelegatingFuture((v, t) -> ignore(t));

        invocationFuture.complete(response);
        String completionValue = delegatingFuture.joinInternal();

        for (int i = 0; i < 3; i++) {
            assertSame(completionValue, delegatingFuture.joinInternal());
        }
    }

    private void prepareDelegatingFuture(BiConsumer<String, Throwable> action) {
        delegatingFuture = new ClientInterceptingDelegatingFuture<>(invocationFuture, serializationService,
                clientMessage -> MapGetCodec.decodeResponse(clientMessage).response, action);
    }

    private void callbackIsExecutedOnce(boolean completedNormally) {
        ExecutionCountingConsumer countingConsumer = new ExecutionCountingConsumer();
        prepareDelegatingFuture(countingConsumer);

        CountDownLatch latch = new CountDownLatch(1);
        int concurrency = Runtime.getRuntime().availableProcessors();
        Future[] spawnedFutures = new Future[concurrency];
        for (int i = 0; i < concurrency; i++) {
            spawnedFutures[i] = spawn(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    ignore(e);
                }
                if (completedNormally) {
                    assertEquals(DESERIALIZED_VALUE, delegatingFuture.joinInternal());
                } else {
                    try {
                        delegatingFuture.joinInternal();
                        fail("Future should have been completed exceptionally");
                    } catch (IllegalArgumentException e) {
                        ignore(e);
                    }
                }
            });
        }
        if (completedNormally) {
            invocationFuture.complete(response);
        } else {
            invocationFuture.completeExceptionally(new IllegalArgumentException());
        }
        latch.countDown();

        FutureUtil.waitWithDeadline(Arrays.asList(spawnedFutures), 30, TimeUnit.SECONDS);
        assertEquals(1, countingConsumer.executionCount.get());
    }

    private BiConsumer<String, Throwable> executedFlagConsumer() {
        return (v, t) -> {
            if (DESERIALIZED_VALUE.equals(v) || (t instanceof IllegalArgumentException)) {
                executed.set(true);
            }
        };
    }

    private static class ExecutionCountingConsumer implements BiConsumer<String, Throwable> {
        final AtomicInteger executionCount = new AtomicInteger();

        @Override
        public void accept(String s, Throwable throwable) {
            executionCount.getAndIncrement();
        }
    }
}
