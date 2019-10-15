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

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static org.junit.Assert.assertEquals;
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
        delegatingFuture = new ClientInterceptingDelegatingFuture<>(invocationFuture, serializationService,
                clientMessage -> MapGetCodec.decodeResponse(clientMessage).response, (v, t) -> {
            if (DESERIALIZED_VALUE.equals(v) || (t instanceof IllegalArgumentException)) {
                executed.set(true);
            }
        });
    }

    @Test
    public void interceptorFromJoinInternal_whenCompletedNormally() {
        invocationFuture.complete(response);

        assertEquals(DESERIALIZED_VALUE, delegatingFuture.joinInternal());
        assertTrue(executed.get());
    }

    @Test
    public void interceptorFromGet_whenCompletedNormally()
            throws ExecutionException, InterruptedException {
        invocationFuture.complete(response);

        assertEquals(DESERIALIZED_VALUE, delegatingFuture.get());
        assertTrue(executed.get());
    }

    @Test
    public void interceptorFromJoin_whenCompletedNormally() {
        invocationFuture.complete(response);

        assertEquals(DESERIALIZED_VALUE, delegatingFuture.join());
        assertTrue(executed.get());
    }

    @Test
    public void interceptorFromJoin_whenCompletedExceptionally() {
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
        invocationFuture.completeExceptionally(new IllegalArgumentException());

        try {
            delegatingFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(IllegalArgumentException.class, e.getCause());
        }
        assertTrue(executed.get());
    }

}
