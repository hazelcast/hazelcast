/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.invocation;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.CompletableFutureAbstractTest;
import com.hazelcast.spi.impl.sequence.CallIdSequenceWithoutBackpressure;
import com.hazelcast.test.ExpectedRuntimeException;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;

public class ClientDelegatingFutureTest_CompletionStageTest extends CompletableFutureAbstractTest {
    /*
     * This test sets up a member & client, as required for
     * construction of ClientInvocation. However, invocations are not
     * sent to the member, as we need to complete invocations normally or
     * exceptionally depending on test case.
     */
    private TestHazelcastFactory factory;
    private ClientMessage response;
    private SerializationService serializationService;
    private ClientInvocation invocation;
    private ClientInvocationFuture invocationFuture;

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(getConfig());
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(factory.newHazelcastClient());
        serializationService = new DefaultSerializationServiceBuilder().build();
        Data key = serializationService.toData("key");
        Data value = serializationService.toData(returnValue);
        ClientMessage request = MapGetCodec.encodeRequest("test", key, 1L);
        response = MapGetCodec.encodeResponse(value);
        ILogger logger = Logger.getLogger(ClientDelegatingFutureTest_CompletionStageTest.class);
        invocationFuture = new ClientInvocationFuture(() -> true, request, logger,
                new CallIdSequenceWithoutBackpressure(), Long.MAX_VALUE);
        invocation = new ClientInvocation(invocationFuture, (ClientInvocationServiceImpl) client.getInvocationService(),
                request, null, null,
                throwable -> false, Long.MAX_VALUE, false, false, -1, null, null);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    @Override
    protected CompletableFuture<Object> newCompletableFuture(boolean exceptional, long completeAfterMillis) {
        invocation.getCallIdSequence().next();

        ClientDelegatingFuture<Object> future =
                new ClientDelegatingFuture<>(invocationFuture, serializationService, MapGetCodec::decodeResponse);

        Executor completionExecutor;
        if (completeAfterMillis <= 0) {
            completionExecutor = CALLER_RUNS;
        } else {
            completionExecutor = command -> new Thread(() -> {
                sleepAtLeastMillis(completeAfterMillis);
                command.run();
            }, "test-completion-thread").start();
        }
        if (exceptional) {
            completionExecutor.execute(() -> invocation.completeExceptionally(new ExpectedRuntimeException()));
        } else {
            completionExecutor.execute(completeNormally(invocation));
        }
        return future;
    }

    private Runnable completeNormally(ClientInvocation future) {
        return () -> future.complete(response);
    }

}
