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

package com.hazelcast.spi.impl;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.ExpectedRuntimeException;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.spi.impl.InternalCompletableFuture.completedExceptionally;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;

public class DelegatingCompletableFutureTest_CompletionStageTest extends CompletableFutureAbstractTest {

    private TestHazelcastFactory factory;
    private SerializationService serializationService;
    private Data value;

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(getConfig());
        serializationService = new DefaultSerializationServiceBuilder().build();
        value = serializationService.toData(returnValue);
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
    protected InternalCompletableFuture<Object> newCompletableFuture(boolean exceptional, long completeAfterMillis) {
        InternalCompletableFuture<Object> future = incompleteFuture();
        Executor completionExecutor;
        if (completeAfterMillis <= 0) {
            completionExecutor = CALLER_RUNS;
        } else {
            completionExecutor = new Executor() {
                @Override
                public void execute(Runnable command) {
                    new Thread(() -> {
                        sleepAtLeastMillis(completeAfterMillis);
                        command.run();
                    }, "test-completion-thread").start();
                }
            };
        }
        if (exceptional) {
            completionExecutor.execute(() -> future.completeExceptionally(new ExpectedRuntimeException()));
        } else {
            completionExecutor.execute(completeNormally(future));
        }
        return new DelegatingCompletableFuture<Object>(serializationService, future);
    }

    protected InternalCompletableFuture<Object> incompleteFuture() {
        return new InternalCompletableFuture<>();
    }

    protected Runnable completeNormally(InternalCompletableFuture<Object> future) {
        return () -> future.complete(returnValue);
    }

}
