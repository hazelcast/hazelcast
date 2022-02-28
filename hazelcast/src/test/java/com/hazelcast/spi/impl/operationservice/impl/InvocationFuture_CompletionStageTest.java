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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.CompletableFutureAbstractTest;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionStage;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;

/**
 * Tests the {@link CompletionStage} implementation of {@link InvocationFuture}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationFuture_CompletionStageTest extends CompletableFutureAbstractTest {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance local;

    @Before
    public void setup() {
        factory = new TestHazelcastInstanceFactory();
        local = factory.newHazelcastInstance(getConfig());
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
        if (completeAfterMillis <= 0) {
            return invokeSync(local, exceptional);
        } else {
            return invokeAsync(local, exceptional, completeAfterMillis);
        }
    }

    InternalCompletableFuture<Object> invokeSync(HazelcastInstance instance, boolean throwsException) {
        return throwsException ? CompletableFutureTestUtil.invokeSync(instance, new Operation() {
            @Override
            public void run() {
                throw new ExpectedRuntimeException();
            }
        }) : CompletableFutureTestUtil.invokeSync(instance, new DummyOperation(returnValue));
    }

    InternalCompletableFuture<Object> invokeAsync(HazelcastInstance instance, boolean throwsException,
                                                        long completeAfterMillis) {
        return throwsException ? CompletableFutureTestUtil.invokeAsync(instance, new Operation() {
            @Override
            public void run() {
                throw new ExpectedRuntimeException();
            }
        }) : CompletableFutureTestUtil.invokeAsync(instance, new SlowOperation(completeAfterMillis, returnValue));
    }
}
