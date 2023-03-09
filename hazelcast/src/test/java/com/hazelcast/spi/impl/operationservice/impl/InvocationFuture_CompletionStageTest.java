/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.OperationTimeoutException;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.hamcrest.CoreMatchers.is;

/**
 * Tests the {@link CompletionStage} implementation of {@link InvocationFuture}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationFuture_CompletionStageTest extends CompletableFutureAbstractTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

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

    // InvocationFuture can be completed with InvocationConstant which indicates exceptional completion.
    // Test that using these InvocationConstants results in exceptional completion of superclass (CompletableFuture)
    // so InvocationFutures are interoperable with JDK's CompletableFutures.
    @Test
    public void testInvocationFuture_superCompletedExceptionally_whenCompletedWithCallTimeoutConstant() {
        InvocationFuture<Void> invocationFuture = invokeSleepingOperation();
        CompletableFuture cf = CompletableFuture.allOf(invocationFuture);
        invocationFuture.complete(InvocationConstant.CALL_TIMEOUT);
        expected.expect(CompletionException.class);
        expected.expectCause(is(OperationTimeoutException.class));
        cf.join();
    }

    @Test
    public void testInvocationFuture_superCompletedExceptionally_whenCompletedWithInterruptedConstant() {
        InvocationFuture<Void> invocationFuture = invokeSleepingOperation();
        CompletableFuture cf = CompletableFuture.allOf(invocationFuture);
        invocationFuture.complete(InvocationConstant.INTERRUPTED);
        expected.expect(CompletionException.class);
        expected.expectCause(is(InterruptedException.class));
        cf.join();
    }

    @Test
    public void testInvocationFuture_superCompletedExceptionally_whenCompletedWithHeartbeatTimeoutConstant() {
        InvocationFuture<Void> invocationFuture = invokeSleepingOperation();
        CompletableFuture cf = CompletableFuture.allOf(invocationFuture);
        invocationFuture.complete(InvocationConstant.HEARTBEAT_TIMEOUT);
        expected.expect(CompletionException.class);
        expected.expectCause(is(OperationTimeoutException.class));
        expected.expectMessage("heartbeat");
        cf.join();
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

    InvocationFuture<Void> invokeSleepingOperation() {
        return (InvocationFuture) CompletableFutureTestUtil.invokeAsync(local,
                new OperationServiceImpl_timeoutTest.SleepingOperation(100_000));
    }
}
