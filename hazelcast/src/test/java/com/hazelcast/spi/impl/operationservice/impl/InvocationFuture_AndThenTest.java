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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationFuture_AndThenTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private OperationServiceImpl operationService;

    @Before
    public void setup() {
        local = createHazelcastInstance();
        operationService = getOperationService(local);
    }

    @Test(expected = NullPointerException.class)
    public void whenNullCallback() {
        DummyOperation op = new DummyOperation(null);

        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, op, getAddress(local));

        future.whenCompleteAsync(null);
    }

    @Test
    public void whenNodeIsShutdown() {
        Address address = getAddress(local);
        local.shutdown();

        DummyOperation op = new DummyOperation(null);
        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, op, address);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> reference = new AtomicReference<>();
        future.exceptionally(t -> {
            reference.set(t);
            latch.countDown();
            return null;
        });
        assertOpenEventually(latch);
        assertInstanceOf(HazelcastInstanceNotActiveException.class, reference.get());
    }

    @Test(expected = NullPointerException.class)
    public void whenNullCallback2() {
        DummyOperation op = new DummyOperation(null);

        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, op, getAddress(local));

        future.whenCompleteAsync(null, mock(Executor.class));
    }

    @Test(expected = NullPointerException.class)
    public void whenNullExecutor() {
        DummyOperation op = new DummyOperation(null);

        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, op, getAddress(local));

        future.whenCompleteAsync(getExecutionCallbackMock(), null);
    }

    // there is a bug: https://github.com/hazelcast/hazelcast/issues/5001
    @Test
    public void whenNullResponse_thenCallbackExecuted() throws ExecutionException, InterruptedException {
        DummyOperation op = new DummyOperation(null);
        final BiConsumer<Object, Throwable> callback = getExecutionCallbackMock();
        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, op, getAddress(local));
        future.get();

        // callback can be completed immediately, since a response (NULL_RESPONSE) has been already set
        future.whenCompleteAsync(callback);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback, times(1)).accept(isNull(), isNull());
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static BiConsumer<Object, Throwable> getExecutionCallbackMock() {
        return mock(BiConsumer.class);
    }
}
