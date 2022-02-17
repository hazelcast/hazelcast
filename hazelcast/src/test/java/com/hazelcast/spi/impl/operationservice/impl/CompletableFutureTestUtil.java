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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.ExpectedRuntimeException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.impl.operationservice.InvocationBuilder.DEFAULT_CALL_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.InvocationBuilder.DEFAULT_TRY_COUNT;
import static com.hazelcast.spi.impl.operationservice.InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getOperationService;

public class CompletableFutureTestUtil {

    public static void ignore() {
        // no-op
    }

    static <T> CompletableFuture<T> invokeSync(HazelcastInstance instance, boolean throwsException) {
        return throwsException ? invokeSync(instance, new Operation() {
            @Override
            public void run() {
                throw new ExpectedRuntimeException();
            }
        }) : invokeSync(instance, new DummyOperation(null));
    }

    static <T> CompletableFuture<T> invokeAsync(HazelcastInstance instance, boolean throwsException) {
        return throwsException ? invokeAsync(instance, new Operation() {
            @Override
            public void run() {
                throw new ExpectedRuntimeException();
            }
        }) : invokeAsync(instance, new SlowOperation(3000));
    }

    public static <R> InternalCompletableFuture<R> invokeSync(HazelcastInstance instance, Operation operation) {
        OperationService operationService = getOperationService(instance);
        Address local = getAddress(instance);
        return operationService.invokeOnTarget(null, operation, local);
    }

    public static <T> InternalCompletableFuture<T> invokeAsync(HazelcastInstance instance, Operation operation) {
        OperationServiceImpl operationService = getOperationService(instance);
        Address local = getAddress(instance);
        TargetInvocation invocation = new TargetInvocation(operationService.getInvocationContext(), operation, local,
                DEFAULT_TRY_COUNT, DEFAULT_TRY_PAUSE_MILLIS, DEFAULT_CALL_TIMEOUT, true);
        return invocation.invokeAsync();
    }

    static class InvocationPromise {
        final boolean sync;
        final boolean throwsException;

        InvocationPromise(boolean sync, boolean throwsException) {
            this.throwsException = throwsException;
            this.sync = sync;
        }

        public <T> CompletableFuture<T> invoke(HazelcastInstance instance) {
            return sync
                    ? invokeSync(instance, throwsException)
                    : invokeAsync(instance, throwsException);
        }

        @Override
        public String toString() {
            return "{" + (sync  ? "sync" : "async")
            + (throwsException ? ",exception}" : "}");
        }
    }

    public static final class CountingExecutor implements Executor {
        public AtomicInteger counter = new AtomicInteger();
        public AtomicBoolean completed = new AtomicBoolean();

        @Override
        public void execute(Runnable command) {
            counter.getAndIncrement();
            completed.set(true);
            command.run();
        }
    }
}
