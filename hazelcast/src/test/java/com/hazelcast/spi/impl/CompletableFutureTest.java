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

package com.hazelcast.spi.impl;

import com.hazelcast.test.ExpectedRuntimeException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;

public class CompletableFutureTest extends CompletableFutureAbstractTest {

    @Override
    protected CompletableFuture<Object> newCompletableFuture(boolean exceptional, long completeAfterMillis) {
        CompletableFuture<Object> future = incompleteFuture();
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
            completionExecutor.execute(() -> future.completeExceptionally(new ExpectedRuntimeException()));
        } else {
            completionExecutor.execute(completeNormally(future));
        }
        return future;
    }

    private CompletableFuture<Object> incompleteFuture() {
        return new CompletableFuture<>();
    }

    private Runnable completeNormally(CompletableFuture<Object> future) {
        return () -> future.complete(returnValue);
    }
}
