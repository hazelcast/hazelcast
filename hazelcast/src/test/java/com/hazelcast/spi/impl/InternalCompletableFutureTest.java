/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Executor;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalCompletableFutureTest extends CompletableFutureAbstractTest {

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
        return future;
    }

    protected InternalCompletableFuture<Object> incompleteFuture() {
        return new InternalCompletableFuture<>();
    }

    protected Runnable completeNormally(InternalCompletableFuture<Object> future) {
        return () -> future.complete(returnValue);
    }
}
