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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

// This test was introduced in https://github.com/hazelcast/hazelcast/pull/9508 and ensures that
// execution callback's onFailure is invoked even when the designated executor rejects the callback
// with RejectedExecutionException. This is wrong. Maybe client shutdown relies on this behaviour.
@Ignore
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractInvocationFuture_ClosedExecutorTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void whenCompleteBeforeShutdown_thenCallback() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TestFuture future = new TestFuture(executorService, logger);
        future.complete(new Object());
        executorService.shutdown();
        final AtomicBoolean onFailure = new AtomicBoolean();
        future.exceptionally(t -> {
                if (t instanceof HazelcastInstanceNotActiveException) {
                    onFailure.set(true);
                }
                return null;
            });
        assertTrue(onFailure.get());
    }

    @Test
    public void whenCompleteAfterShutdown_thenCallback() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TestFuture future = new TestFuture(executorService, logger);
        executorService.shutdown();
        future.complete(new Object());
        final AtomicBoolean onFailure = new AtomicBoolean();
        future.exceptionally(t -> {
            if (t instanceof HazelcastInstanceNotActiveException) {
                onFailure.set(true);
            }
            return null;
        });
        assertTrue(onFailure.get());
    }

}
