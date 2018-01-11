/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_TaskDoneTest extends HazelcastTestSupport {

    private InternalOperationService operationService;

    @Before
    public void before() {
        operationService = getOperationService(createHazelcastInstance());
    }

    @Test
    public void when_invocationDone_thenCallbackRuns() {
        // Given
        final DummyOperation op = new DummyOperation(null);
        final DoneCallback cb = new DoneCallback();

        // When
        operationService.createInvocationBuilder("mockService", op, 0).setDoneCallback(cb).invoke();

        // Then
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(cb.done);
            }
        });
    }

    @Test
    public void when_invocationFutureCanceled_thenCallbackRunsEventually() throws InterruptedException {
        // Given
        final LatchAwaitOperation latchAwaitOp = new LatchAwaitOperation();
        final DoneCallback cb = new DoneCallback();
        final ICompletableFuture<Object> fut =
                operationService.createInvocationBuilder("mockService", latchAwaitOp, 0).setDoneCallback(cb).invoke();
        final FailedLatchExecutionCallback canceledCallback = new FailedLatchExecutionCallback();
        fut.andThen(canceledCallback);

        // When
        fut.cancel(true);
        assertOpenEventually(canceledCallback.latch);

        // Then
        assertFalse(cb.done);
        latchAwaitOp.latch.countDown();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(cb.done);
            }
        });
    }

    static class DoneCallback implements Runnable {
        volatile boolean done;

        @Override
        public void run() {
            done = true;
        }
    }

    static class LatchAwaitOperation extends Operation {
        final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run() throws Exception {
            latch.await();
        }
    }

    static class FailedLatchExecutionCallback implements ExecutionCallback<Object> {
        final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onFailure(Throwable t) {
            latch.countDown();
        }

        @Override
        public void onResponse(Object response) {
        }
    }
}
