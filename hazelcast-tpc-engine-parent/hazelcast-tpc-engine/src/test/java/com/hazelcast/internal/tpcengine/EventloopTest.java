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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.CircularQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.tpcengine.Eventloop.getThreadLocalEventloop;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertSuccessEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static junit.framework.TestCase.assertSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public abstract class EventloopTest {

    private Reactor reactor;

    public int runQueueCapacity = 1024;

    public abstract Reactor.Builder newReactorBuilder();

    @Before
    public void before() {
        Reactor.Builder builder = newReactorBuilder();
        builder.runQueueLimit = runQueueCapacity;
        reactor = builder.build().start();
    }

    @After
    public void after() {
        terminate(reactor);
    }

    @Test
    public void test_tooManyTaskQueues() {
        CompletableFuture future = new CompletableFuture();
        reactor.offer(() -> {
            for (int k = 0; k < runQueueCapacity - 1; k++) {
                TaskQueue.Builder taskQueueBuilder = reactor.eventloop.newTaskQueueBuilder();
                taskQueueBuilder.inside = new CircularQueue<>(10);
                taskQueueBuilder.build();
            }

            try {
                TaskQueue.Builder taskQueueBuilder = reactor.eventloop.newTaskQueueBuilder();
                taskQueueBuilder.inside = new CircularQueue<>(10);
                taskQueueBuilder.build();
                future.completeExceptionally(
                        new IllegalStateException(
                                "Should not be able to create more than " + runQueueCapacity + " task queues"));
            } catch (IllegalStateException e) {
                future.complete(null);
            }
        });

        future.join();
    }


    @Test
    public void test_checkOnEventloopThread_whenNotOnEventloopThread() {
        assertThrows(IllegalThreadStateException.class, () -> reactor.eventloop.checkOnEventloopThread());
    }

    @Test
    public void test_checkOnEventloopThread_whenOnEventloopThread() {
        CompletableFuture<Void> future = reactor.submit(() -> reactor.eventloop.checkOnEventloopThread());
        assertSuccessEventually(future);
    }

    @Test
    public void test_getThreadlocalEventloop() throws ExecutionException, InterruptedException {
        assertNull(getThreadLocalEventloop());

        CompletableFuture<Eventloop> future = reactor.submit(Eventloop::getThreadLocalEventloop);

        assertSuccessEventually(future);
        assertSame(reactor.eventloop, future.get());
    }
}
