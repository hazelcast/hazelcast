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

import com.hazelcast.internal.tpcengine.util.Promise;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertEqualsEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertSuccessEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public abstract class DeadlineSchedulerTest {

    private Reactor reactor;

    public void before() {
        reactor = newReactor();
    }

    @After
    public void after() throws InterruptedException {
        terminate(reactor);
    }

    public abstract Reactor.Builder newReactorBuilder();

    public Reactor newReactor() {
        return newReactor(null);
    }

    public Reactor newReactor(Consumer<Reactor.Builder> configFn) {
        Reactor.Builder reactorBuilder = newReactorBuilder();
        if (configFn != null) {
            configFn.accept(reactorBuilder);
        }
        Reactor reactor = reactorBuilder.build();
        return reactor;
    }

    @Test
    public void test_runQueueOverload() {
        int deadlineRunQueueLimit = 100;
        Reactor reactor = newReactor(new Consumer<Reactor.Builder>() {
            @Override
            public void accept(Reactor.Builder builder) {
                builder.deadlineRunQueueLimit = deadlineRunQueueLimit;
            }
        });
        reactor.start();

        CompletableFuture<Void> success = reactor.submit(() -> {
            Eventloop eventloop = reactor.eventloop;
            DeadlineScheduler deadlineScheduler = eventloop.deadlineScheduler();
            for (int k = 0; k < deadlineRunQueueLimit; k++) {
                assertTrue(deadlineScheduler.schedule(mock(Runnable.class), 100, SECONDS));
            }

            assertFalse(deadlineScheduler.schedule(mock(Runnable.class), 100, SECONDS));
        });

        assertSuccessEventually(success);
    }

    @Test
    public void test_scheduleAtFixedRate() {
        Reactor reactor = newReactor();
        reactor.start();

        CountdownCallable countdownCallable = new CountdownCallable(100);

        reactor.offer(() -> {
            Eventloop eventloop = reactor.eventloop;
            DeadlineScheduler deadlineScheduler = eventloop.deadlineScheduler();
            assertTrue(deadlineScheduler.scheduleAtFixedRate(
                    countdownCallable, 0, 5, MILLISECONDS, reactor.defaultTaskQueue()));
        });

        assertSuccessEventually(countdownCallable.future);
    }

    @Test
    public void test_scheduleAtFixedDelay() {
        Reactor reactor = newReactor();
        reactor.start();

        CountdownCallable countdownCallable = new CountdownCallable(100);

        reactor.offer(() -> {
            Eventloop eventloop = reactor.eventloop;
            DeadlineScheduler deadlineScheduler = eventloop.deadlineScheduler();
            assertTrue(deadlineScheduler.scheduleWithFixedDelay(
                    countdownCallable, 0, 5, MILLISECONDS, reactor.defaultTaskQueue()));

        });

        assertSuccessEventually(countdownCallable.future);
    }

    private static final class CountdownCallable implements Callable<Boolean> {
        private final int limit;
        private final CompletableFuture future = new CompletableFuture();
        private int current;

        private CountdownCallable(int limit) {
            this.limit = limit;
        }

        @Override
        public Boolean call() throws Exception {
            if (current == limit - 1) {
                future.complete(null);
                return false;
            }
            current++;
            return true;
        }
    }

    @Test
    public void test_schedule() {
        Reactor reactor = newReactor();
        reactor.start();

        Task task = new Task();

        reactor.offer(() -> reactor.eventloop.deadlineScheduler().schedule(task, 1, SECONDS));

        assertTrueEventually(() -> assertEquals(1, task.count.get()));
    }

    private static final class Task implements Runnable {
        private final AtomicLong count = new AtomicLong();

        @Override
        public void run() {
            count.incrementAndGet();
        }
    }

    @Test
    public void test_sleep() {
        Reactor reactor = newReactor();
        reactor.start();

        AtomicInteger executedCount = new AtomicInteger();
        long startMs = System.currentTimeMillis();

        reactor.offer(() -> {
            Promise promise = new Promise(reactor.eventloop);
            reactor.eventloop().deadlineScheduler.sleep(1, SECONDS, promise);
            promise.then((o, ex) -> executedCount.incrementAndGet());
        });

        assertEqualsEventually(1, executedCount);
        long duration = System.currentTimeMillis() - startMs;
        System.out.println("duration:" + duration + " ms");
    }

}
