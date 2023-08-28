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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertEqualsEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertSuccessEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueOneSecond;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.sleepMillis;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static java.lang.Math.abs;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public abstract class DeadlineSchedulerTest {

    private final List<Reactor> reactors = new ArrayList<>();

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    public abstract Reactor.Builder newReactorBuilder();

    private Reactor newReactor() {
        return newReactor(null);
    }

    private Reactor newReactor(Consumer<Reactor.Builder> configFn) {
        Reactor.Builder reactorBuilder = newReactorBuilder();
        if (configFn != null) {
            configFn.accept(reactorBuilder);
        }
        Reactor reactor = reactorBuilder.build();
        reactors.add(reactor);
        return reactor;
    }

    private static final class CountingRunnable implements Runnable {
        private final AtomicLong count = new AtomicLong();

        @Override
        public void run() {
            count.incrementAndGet();
        }
    }

    public class FirstTimeExceptionCallable implements Callable<Boolean> {
        private AtomicLong calls = new AtomicLong();
        private int iteration;

        @Override
        public Boolean call() throws Exception {
            calls.incrementAndGet();
            if (calls.get() == 1) {
                throw new RuntimeException();
            }
            return true;
        }
    }

    private static class SleepingCallable implements Callable<Boolean> {
        private final long sleepMs;
        private final AtomicInteger count = new AtomicInteger();

        private SleepingCallable(long sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public Boolean call() throws Exception {
            count.incrementAndGet();
            Thread.sleep(sleepMs);
            return true;
        }
    }

    @Test
    public void test_schedule_runQueueOverload() {
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
    public void test_scheduleAtFixedRate_whenRunQueueOverload() {
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

            assertFalse(deadlineScheduler.scheduleAtFixedRate(
                    mock(Callable.class), 0, 100, SECONDS, reactor.defaultTaskQueue));
        });

        assertSuccessEventually(success);
    }

    @Test
    public void test_scheduleAtFixedRate() {
        Reactor reactor = newReactor();
        reactor.start();
        int periodMillis = 100;

        SleepingCallable sleepingCallable = new SleepingCallable(80);
        CompletableFuture<Void> success = reactor.submit(() -> {
            Eventloop eventloop = reactor.eventloop;
            DeadlineScheduler deadlineScheduler = eventloop.deadlineScheduler();

            assertTrue(deadlineScheduler.scheduleAtFixedRate(
                    sleepingCallable, 0, periodMillis, MILLISECONDS, reactor.defaultTaskQueue));
        });

        assertSuccessEventually(success);
        // the shorter the test, the bigger the chance you run into a false positive.
        long durationMillis = SECONDS.toMillis(5);
        sleepMillis(durationMillis);
        long actual = sleepingCallable.count.get();
        long expected = durationMillis / periodMillis;
        assertTrue("actual:" + actual + " expected:" + expected, abs(actual - expected) < 5);
    }

    @Test
    public void test_scheduleAtFixedRate_whenCallableReturnsFalse_thenNotRescheduled() {
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
        assertTrueOneSecond(() -> {
            assertEquals(countdownCallable.limit, countdownCallable.calls.get());
        });
    }

    @Test
    public void test_scheduleAtFixedRate_whenCallableFails_thenStopRunning() {
        int deadlineRunQueueLimit = 100;
        Reactor reactor = newReactor();
        reactor.start();

        FirstTimeExceptionCallable firstTimeException = new FirstTimeExceptionCallable();
        CompletableFuture<Void> success = reactor.submit(() -> {
            Eventloop eventloop = reactor.eventloop;
            DeadlineScheduler deadlineScheduler = eventloop.deadlineScheduler();
            deadlineScheduler.scheduleAtFixedRate(
                    firstTimeException, 1, 1, SECONDS, reactor.defaultTaskQueue);
        });

        assertSuccessEventually(success);
        assertTrueEventually(() -> {
            assertEquals(1, firstTimeException.calls.get());
        });
        assertTrueOneSecond(() -> {
            assertEquals(1, firstTimeException.calls.get());
        });
    }

    @Test
    public void test_scheduleWithFixedDelay_whenRunQueueOverload() {
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

            assertFalse(deadlineScheduler.scheduleWithFixedDelay(
                    mock(Callable.class), 0, 100, SECONDS, reactor.defaultTaskQueue));
        });

        assertSuccessEventually(success);
    }

    @Test
    public void test_scheduleWithFixedDelay_whenCallableFailsThenStopRunning() {
        Reactor reactor = newReactor();
        reactor.start();

        FirstTimeExceptionCallable firstTimeException = new FirstTimeExceptionCallable();
        CompletableFuture<Void> success = reactor.submit(() -> {
            Eventloop eventloop = reactor.eventloop;
            DeadlineScheduler deadlineScheduler = eventloop.deadlineScheduler();
            deadlineScheduler.scheduleWithFixedDelay(
                    firstTimeException, 1, 1, SECONDS, reactor.defaultTaskQueue);
        });

        assertSuccessEventually(success);
        assertTrueEventually(() -> {
            assertEquals(1, firstTimeException.calls.get());
        });
        assertTrueOneSecond(() -> {
            assertEquals(1, firstTimeException.calls.get());
        });
    }

    @Test
    public void test_scheduleAtFixedDelay_whenCallableReturnsFalse_thenNotRescheduled() {
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
        assertTrueOneSecond(() -> {
            assertEquals(countdownCallable.limit, countdownCallable.calls.get());
        });
    }

    @Test
    public void test_scheduleWithFixedDelay() {
        Reactor reactor = newReactor();
        reactor.start();
        int delayMillis = 100;

        int sleepMs = 50;
        SleepingCallable sleepingCallable = new SleepingCallable(sleepMs);
        CompletableFuture<Void> success = reactor.submit(() -> {
            Eventloop eventloop = reactor.eventloop;
            DeadlineScheduler deadlineScheduler = eventloop.deadlineScheduler();

            assertTrue(deadlineScheduler.scheduleWithFixedDelay(
                    sleepingCallable, 0, delayMillis, MILLISECONDS, reactor.defaultTaskQueue));
        });

        assertSuccessEventually(success);
        long durationMillis = SECONDS.toMillis(2);
        sleepMillis(durationMillis);
        long actual = sleepingCallable.count.get();
        long expected = durationMillis / (delayMillis + sleepMs);
        assertTrue("actual:" + actual + " expected:" + expected, abs(actual - expected) < 3);
    }

    private static final class CountdownCallable implements Callable<Boolean> {
        private final int limit;
        private final CompletableFuture future = new CompletableFuture();
        private final AtomicLong calls = new AtomicLong();

        private CountdownCallable(int limit) {
            this.limit = limit;
        }

        @Override
        public Boolean call() throws Exception {
            calls.incrementAndGet();

            if (calls.get() == limit) {
                future.complete(null);
                return false;
            }

            return true;
        }
    }

    @Test
    public void test_schedule() {
        Reactor reactor = newReactor();
        reactor.start();

        CountingRunnable task = new CountingRunnable();

        reactor.offer(() -> reactor.eventloop.deadlineScheduler().schedule(task, 1, SECONDS));

        assertTrueEventually(() -> assertEquals(1, task.count.get()));
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
