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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertSuccessEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class DeadlineSchedulerSoakTest {
    public static final int DEADLINE_RUN_QUEUE_LIMIT = 100;

    public int concurrentDeadlineTasks = 100;
    public long iterations = 1000;

    private Reactor reactor;
    private AtomicLong completed = new AtomicLong();
    private PrintAtomicLongThread progressThread = new PrintAtomicLongThread("completed:", completed);

    @Before
    public void before() {
        Reactor.Builder builder = newReactorBuilder();
        builder.deadlineRunQueueLimit = concurrentDeadlineTasks;
        // make sure that the runQueue can handle the amount of deadline tasks.
        builder.runQueueLimit = concurrentDeadlineTasks + 100;
        reactor = builder.build();
        reactor.start();

        progressThread.start();
    }

    @After
    public void after() throws InterruptedException {
        terminate(reactor);
        progressThread.shutdown();
    }

    public abstract Reactor.Builder newReactorBuilder();

    @Test
    public void test_fixedSchedule() {
        test(false);
    }

    @Test
    public void test_fixedRate() {
        test(true);
    }

    public void test(boolean fixedRate) {
        List<ScheduledCallable> tasks = new ArrayList<>();
        for (int k = 0; k < concurrentDeadlineTasks; k++) {
            tasks.add(new ScheduledCallable());
        }

        reactor.offer(new Runnable() {
            @Override
            public void run() {
                Eventloop eventloop = reactor.eventloop();
                DeadlineScheduler deadlineScheduler = eventloop.deadlineScheduler();
                for (int k = 0; k < concurrentDeadlineTasks; k++) {
                    if (fixedRate) {
                        deadlineScheduler.scheduleAtFixedRate(
                                tasks.get(k), 0, 1, MILLISECONDS, reactor.defaultTaskQueue());
                    } else {
                        deadlineScheduler.scheduleWithFixedDelay(
                                tasks.get(k), 0, 1, MILLISECONDS, reactor.defaultTaskQueue());
                    }
                }
            }
        });

        for (ScheduledCallable task : tasks) {
            assertSuccessEventually(task.future);
        }
    }

    private class ScheduledCallable implements Callable<Boolean> {

        private int iteration;
        private CompletableFuture future = new CompletableFuture();

        @Override
        public Boolean call() throws Exception {
            if (iteration == iterations) {
                future.complete(null);
                return false;
            }
            completed.incrementAndGet();
            iteration++;
            return true;
        }
    }
}
