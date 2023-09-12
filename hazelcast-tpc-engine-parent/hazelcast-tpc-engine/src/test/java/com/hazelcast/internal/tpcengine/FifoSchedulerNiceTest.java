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

import org.jctools.queues.MpscArrayQueue;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.Reactor.Builder.newReactorBuilder;
import static com.hazelcast.internal.tpcengine.ReactorType.NIO;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertSuccessEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;

public class FifoSchedulerNiceTest {
    private final List<Reactor> reactors = new ArrayList<>();

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    public Reactor newReactor(Consumer<Reactor.Builder> configFn) {
        Reactor.Builder reactorBuilder = newReactorBuilder(NIO);
        reactorBuilder.cfs = false;
        if (configFn != null) {
            configFn.accept(reactorBuilder);
        }
        Reactor reactor = reactorBuilder.build();
        reactors.add(reactor);
        reactor.start();
        return reactor;
    }

    @Test
    public void test() throws InterruptedException {
        Reactor reactor = newReactor(null);

        CompletableFuture<List<DummyTask>> future = reactor.submit(new Callable<List<DummyTask>>() {
            @Override
            public List<DummyTask> call() throws Exception {
                Eventloop eventloop = reactor.eventloop();

                List<DummyTask> tasks = new ArrayList<>();
                int descriptor = 1;
                for (int nice = TaskQueue.Builder.MIN_NICE; nice < TaskQueue.Builder.MAX_NICE; nice++) {
                    TaskQueue.Builder taskQueueBuilder = reactor.newTaskQueueBuilder();
                    taskQueueBuilder.nice = nice;
                    taskQueueBuilder.descriptor = descriptor;
                    taskQueueBuilder.queue = new MpscArrayQueue<>(1024);
                    taskQueueBuilder.concurrent = true;
                    TaskQueue taskQueue = taskQueueBuilder.build();
                    DummyTask dummyTask = new DummyTask();
                    tasks.add(dummyTask);
                    taskQueue.offer(dummyTask);
                    descriptor++;
                }
                return tasks;
            }
        });

        assertSuccessEventually(future);
        List<DummyTask> tasks = future.join();

        Thread.sleep(6000);

        // every task should be performed roughly the same number of times because
        // the fifoscheduler doesn't care for the nice level of the TaskQueue.
        boolean first = true;
        long firstCount = 0;
        int failures = 0;
        StringBuffer sb = new StringBuffer();
        for (int k = 0; k < tasks.size(); k++) {
            DummyTask task = tasks.get(k);
            long current = task.runs.get();
            if (first) {
                firstCount = current;
                sb.append(k + " " + current).append('\n');
            } else {
                double differencePercent = 100 * (1 - (1.0d * current / firstCount));
                // If this test fails spuriously we could slightly increase the
                // 3.0 value or run longer.
                boolean success = abs(differencePercent) < 3.0;
                if (!success) {
                    failures++;
                }
                sb.append(k + " " + current + " " + differencePercent + "% success:" + success + "\n");
            }
            first = false;
        }

        System.out.println(sb.toString());
        assertEquals(sb.toString(), 0, failures);
    }

    private class DummyTask extends Task {
        private final AtomicLong runs = new AtomicLong();

        @Override
        public int run() throws Throwable {
            runs.incrementAndGet();
            Thread.yield();
            return RUN_YIELD;
        }
    }
}
