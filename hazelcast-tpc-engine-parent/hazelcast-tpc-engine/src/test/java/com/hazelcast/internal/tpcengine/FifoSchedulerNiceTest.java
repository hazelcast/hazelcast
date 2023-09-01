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

import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertEquals;

public class FifoSchedulerNiceTest {
    private final List<Reactor> reactors = new ArrayList<>();

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    public Reactor newReactor(Consumer<Reactor.Builder> configFn) {
        Reactor.Builder reactorBuilder = Reactor.Builder.newReactorBuilder(ReactorType.NIO);
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
                for (int nice = TaskQueue.Builder.MIN_NICE; nice < TaskQueue.Builder.MAX_NICE; nice++) {
                    TaskQueue.Builder taskQueueBuilder = eventloop.newTaskQueueBuilder();
                    taskQueueBuilder.nice = nice;
                    taskQueueBuilder.inside = new MpscArrayQueue<>(1024);
                    taskQueueBuilder.outside = new MpscArrayQueue<>(1024);
                    TaskQueue taskQueue = taskQueueBuilder.build();
                    DummyTask dummyTask = new DummyTask();
                    tasks.add(dummyTask);
                    taskQueue.offer(dummyTask);
                }
                return tasks;
            }
        });

        TpcTestSupport.assertSuccessEventually(future);
        List<DummyTask> tasks = future.join();

        Thread.sleep(2000);

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
                double v = 100 * (1 - (1.0d * current / firstCount));
                boolean success = v > -1.0 && v < 1.0;
                if (!success) {
                    failures++;
                }
                sb.append(k + " " + current + " " + v + " success:" + success + "\n");
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
            return RUN_YIELD;
        }
    }
}
