/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast;


import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.Task;
import com.hazelcast.internal.tpcengine.TaskProcessor;
import com.hazelcast.internal.tpcengine.TaskQueueHandle;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.util.ThreadAffinity;
import org.jctools.util.PaddedAtomicLong;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static com.hazelcast.internal.tpcengine.TaskQueueBuilder.MAX_NICE;
import static com.hazelcast.internal.tpcengine.TaskQueueBuilder.MIN_NICE;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Ideally the performance of the context switch should be bound to the time it takes to call
 * the {@link System#nanoTime()}. And that should be around the 25-30 ns on Linux.
 * <p>
 * Make sure the following JVM parameter is added:
 * --add-opens java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SchedulingBenchmark {

    public int runtimeSeconds = 20;
    public int tasksPerTaskGroupCnt = 1;
    public boolean useEventloopDirectly = true;
    public ReactorType reactorType = ReactorType.NIO;
    public boolean useTask = true;
    public int clockSampleInterval = 1;
    public int taskGroupCnt = 10;
    public boolean randomNiceLevel = false;
    public boolean useCfs = true;
    public String affinity = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15";
    // When set to -1, value is ignored.
    public long minGranularityNanos = -1;
    // When set to -1, value is ignored.
    public long targetLatencyNanos = -1;
    public int reactorCount = 16;

    private volatile boolean stop = false;
    private PaddedAtomicLong[] csCounters;
    private final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        SchedulingBenchmark benchmark = new SchedulingBenchmark();
        benchmark.run();
    }

    public void run() throws InterruptedException {
        printConfig();

        csCounters = new PaddedAtomicLong[reactorCount];
        for (int k = 0; k < reactorCount; k++) {
            csCounters[k] = new PaddedAtomicLong();
        }

        Reactor[] reactors = newReactors();

        long start = currentTimeMillis();

        for (int reactorIndex = 0; reactorIndex < reactorCount; reactorIndex++) {
            Reactor reactor = reactors[reactorIndex];
            final PaddedAtomicLong counter = csCounters[reactorIndex];
            reactor.execute(() -> {
                List<TaskQueueHandle> handles = new ArrayList<>();
                if (taskGroupCnt == 0) {
                    handles.add(reactor.eventloop().defaultTaskQueueHandle());
                } else {
                    for (int k = 0; k < taskGroupCnt; k++) {
                        handles.add(taskGroupFactory.apply(reactor.eventloop()));
                    }
                }

                for (TaskQueueHandle handle : handles) {
                    for (int k = 0; k < tasksPerTaskGroupCnt; k++) {
                        if (useTask) {
                            RunnableJob task = new RunnableJob(reactor, handle, useEventloopDirectly, counter);
                            reactor.offer(task, handle);
                        } else {
                            TaskJob task = new TaskJob(counter);
                            reactor.offer(task, handle);
                        }
                    }
                }
            });
        }

        MonitorThread monitor = new MonitorThread();
        monitor.start();
        monitor.join();

        for (Reactor reactor : reactors) {
            reactor.shutdown();
        }

        for (Reactor reactor : reactors) {
            reactor.awaitTermination(5, SECONDS);
        }

        printResults(start);
    }

    private void printResults(long start) {
        long csCount = sum(csCounters);
        long duration = currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Context switches:" + csCount);
        System.out.println("Throughput:" + (csCount * 1000f / duration) + " tasks/second");
    }

    private void printConfig() {
        System.out.println("ReactorType:" + reactorType);
        System.out.println("reactorCount:" + reactorCount);
        System.out.println("minGranularityNanos:" + minGranularityNanos);
        System.out.println("targetLatencyNanos:" + targetLatencyNanos);
        System.out.println("runtimeSeconds:" + runtimeSeconds);
        System.out.println("clockSampleInterval:" + clockSampleInterval);
        System.out.println("taskGroupCnt:" + taskGroupCnt);
        System.out.println("tasksPerTaskGroupCnt:" + tasksPerTaskGroupCnt);
        System.out.println("randomNiceLevel:" + randomNiceLevel);
        System.out.println("use completely fair scheduler:" + useCfs);
        System.out.println("affinity:" + affinity);
        System.out.println("useEventloopDirectly:" + useEventloopDirectly);
        System.out.println("useTask:" + useTask);
    }

    private Reactor[] newReactors() {
        ThreadAffinity threadAffinity = affinity == null ? null : new ThreadAffinity(affinity);
        Reactor[] reactors = new Reactor[reactorCount];
        for (int k = 0; k < reactors.length; k++) {
            ReactorBuilder builder = ReactorBuilder.newReactorBuilder(reactorType);
            builder.setCfs(useCfs);
            builder.setRunQueueCapacity(taskGroupCnt + 1);

            if (minGranularityNanos != -1) {
                builder.setMinGranularity(minGranularityNanos, NANOSECONDS);
            }

            if (targetLatencyNanos != -1) {
                builder.setTargetLatency(targetLatencyNanos, NANOSECONDS);
            }

            builder.setThreadAffinity(threadAffinity);

            Reactor reactor = builder.build();
            reactor.start();
            reactors[k] = reactor;
        }
        return reactors;
    }

    public final Function<Eventloop, TaskQueueHandle> taskGroupFactory = eventloop -> {
        int priority = randomNiceLevel
                ? random.nextInt(MAX_NICE - MIN_NICE + 1) + MIN_NICE
                : 0;

        return eventloop
                .newTaskQueueBuilder()
                .setNice(priority)
                .setClockSampleInterval(clockSampleInterval)
                .setLocal(new CircularQueue<>(1024))
                .build();
    };

    private class RunnableJob implements Runnable {
        private final Eventloop eventloop;
        private final boolean useEventloopDirectly;
        private final Reactor reactor;
        private final TaskQueueHandle taskGroupHandle;
        private final PaddedAtomicLong counter;

        public RunnableJob(Reactor reactor,
                           TaskQueueHandle taskGroupHandle,
                           boolean useEventloopDirectly,
                           PaddedAtomicLong counter) {
            this.reactor = reactor;
            this.eventloop = reactor.eventloop();
            this.taskGroupHandle = taskGroupHandle;
            this.useEventloopDirectly = useEventloopDirectly;
            this.counter = counter;
        }

        @Override
        public void run() {
            if (stop) {
                return;
            }

            counter.lazySet(counter.get() + 1);
            if (useEventloopDirectly) {
                eventloop.offer(this, taskGroupHandle);
            } else {
                reactor.offer(this, taskGroupHandle);
            }
        }
    }

    private class TaskJob extends Task {
        private final PaddedAtomicLong counter;

        private TaskJob(PaddedAtomicLong counter) {
            this.counter = counter;
        }

        @Override
        public int process() {
            if (stop) {
                return TaskProcessor.TASK_COMPLETED;
            }

            counter.lazySet(counter.get() + 1);
            return TaskProcessor.TASK_YIELD;
        }
    }

    private class MonitorThread extends Thread {
        private long last = 0;

        public MonitorThread() {
            super("MonitorThread");
        }

        @Override
        public void run() {
            try {
                run0();
            } catch (Throwable t) {
                t.printStackTrace();
            }
            stop = true;
        }

        private void run0() throws Exception {
            long end = currentTimeMillis() + SECONDS.toMillis(runtimeSeconds);
            while (currentTimeMillis() < end) {
                Thread.sleep(SECONDS.toMillis(1));

                long total = sum(csCounters);
                long diff = total - last;
                last = total;
                System.out.println("  thp " + diff + " tasks/sec");
            }
        }
    }

    private static long sum(PaddedAtomicLong[] array) {
        long sum = 0;
        for (PaddedAtomicLong c : array) {
            sum += c.get();
        }
        return sum;
    }
}
