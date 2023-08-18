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

package com.hazelcast.internal.tpcengine;


import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.util.ThreadAffinity;
import org.jctools.util.PaddedAtomicLong;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static com.hazelcast.internal.tpcengine.FormatUtil.humanReadableCountSI;
import static com.hazelcast.internal.tpcengine.TaskQueue.Builder.MAX_NICE;
import static com.hazelcast.internal.tpcengine.TaskQueue.Builder.MIN_NICE;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
    public ReactorType reactorType = ReactorType.NIO;
    public boolean useTask = true;
    public int clockSampleInterval = 1;
    public int taskGroupCnt = 1;
    public boolean randomNiceLevel = false;
    public boolean cfs = true;
    public String affinity = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15";
    // When set to -1, value is ignored.
    public long minGranularityNanos = -1;
    // When set to -1, value is ignored.
    public long targetLatencyNanos = -1;
    public int reactorCount = 1;

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
                List<TaskQueue> taskQueues = new ArrayList<>();
                if (taskGroupCnt == 0) {
                    taskQueues.add(reactor.eventloop().defaultTaskQueue());
                } else {
                    for (int k = 0; k < taskGroupCnt; k++) {
                        taskQueues.add(taskGroupFactory.apply(reactor.eventloop()));
                    }
                }

                for (TaskQueue taskQueue : taskQueues) {
                    for (int k = 0; k < tasksPerTaskGroupCnt; k++) {
                        if (useTask) {
                            RunnableJob task = new RunnableJob(taskQueue, counter);
                            taskQueue.offer(task);
                        } else {
                            TaskJob task = new TaskJob(counter);
                            taskQueue.offer(task);
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

    private void printResults(long startMs) {
        long csCount = sum(csCounters);
        long durationMs = currentTimeMillis() - startMs;
        System.out.println("Duration " + durationMs + " ms");
        System.out.println("Context switches:" + csCount);
        System.out.println("Throughput:" + (csCount * 1000f / durationMs) + " tasks/second");
        System.out.println("Avg context switch latency:" + (MILLISECONDS.toNanos(durationMs) / (csCount / reactorCount)) + " ns");
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
        System.out.println("use completely fair scheduler:" + cfs);
        System.out.println("affinity:" + affinity);
        System.out.println("useTask:" + useTask);
    }

    private Reactor[] newReactors() {
        ThreadAffinity threadAffinity = affinity == null ? null : new ThreadAffinity(affinity);
        Reactor[] reactors = new Reactor[reactorCount];
        for (int k = 0; k < reactors.length; k++) {
            Reactor.Builder reactorBuilder = Reactor.Builder.newReactorBuilder(reactorType);
            reactorBuilder.cfs = cfs;
            reactorBuilder.runQueueLimit = taskGroupCnt + 1;
            reactorBuilder.threadAffinity = threadAffinity;

            if (minGranularityNanos != -1) {
                reactorBuilder.minGranularityNanos = minGranularityNanos;
            }

            if (targetLatencyNanos != -1) {
                reactorBuilder.targetLatencyNanos = targetLatencyNanos;
            }

            Reactor reactor = reactorBuilder.build();
            reactor.start();
            reactors[k] = reactor;
        }
        return reactors;
    }

    public final Function<Eventloop, TaskQueue> taskGroupFactory = eventloop -> {
        int nice = randomNiceLevel
                ? random.nextInt(MAX_NICE - MIN_NICE + 1) + MIN_NICE
                : 0;

        TaskQueue.Builder taskQueueBuilder = eventloop.newTaskQueueBuilder();
        taskQueueBuilder.nice = nice;
        taskQueueBuilder.clockSampleInterval = clockSampleInterval;
        taskQueueBuilder.inside = new CircularQueue<>(1024);
        return taskQueueBuilder.build();
    };

    private class RunnableJob implements Runnable {
        private final TaskQueue taskQueue;
        private final PaddedAtomicLong counter;

        public RunnableJob(TaskQueue taskQueue, PaddedAtomicLong counter) {
            this.taskQueue = taskQueue;
            this.counter = counter;
        }

        @Override
        public void run() {
            if (stop) {
                return;
            }

            counter.lazySet(counter.get() + 1);
            taskQueue.offer(this);
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
            long runtimeMs = SECONDS.toMillis(runtimeSeconds);
            long startMs = currentTimeMillis();
            long endMs = startMs + runtimeMs;
            StringBuffer sb = new StringBuffer();
            long last = 0;
            while (currentTimeMillis() < endMs) {
                Thread.sleep(SECONDS.toMillis(1));
                long nowMs = System.currentTimeMillis();

                long completedMs = MILLISECONDS.toSeconds(nowMs - startMs);
                long completedMinutes = completedMs / 60;
                long completedSeconds = completedMs % 60;

                double completed = (100f * completedMs) / runtimeMs;
                sb.append("  [done ");
                sb.append(completedMinutes);
                sb.append("m:");
                sb.append(completedSeconds);
                sb.append("s ");
                sb.append(String.format("%,.3f", completed));
                sb.append("%]");

                long eta = MILLISECONDS.toSeconds(endMs - nowMs);
                long etaMinutes = eta / 60;
                long etaSeconds = eta % 60;
                sb.append("[eta ");
                sb.append(etaMinutes);
                sb.append("m:");
                sb.append(etaSeconds);
                sb.append("s]");

                long total = sum(csCounters);
                long diff = total - last;
                last = total;

                sb.append("[thp=");
                sb.append(humanReadableCountSI(diff));
                sb.append("/s]");

                System.out.println(sb);
                sb.setLength(0);
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
