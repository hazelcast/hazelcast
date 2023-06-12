package com.hazelcast;


import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.Task;
import com.hazelcast.internal.tpcengine.TaskQueueBuilder;
import com.hazelcast.internal.tpcengine.TaskQueueHandle;
import com.hazelcast.internal.tpcengine.util.CircularQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.hazelcast.internal.tpcengine.TaskQueueBuilder.MAX_NICE;
import static com.hazelcast.internal.tpcengine.TaskQueueBuilder.MIN_NICE;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Ideally the performance of the context switch should be bound to the time it takes to call
 * the {@link System#nanoTime()}. And that should be around the 25-30 ns on Linux.
 * <p>
 * Make sure the following JVM parameter is added:
 * --add-opens java.base/sun.nio.ch=ALL-UNNAMED
 */

public class ContextSwitchBenchmark {

    public static final int durationSeconds = 600;
    public static final int operations = 100 * 1000 * 1000;
    public static final int tasksPerTaskGroup = 1;
    public static final boolean useEventloopDirectly = true;
    public static final ReactorType reactorType = ReactorType.IOURING;
    public static final boolean useTask = true;
    public static final int clockSampleInterval = 1;
    public static final int taskGroupCount = 10;
    public static final boolean randomNiceLevel = false;
    public static final boolean useCfs = true;

    private static final List<TaskQueueHandle> handles = new ArrayList<>();
    private static volatile boolean stop = false;
    private static final AtomicLong counter = new AtomicLong();

    public static final Function<Eventloop, TaskQueueHandle> taskGroupFactor = eventloop -> {
        Random random = new Random();
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

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch completionLatch = new CountDownLatch(taskGroupCount * tasksPerTaskGroup);

        ReactorBuilder reactorBuilder = ReactorBuilder.newReactorBuilder(reactorType);
        reactorBuilder.setCfs(useCfs);
        reactorBuilder.setRunQueueCapacity(taskGroupCount + 1);
        //reactorBuilder.setBatchSize(1);
        //reactorBuilder.setClockRefreshPeriod(1);
        Reactor reactor = reactorBuilder.build();
        reactor.start();

        long start = System.currentTimeMillis();

        reactor.execute(() -> {
            if (taskGroupCount == 0) {
                handles.add(reactor.eventloop().primordialTaskQueueHandle());
            } else {
                for (int k = 0; k < taskGroupCount; k++) {
                    handles.add(taskGroupFactor.apply(reactor.eventloop()));
                }
            }

            for (TaskQueueHandle handle : handles) {
                for (int k = 0; k < tasksPerTaskGroup; k++) {
                    if (useTask) {
                        RunnableJob task = new RunnableJob(reactor, handle, completionLatch, useEventloopDirectly);
                        reactor.offer(task, handle);
                    } else {
                        TaskJob task = new TaskJob(completionLatch);
                        reactor.offer(task, handle);
                    }
                }
            }
        });

        Monitor monitor = new Monitor(durationSeconds);
        monitor.start();
        monitor.join();

        long count = counter.get();

        long duration = System.currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (count * 1000f / duration) + " tasks/second");

    }

    private static class RunnableJob implements Runnable {
        private final CountDownLatch latch;
        private final Eventloop eventloop;
        private final boolean useEventloopDirectly;
        private final Reactor reactor;
        private final TaskQueueHandle taskGroupHandle;

        public RunnableJob(Reactor reactor,
                           TaskQueueHandle taskGroupHandle,
                           CountDownLatch latch,
                           boolean useEventloopDirectly) {
            this.reactor = reactor;
            this.eventloop = reactor.eventloop();
            this.taskGroupHandle = taskGroupHandle;
            this.latch = latch;
            this.useEventloopDirectly = useEventloopDirectly;
        }

        @Override
        public void run() {
            if (stop) {
                return;
            }

            counter.setOpaque(counter.getOpaque() + 1);
            if (useEventloopDirectly) {
                eventloop.offer(this, taskGroupHandle);
            } else {
                reactor.offer(this, taskGroupHandle);
            }
        }
    }

    private static class TaskJob extends Task {
        private final CountDownLatch latch;

        public TaskJob(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public int process() {
            if (stop) {
                latch.countDown();
                return Task.TASK_COMPLETED;
            }

            counter.setOpaque(counter.getOpaque() + 1);
            return Task.TASK_YIELD;
        }
    }

    private static class Monitor extends Thread {
        private final int durationSecond;
        private long last = 0;

        public Monitor(int durationSecond) {
            this.durationSecond = durationSecond;
        }

        @Override
        public void run() {
            long end = System.currentTimeMillis() + SECONDS.toMillis(durationSecond);
            while (System.currentTimeMillis() < end) {
                try {
                    Thread.sleep(SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                }

                long total = counter.get();
                long diff = total - last;
                last = total;
                System.out.println("  thp " + diff + " tasks/sec");
            }

            stop = true;
        }
    }
}
