package com.hazelcast;


import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.Task;
import com.hazelcast.internal.tpcengine.TaskQueueHandle;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Ideally the performance of the context switch should be bound to the time it takes to call
 * the {@link System#nanoTime()}. And that should be around the 25-30 ns on Linux.
 *
 * Make sure the following JVM parameter is added:
 * --add-opens java.base/sun.nio.ch=ALL-UNNAMED
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Threads(value = 1)
public class ContextSwitchBenchmark {

    public static final int operations = 100 * 1000 * 1000;
    public static final int concurrency = 1;
    public static final boolean useEventloopDirectly = true;
    public static final ReactorType reactorType = ReactorType.NIO;
    public static final boolean useTask = true;
    public static final boolean usePrimordialTaskQueue = true;
    public static final int clockSampleInterval = 1;
    private Reactor reactor;

    @Setup
    public void setup() {
        ReactorBuilder reactorBuilder = ReactorBuilder.newReactorBuilder(reactorType);
        reactorBuilder.setCfs(true);
        //reactorBuilder.setBatchSize(1);
        //reactorBuilder.setClockRefreshPeriod(1);
        reactor = reactorBuilder.build();
        reactor.start();
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        reactor.shutdown();
        reactor.awaitTermination(5, SECONDS);
    }

    @Benchmark
    @OperationsPerInvocation(value = operations)
    public void run() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(concurrency);
        reactor.execute(() -> {
            TaskQueueHandle handle;
            if (usePrimordialTaskQueue) {
                handle = reactor.eventloop().primordialTaskQueueHandle();
            } else {
                handle = reactor.eventloop().newTaskQueueBuilder()
                        .setLocal(new CircularQueue<>(1024))
                        .setClockSampleInterval(clockSampleInterval)
                        .setName("bla")
                        .build();
            }

            for (int k = 0; k < concurrency; k++) {
                if (useTask) {
                    RunnableJob task = new RunnableJob(reactor, handle, operations / concurrency, latch, useEventloopDirectly);
                    reactor.offer(task, handle);
                } else {
                    TaskJob task = new TaskJob(operations / concurrency, latch);
                    reactor.offer(task, handle);
                }
            }
        });

        latch.await();
    }

    private static class RunnableJob implements Runnable {
        private final CountDownLatch latch;
        private final Eventloop eventloop;
        private final boolean useEventloopDirectly;
        private final Reactor reactor;
        private final long operations;
        private final TaskQueueHandle taskGroupHandle;
        private long iteration = 0;

        public RunnableJob(Reactor reactor,
                           TaskQueueHandle taskGroupHandle,
                           long operations,
                           CountDownLatch latch,
                           boolean useEventloopDirectly) {
            this.reactor = reactor;
            this.eventloop = reactor.eventloop();
            this.operations = operations;
            this.taskGroupHandle = taskGroupHandle;
            this.latch = latch;
            this.useEventloopDirectly = useEventloopDirectly;
        }

        @Override
        public void run() {
            iteration++;
            if (operations == iteration) {
                latch.countDown();
            } else if (useEventloopDirectly) {
                eventloop.offer(this, taskGroupHandle);
            } else {
                reactor.offer(this, taskGroupHandle);
            }
        }
    }

    private static class TaskJob extends Task {
        private final CountDownLatch latch;
        private final long operations;
        private long iteration = 0;

        public TaskJob(long operations, CountDownLatch latch) {
            this.operations = operations;
            this.latch = latch;
        }

        @Override
        public int process() {
            iteration++;
            if (operations == iteration) {
                latch.countDown();
                return Task.TASK_COMPLETED;
            } else {
                return Task.TASK_YIELD;
            }
        }
    }
}
