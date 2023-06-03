package com.hazelcast;


import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.TaskGroupHandle;
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
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Threads(value = 1)
public class ContextSwitchBenchmark {

    public static final int operations = 100 * 1000 * 1000;
    public static final int concurrency = 10;
    public static final boolean useEventloopDirectly = true;
    public static final ReactorType reactorType = ReactorType.NIO;

    private Reactor reactor;

    @Setup
    public void setup() {
        ReactorBuilder reactorBuilder = ReactorBuilder.newReactorBuilder(reactorType);
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
//            TaskGroupHandle handle = reactor.eventloop().rootTaskGroupHandle;
            TaskGroupHandle handle = reactor.eventloop().newTaskGroupBuilder()
                    .setLocalQueue(new CircularQueue<>(1024))
                    .setName("bla")
                    .build();

//            eventloop.unsafe().schedule(() -> {
//            }, 1000, SECONDS);

            for (int k = 0; k < concurrency; k++) {
                Job task = new Job(reactor, handle, operations / concurrency, latch, useEventloopDirectly);
                if (useEventloopDirectly) {
                    reactor.eventloop().offer(task, handle);
                } else {
                    reactor.offer(task, handle);
                }
            }
        });

        latch.await();
    }

    private static class Job implements Runnable {
        private final CountDownLatch latch;
        private final Eventloop eventloop;
        private final boolean useEventloopDirectly;
        private final Reactor reactor;
        private final long operations;
        private final TaskGroupHandle taskGroupHandle;
        private long iteration = 0;

        public Job(Reactor reactor, TaskGroupHandle taskGroupHandle, long operations, CountDownLatch latch, boolean useEventloopDirectly) {
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
}
