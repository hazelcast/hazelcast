package com.hazelcast;


import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.EventloopBuilder;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.internal.tpc.nio.NioEventloopBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Threads(value =1 )
public class EventloopBenchmark {

    public static final int OPERATIONS = 100 * 1000 * 1000;
    private static final int concurrency = 10;
    private static final boolean useUnsafe = true;
    private Eventloop eventloop;

    @Setup
    public void setup() {
        EventloopBuilder eventloopBuilder = new NioEventloopBuilder();
        eventloop = eventloopBuilder.create();
        eventloop.start();
    }

    @TearDown
    public void teardown() throws InterruptedException {
        eventloop.shutdown();
        eventloop.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Benchmark
    @OperationsPerInvocation(value = OPERATIONS)
    public void run() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(concurrency);
        eventloop.execute(() -> {
//            eventloop.unsafe().schedule(() -> {
//            }, 1000, SECONDS);

            for (int k = 0; k < concurrency; k++) {
                Task task = new Task(eventloop, OPERATIONS/concurrency, latch, useUnsafe);
                if (useUnsafe) {
                    eventloop.unsafe().offer(task);
                } else {
                    eventloop.offer(task);
                }
            }
        });

        latch.await();
    }

    private static class Task implements Runnable {
        private final CountDownLatch latch;
        private final Eventloop eventloop;
        private final boolean useUnsafe;
        private Eventloop.Unsafe unsafe;
        private long iteration = 0;
        private final long operations;

        public Task(Eventloop eventloop, long operations, CountDownLatch latch, boolean useUnsafe) {
            this.eventloop = eventloop;
            this.unsafe = eventloop.unsafe();
            this.operations = operations;
            this.latch = latch;
            this.useUnsafe = useUnsafe;
        }

        @Override
        public void run() {
            iteration++;
            if (operations == iteration) {
                latch.countDown();
            } else if (useUnsafe) {
                unsafe.offer(this);
            } else {
                eventloop.offer(this);
            }
        }
    }
}
