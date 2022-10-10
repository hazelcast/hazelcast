package com.hazelcast;


import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.Reactor;
import com.hazelcast.internal.tpc.ReactorBuilder;
import com.hazelcast.internal.tpc.nio.NioReactorBuilder;
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

import static java.util.concurrent.TimeUnit.SECONDS;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Threads(value = 1)
public class EventloopBenchmark {

    public static final int OPERATIONS = 100 * 1000 * 1000;
    private static final int concurrency = 10;
    private static final boolean useEventloopDirectly = true;
    private Reactor reactor;

    @Setup
    public void setup() {
        ReactorBuilder reactorBuilder = new NioReactorBuilder();
        //Eventloop.Configuration reactorBuilder = new NioEventloop.NioConfiguration();
        reactorBuilder.setBatchSize(16);
        reactorBuilder.setClockRefreshPeriod(16);
        reactor = reactorBuilder.build();
        reactor.start();
    }

    @TearDown
    public void teardown() throws InterruptedException {
        reactor.shutdown();
        reactor.awaitTermination(5, SECONDS);
    }

    @Benchmark
    @OperationsPerInvocation(value = OPERATIONS)
    public void run() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(concurrency);
        reactor.execute(() -> {
//            eventloop.unsafe().schedule(() -> {
//            }, 1000, SECONDS);

            for (int k = 0; k < concurrency; k++) {
                Task task = new Task(reactor, OPERATIONS / concurrency, latch, useEventloopDirectly);
                if (useEventloopDirectly) {
                    reactor.eventloop().localTaskQueue.offer(task);
                } else {
                    reactor.offer(task);
                }
            }
        });

        latch.await();
    }

    private static class Task implements Runnable {
        private final CountDownLatch latch;
        private final Eventloop eventloop;
        private final boolean useEventloopDirectly;
        private final Reactor reactor;
        private final long operations;
        private long iteration = 0;

        public Task(Reactor reactor, long operations, CountDownLatch latch, boolean useEventloopDirectly) {
            this.reactor = reactor;
            this.eventloop = reactor.eventloop();
            this.operations = operations;
            this.latch = latch;
            this.useEventloopDirectly = useEventloopDirectly;
        }

        @Override
        public void run() {
            iteration++;
            if (operations == iteration) {
                latch.countDown();
            } else if (useEventloopDirectly) {
                eventloop.localTaskQueue.offer(this);
            } else {
                reactor.offer(this);
            }
        }
    }
}
