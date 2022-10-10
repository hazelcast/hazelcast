package com.hazelcast.iouring;


import com.hazelcast.internal.tpc.Reactor;
import com.hazelcast.internal.tpc.iouring.IOUringReactorBuilder;
import com.hazelcast.internal.tpc.util.CircularQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
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
//@Fork(value = 1, jvmArgs = {"-XX:+UnlockDiagnosticVMOptions","-XX:+DebugNonSafepoints"})
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@Threads(value = 1)
public class IOUringEventloopBenchmark {

    public static final int OPERATIONS = 100 * 1000 * 1000;
    private static final int concurrency = 10;
    private Reactor reactor;

    @Setup
    public void setup() {
        IOUringReactorBuilder reactorBuilder = new IOUringReactorBuilder();
        //reactorBuilder.setClockRefreshPeriod(-1);
        reactorBuilder.setBatchSize(512);// 1024 gives very good performance
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
    public void offer() throws InterruptedException {
        benchmark(TheMethod.offer);
    }

    @Benchmark
    @OperationsPerInvocation(value = OPERATIONS)
    public void unsafeOffer() throws InterruptedException {
        benchmark(TheMethod.unsafeOffer);
    }

    public void benchmark(TheMethod theMethod) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(concurrency);
        reactor.execute(() -> {
            for (int k = 0; k < concurrency; k++) {
                Task task = new Task(reactor, OPERATIONS / concurrency, latch, theMethod);
                reactor.eventloop().localTaskQueue.add(task);
            }
        });

        latch.await();
    }

    @Benchmark
    @OperationsPerInvocation(value = OPERATIONS)
    public void unsafe_offer_with_pendingScheduledTask() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(concurrency);
        reactor.execute(() -> {
            // schedule task far in the future.
            reactor.eventloop().schedule(() -> {
            }, 1000, SECONDS);

            for (int k = 0; k < concurrency; k++) {
                Task task = new Task(reactor, OPERATIONS / concurrency, latch, TheMethod.unsafeOffer);
                reactor.eventloop().localTaskQueue.offer(task);
            }
        });

        latch.await();
    }

//  //  @Benchmark
//    @OperationsPerInvocation(value = OPERATIONS)
//    public void unsafeOffer_pendingScheduledTask() throws InterruptedException {
//        CountDownLatch latch = new CountDownLatch(concurrency);
//        eventloop.execute(() -> {
//            eventloop.unsafe().schedule(() -> {
//            }, 1000, SECONDS);
//
//            for (int k = 0; k < concurrency; k++) {
//                Task task = new Task(eventloop, OPERATIONS / concurrency, latch, true);
//                eventloop.unsafe().offer(task);
//            }
//        });
//
//        latch.await();
//    }

    private static class Task implements Runnable {
        private final CountDownLatch latch;
        private final Reactor reactor;
        private final TheMethod theMethod;
        private final CircularQueue<Runnable> localTaskQueue;
        private long iteration = 0;
        private final long operations;

        public Task(Reactor reactor, long operations, CountDownLatch latch, TheMethod theMethod) {
            this.reactor = reactor;
            this.localTaskQueue = this.reactor.eventloop().localTaskQueue;
            this.operations = operations;
            this.latch = latch;
            this.theMethod = theMethod;
        }

        @Override
        public void run() {
            iteration++;
            if (operations == iteration) {
                latch.countDown();
            } else {
                switch (theMethod) {
                    case offer:
                        if (!reactor.offer(this)) {
                            throw new RuntimeException();
                        }
                        break;
                    case unsafeOffer:
                        //if (!unsafe.offer(this)) {
                        if (!localTaskQueue.offer(this)) {
                            throw new RuntimeException();
                        }
                        break;
                    default:
                        throw new RuntimeException();
                }
            }
        }
    }

    private enum TheMethod {
        offer,
        unsafeOffer
    }
}
