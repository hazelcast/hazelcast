package com.hazelcast.internal.tpcengine.iouring;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Threads(value = 1)
public class EnterBenchmark {
    @Param({"true", "false"})
    public boolean registerRingFd;

    private Uring uring;
    private SubmissionQueue sq;

    @Setup
    public void setup() {
        this.uring = new Uring(4096, 0);
        if (registerRingFd) {
            uring.registerRingFd();
        }
        this.sq = uring.submissionQueue();
    }

    @Benchmark
    public void benchmark() {
        sq.enter(0, 0, 0);
    }
}
