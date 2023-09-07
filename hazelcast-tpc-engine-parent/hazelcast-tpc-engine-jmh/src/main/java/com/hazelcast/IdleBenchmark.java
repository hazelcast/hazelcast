package com.hazelcast;

import com.hazelcast.internal.tpcengine.util.EpochClock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Threads(value = 1)
public class IdleBenchmark {

    @Benchmark
    public void yield() {
        Thread.yield();
    }

    @Benchmark
    public void parkNanos0() {
        LockSupport.parkNanos(1);
    }

    @Benchmark
    public void onSpinWait(){
        Thread.onSpinWait();
    }
}
