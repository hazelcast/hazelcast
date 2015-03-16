package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
public class AtomicLongBenchmark {
    public static final int ITERATIONS = 1000 * 1000 * 10;
    private HazelcastInstance hz;
    private IAtomicLong counter;

    @Setup
    public void setup() {
        Hazelcast.shutdownAll();
        hz = Hazelcast.newHazelcastInstance();
        counter = hz.getAtomicLong("x");
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Benchmark
    public void benchmarkFoo(Blackhole fox) {
        IAtomicLong counter = this.counter;
        counter.set(2);
        for (int k = 0; k < ITERATIONS; k++) {
            long value = counter.get();
            if (value != 2) {
                throw new RuntimeException();
            }
            if (fox != null) {
                fox.consume(value);
            }
        }
    }

    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
                .include(".*")
                .warmupIterations(2)
                .operationsPerInvocation(ITERATIONS)
                .measurementIterations(5)
                .jvmArgs("-server")
                .forks(1)
                .build();

        new Runner(opts).run();
//
//        AtomicLongBenchmark benchmark = new AtomicLongBenchmark();
//        benchmark.setup();
//        benchmark.benchmarkFoo(null);
    }
}