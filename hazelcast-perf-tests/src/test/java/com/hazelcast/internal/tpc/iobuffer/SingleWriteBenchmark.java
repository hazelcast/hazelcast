package com.hazelcast.internal.tpc.iobuffer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class SingleWriteBenchmark {
    public static final int RANDOM_BYTES_SIZE = 1024;

    private final IOBufferAllocator<ThreadLocalIOBuffer> allocator = IOBufferAllocatorFactory.createGrowingThreadLocal();
    private final byte[] randomBytes = new byte[RANDOM_BYTES_SIZE];

    @Param({"true", "false"})
    public boolean requireAllocation;

    @Param("100")
    public int iterations;

    @Setup(Level.Invocation)
    public void setup() {
        if (!requireAllocation) {
            allocator.free(allocator.allocate(RANDOM_BYTES_SIZE * iterations));
        }
    }

    @Benchmark
    @Fork(value = 2, warmups = 2)
    @Warmup(iterations = 2)
    @Measurement(iterations = 2)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void testSingleWrite() {
        ThreadLocalIOBuffer buffer = allocator.allocate();
        for (int i = 0; i < iterations; i++) {
            buffer.writeBytes(randomBytes);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SingleWriteBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
