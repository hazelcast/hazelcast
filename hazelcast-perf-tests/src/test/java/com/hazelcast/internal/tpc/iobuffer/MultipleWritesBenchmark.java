/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Stats for requireAllocation
 *          40 768,58 msec task-clock                #    1,001 CPUs utilized
 *              6 738      context-switches          #  165,274 /sec
 *                194      cpu-migrations            #    4,759 /sec
 *             49 490      page-faults               #    1,214 K/sec
 *    136 651 489 476      cycles                    #    3,352 GHz                      (31,01%)
 *    137 070 252 035      instructions              #    1,00  insn per cycle           (38,77%)
 *     24 875 086 768      branches                  #  610,153 M/sec                    (38,85%)
 *         68 233 030      branch-misses             #    0,27% of all branches          (38,87%)
 *     38 414 268 162      L1-dcache-loads           #  942,252 M/sec                    (38,81%)
 *      8 852 814 785      L1-dcache-load-misses     #   23,05% of all L1-dcache accesses  (38,73%)
 *         64 001 313      LLC-loads                 #    1,570 M/sec                    (30,84%)
 *         22 182 688      LLC-load-misses           #   34,66% of all LL-cache accesses  (30,78%)
 *         64 306 340      L1-icache-load-misses                                         (30,74%)
 *     38 327 130 893      dTLB-loads                #  940,114 M/sec                    (30,69%)
 *          3 511 578      dTLB-load-misses          #    0,01% of all dTLB cache accesses  (30,72%)
 *          1 748 594      iTLB-loads                #   42,891 K/sec                    (30,71%)
 *          1 805 409      iTLB-load-misses          #  103,25% of all iTLB cache accesses  (30,82%)
 *
 * Stats for !requireAllocation:
 *         40 753,23 msec task-clock                #    1,001 CPUs utilized
 *              6 321      context-switches          #  155,104 /sec
 *                187      cpu-migrations            #    4,589 /sec
 *             29 143      page-faults               #  715,109 /sec
 *    133 347 902 705      cycles                    #    3,272 GHz                      (31,03%)
 *    320 571 238 376      instructions              #    2,40  insn per cycle           (38,76%)
 *     56 919 879 597      branches                  #    1,397 G/sec                    (38,66%)
 *        117 439 424      branch-misses             #    0,21% of all branches          (38,68%)
 *     88 086 612 593      L1-dcache-loads           #    2,161 G/sec                    (38,70%)
 *     18 374 477 887      L1-dcache-load-misses     #   20,86% of all L1-dcache accesses  (38,61%)
 *         10 948 353      LLC-loads                 #  268,650 K/sec                    (30,76%)
 *            613 680      LLC-load-misses           #    5,61% of all LL-cache accesses  (30,71%)
 *         62 149 266      L1-icache-load-misses                                         (30,73%)
 *     88 384 214 088      dTLB-loads                #    2,169 G/sec                    (30,74%)
 *            555 727      dTLB-load-misses          #    0,00% of all dTLB cache accesses  (30,92%)
 *          3 477 137      iTLB-loads                #   85,322 K/sec                    (30,97%)
 *          1 504 441      iTLB-load-misses          #   43,27% of all iTLB cache accesses  (30,99%)
 */
@State(Scope.Benchmark)
public class MultipleWritesBenchmark {
    public static final int RANDOM_BYTES_SIZE = 1024;

    private final IOBufferAllocator<ThreadLocalIOBuffer> allocator = IOBufferAllocatorFactory.createGrowingThreadLocal();
    private final byte[] randomBytes = new byte[RANDOM_BYTES_SIZE];
    private List<ThreadLocalIOBuffer> garbageAllocators = new ArrayList<>();

    @Param({"true", "false"})
    public boolean requireAllocation;

    @Param("100")
    public int iterationsPerCycle;

    @Param({"1000"})
    public int iterationCount;

    @Setup(Level.Invocation)
    public void setup() {
        garbageAllocators = new ArrayList<>(iterationCount);
        if (!requireAllocation) {
            allocator.free(allocator.allocate(RANDOM_BYTES_SIZE * iterationsPerCycle));
        }
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        garbageAllocators.forEach(allocator::free);
    }

    @Benchmark
    @Fork(value = 2, warmups = 2)
    @Warmup(iterations = 2)
    @Measurement(iterations = 2)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void testMultipleWrites() {
        for (int j = 0; j < iterationCount; j++) {
            ThreadLocalIOBuffer buffer = allocator.allocate();

            for (int i = 0; i < iterationsPerCycle; i++) {
                buffer.writeBytes(randomBytes);
            }

            if (!requireAllocation) {
                allocator.free(buffer);
            } else {
                garbageAllocators.add(buffer);
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MultipleWritesBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
