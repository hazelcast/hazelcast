/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.merkletree;

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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 5, jvmArgsAppend = {"-Xms512g", "-Xmx512g"})
@State(Scope.Benchmark)
public class ArrayMerkleTreeBenchmark {
    private static final int HUGE_PRIME = 982455757;
    private static final int PREFILL_COUNT = 100000;

    private int anInt = HUGE_PRIME;

    @Benchmark
    @Fork(value = 5, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
    public void updateAdd_heap_4G(BenchmarkContext context) {
        int anEntry = getAnInt();
        context.merkleTree.updateAdd(anEntry, anEntry);
    }

    @Benchmark
    @Fork(value = 5, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
    public void updateAdd_heap_2G(BenchmarkContext context) {
        int anEntry = getAnInt();
        context.merkleTree.updateAdd(anEntry, anEntry);
    }

    @Benchmark
    @Fork(value = 5, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
    public void updateAdd_heap_1G(BenchmarkContext context) {
        int anEntry = getAnInt();
        context.merkleTree.updateAdd(anEntry, anEntry);
    }

    @Benchmark
    public void updateReplace(PreFilledBenchmarkContext context) {
        int key = getAnInt(PREFILL_COUNT);
        int oldValue = key;
        int newValue = getAnInt();
        context.merkleTree.updateReplace(key, oldValue, newValue);
    }

    @Benchmark
    public void updateRemove(PreFilledBenchmarkContext context) {
        int key = getAnInt(PREFILL_COUNT);
        int value = key;
        context.merkleTree.updateRemove(key, value);
    }

    private int getAnInt() {
        anInt += HUGE_PRIME;
        return anInt;
    }

    private int getAnInt(int max) {
        anInt = (anInt + HUGE_PRIME) % max;
        return anInt;
    }

    @State(Scope.Benchmark)
    public static class BenchmarkContext {
        @Param({"8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"})
        protected int depth;

        protected MerkleTree merkleTree;

        @Setup(Level.Trial)
        public void setUp() {
            merkleTree = new ArrayMerkleTree(depth, 100000);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            System.out.println("Depth: " + depth);
            System.out.println("Footprint: " + merkleTree.footprint() / 1024 + " (KB)");
        }
    }

    @State(Scope.Benchmark)
    public static class PreFilledBenchmarkContext extends BenchmarkContext {

        @Setup(Level.Trial)
        public void setUp() {
            super.setUp();
            for (int i = 0; i < PREFILL_COUNT; i++) {
                int value = i * HUGE_PRIME;
                merkleTree.updateAdd(value, value);
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ArrayMerkleTreeBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                //                .addProfiler(SafepointsProfiler.class)
                //                .addProfiler(LinuxPerfProfiler.class)
                .addProfiler(GCProfiler.class)
                //                .addProfiler(HotspotMemoryProfiler.class)
                //                .verbosity(VerboseMode.SILENT)
                .build();

        new Runner(opt).run();
    }

}
