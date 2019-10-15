/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Benchmark for OAHashSet using Java HashSet implementation as a baseline
 * <p>
 * This benchmark puts the heap under pressure and addition benchmarks
 * may produce different amount of garbage, depending on the performance
 * of OAHashSet and Java HashSet. This makes comparing with the baseline
 * pointless, since the more frequent GC executions distort the results.
 * Due to this, we want to keep GC away from the measurements by either
 * <ul>
 * <li> increasing the heap size big enough to avoid GC
 * <li> or make JMH to run GC between iterations via {@link OptionsBuilder#shouldDoGC}
 * at the cost of significantly increased benchmark execution time.
 * See {@link #main(String[])}
 * </ul>
 * The GC-free benchmark execution can be verified by adding {@link GCProfiler}
 * in {@link #main(String[])}
 */
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 100, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 100, timeUnit = MILLISECONDS)
@Fork(value = 5, jvmArgsAppend = {"-Xms8G", "-Xmx8G"})
@State(Scope.Benchmark)
public class OAHashSetBenchmark {
    // cost factors for simulating expensive equals() and hashcode() methods
    // higher cost factors make the impact of the equals() and hashCode() methods more significant to the
    // benchmark results and highlight the effectiveness of the cached hashes in the OAHash implementation
    // on the other hand, higher cost factors make the test biased in favor of the OAHashSet implementation
    private static final int EQUALS_COST_FACTOR = 10;
    private static final int HASHCODE_COST_FACTOR = 10;

    private static final int HUGE_PRIME = 982455757;
    private static final int OPS_10K = 10 * 1000;
    private static final int OPS_100K = 100 * 1000;
    private static final int OPS_1M = 1000 * 1000;
    private static final int OPS_10M = 10 * 1000 * 1000;

    private int anInt = HUGE_PRIME;
    private List<OAHashSet<?>> setsToKeep = new LinkedList<OAHashSet<?>>();

    @Benchmark
    public boolean add_java_HashSet(JavaHashSetContext context) {
        return context.set.add(getAnEntry());
    }

    @Benchmark
    public boolean add_hz_OAHashSet(OAHashSetContext context) {
        return context.set.add(getAnEntry());
    }

    @Benchmark
    public boolean add_hz_OAHashSet_withHash(OAHashSetContext context) {
        ObjectWithExpensiveHashCodeAndEquals anEntry = getAnEntry();
        return context.set.add(anEntry, anEntry.getHash());
    }

    @Benchmark
    public boolean contains_java_HashSet(PreFilledJavaHashSetContext context) {
        ObjectWithExpensiveHashCodeAndEquals anEntry = getAnEntry(context.size);
        return context.set.contains(anEntry);
    }

    @Benchmark
    public boolean contains_hz_OAHashSet(PreFilledOAHashSetContext context) {
        ObjectWithExpensiveHashCodeAndEquals anEntry = getAnEntry(context.size);
        return context.set.contains(anEntry);
    }

    @Benchmark
    public boolean contains_hz_OAHashSet_withHash(PreFilledOAHashSetContext context) {
        ObjectWithExpensiveHashCodeAndEquals anEntry = getAnEntry(context.size);
        return context.set.contains(anEntry, anEntry.getHash());
    }

    @Benchmark
    public boolean remove_java_HashSet(PreFilledJavaHashSetContext context) {
        ObjectWithExpensiveHashCodeAndEquals anEntry = getAnEntry(context.size);
        return context.set.remove(anEntry);
    }

    @Benchmark
    public boolean remove_hz_OAHashSet(PreFilledOAHashSetContext context) {
        ObjectWithExpensiveHashCodeAndEquals anEntry = getAnEntry(context.size);
        return context.set.remove(anEntry);
    }

    @Benchmark
    public boolean remove_hz_OAHashSet_withHash(PreFilledOAHashSetContext context) {
        ObjectWithExpensiveHashCodeAndEquals anEntry = getAnEntry(context.size);
        return context.set.remove(anEntry, anEntry.getHash());
    }

    @Benchmark
    public void clear(PreFilledOAHashSetContext context) {
        context.set.clear();
    }

    @Benchmark
    @Measurement(iterations = 1, batchSize = OPS_10K)
    @BenchmarkMode({Mode.SingleShotTime})
    @OutputTimeUnit(MILLISECONDS)
    @Fork(jvmArgsAppend = {"-Xms1G", "-Xmx1G"})
    public OAHashSet<Integer> instanceCreation_case1_10K() {
        return createOaHashSet();
    }

    @Benchmark
    @Measurement(iterations = 1, batchSize = OPS_100K)
    @BenchmarkMode({Mode.SingleShotTime})
    @Fork(jvmArgsAppend = {"-Xms1G", "-Xmx1G"})
    @OutputTimeUnit(MILLISECONDS)
    public OAHashSet<Integer> instanceCreation_case2_100K() {
        return createOaHashSet();
    }

    @Benchmark
    @Measurement(iterations = 1, batchSize = OPS_1M)
    @BenchmarkMode({Mode.SingleShotTime})
    @Fork(jvmArgsAppend = {"-Xms1G", "-Xmx1G"})
    @OutputTimeUnit(MILLISECONDS)
    public OAHashSet<Integer> instanceCreation_case3_1M() {
        return createOaHashSet();
    }

    @Benchmark
    @Measurement(iterations = 1, batchSize = OPS_10M)
    @BenchmarkMode({Mode.SingleShotTime})
    @Fork(jvmArgsAppend = {"-Xms2G", "-Xmx2G"})
    @OutputTimeUnit(MILLISECONDS)
    public OAHashSet<Integer> instanceCreation_case4_10M() {
        return createOaHashSet();
    }

    private OAHashSet<Integer> createOaHashSet() {
        // keeping a live set of objects so that GC has some realistic work to do
        OAHashSet<Integer> oaHashSet = new OAHashSet<Integer>(3);
        setsToKeep.add(oaHashSet);
        return oaHashSet;
    }

    private int getAnInt() {
        anInt += HUGE_PRIME;
        return anInt;
    }

    private int getAnInt(int max) {
        return getAnInt() % max;
    }

    private ObjectWithExpensiveHashCodeAndEquals getAnEntry() {
        return new ObjectWithExpensiveHashCodeAndEquals(getAnInt(), EQUALS_COST_FACTOR, HASHCODE_COST_FACTOR);
    }

    private ObjectWithExpensiveHashCodeAndEquals getAnEntry(int maxInt) {
        return new ObjectWithExpensiveHashCodeAndEquals(getAnInt(maxInt), EQUALS_COST_FACTOR, HASHCODE_COST_FACTOR);
    }

    public abstract static class BenchmarkContext {
        @Param({"10", "100", "1000", "10000", "100000", "1000000"})
        protected int size;

        protected int getValue(int base) {
            return base * HUGE_PRIME % size;
        }
    }

    @State(Scope.Benchmark)
    public static class OAHashSetContext extends BenchmarkContext {
        protected OAHashSet<ObjectWithExpensiveHashCodeAndEquals> set;

        @Setup(Level.Trial)
        public void setUp() {
            set = new OAHashSet<ObjectWithExpensiveHashCodeAndEquals>(size);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            //            System.out.println("OAHashSet.size: " + set.size());
            //            System.out.println("OAHashSet.capacity: " + set.capacity());
            //            System.out.println("OAHashSet.footprint: " + set.footprint());
        }
    }

    @State(Scope.Benchmark)
    public static class PreFilledOAHashSetContext extends OAHashSetContext {

        @Setup(Level.Trial)
        public void setUp() {
            super.setUp();
            for (int i = 0; set.size() < size * 0.8F; i++) {
                int value = getValue(i);
                set.add(new ObjectWithExpensiveHashCodeAndEquals(value, EQUALS_COST_FACTOR, HASHCODE_COST_FACTOR), value);
            }
            //            System.out.println("Prefilled to size " + set.size() + " for initial capacity " + size);
        }
    }

    @State(Scope.Benchmark)
    public static class JavaHashSetContext extends BenchmarkContext {
        protected HashSet<ObjectWithExpensiveHashCodeAndEquals> set;

        @Setup(Level.Trial)
        public void setUp() {
            set = new HashSet<ObjectWithExpensiveHashCodeAndEquals>(size);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            //            System.out.println("HashSet.size: " + set.size());
        }
    }

    @State(Scope.Benchmark)
    public static class PreFilledJavaHashSetContext extends JavaHashSetContext {

        @Setup(Level.Trial)
        public void setUp() {
            super.setUp();
            for (int i = 0; set.size() < size * 0.8F; i++) {
                int value = getValue(i);
                set.add(new ObjectWithExpensiveHashCodeAndEquals(value, EQUALS_COST_FACTOR, HASHCODE_COST_FACTOR));
            }
            //            System.out.println("Prefilled to size " + set.size() + " for initial capacity " + size);
        }
    }

    private static class ObjectWithExpensiveHashCodeAndEquals {
        private final int hash;
        private final Integer value;
        private final String valueAsStr;
        private final String sqrtAsStr;
        private final int equalsCostFactor;
        private final int hashCodeCostFactor;

        private ObjectWithExpensiveHashCodeAndEquals(int value, int equalsCostFactor, int hashCodeCostFactor) {
            this.value = value;
            this.valueAsStr = "Lorem ipsum dolor sit amet, consectetur adipiscing elit" + value;
            this.sqrtAsStr = Double.toString(Math.sqrt(value));
            this.equalsCostFactor = equalsCostFactor;
            this.hashCodeCostFactor = hashCodeCostFactor;
            this.hash = hashCodeInternal();
        }

        private int getHash() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            boolean equals = false;

            for (int i = 0; i < equalsCostFactor; i++) {
                equals = equalsInternal(o);
            }

            return equals;
        }

        private boolean equalsInternal(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ObjectWithExpensiveHashCodeAndEquals that = (ObjectWithExpensiveHashCodeAndEquals) o;

            if (value != null ? !value.equals(that.value) : that.value != null) {
                return false;
            }
            if (valueAsStr != null ? !valueAsStr.equals(that.valueAsStr) : that.valueAsStr != null) {
                return false;
            }
            return sqrtAsStr != null ? sqrtAsStr.equals(that.sqrtAsStr) : that.sqrtAsStr == null;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;

            for (int i = 0; i < hashCodeCostFactor; i++) {
                hashCode = hashCodeInternal();
            }

            return hashCode;
        }

        private int hashCodeInternal() {
            int result = value != null ? value.hashCode() : 0;
            result = 31 * result + (valueAsStr != null ? valueAsStr.hashCode() : 0);
            result = 31 * result + (sqrtAsStr != null ? sqrtAsStr.hashCode() : 0);
            return result;
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OAHashSetBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                //                .addProfiler(GCProfiler.class)
                //                .addProfiler(LinuxPerfProfiler.class)
                //                .addProfiler(HotspotMemoryProfiler.class)
                //                .shouldDoGC(true)
                //                .verbosity(VerboseMode.SILENT)
                .build();

        new Runner(opt).run();
    }

}
