/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.bitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Random;

@State(Scope.Benchmark)
public class SparseBitSetIterateBenchmark {

    private static final int SIZE = 1000000;
    private static final long MEMBER_MASK = 0x00000000000FFFFFL;

    private final SparseBitSet bitSet = new SparseBitSet();
    private final Roaring64NavigableMap roaringBitmap = new Roaring64NavigableMap();

    private AscendingLongIterator iterator;
    private LongIterator iteratorRoaring;

    @Setup
    public void setup() {
        Random random = new Random(404);
        for (int i = 0; i < SIZE; ++i) {
            long v = random.nextLong() & MEMBER_MASK;
            bitSet.add(v);
        }
        iterator = bitSet.iterator();

        random = new Random(404);
        for (int i = 0; i < SIZE; ++i) {
            long v = random.nextLong() & MEMBER_MASK;
            roaringBitmap.addLong(v);
        }

        iteratorRoaring = roaringBitmap.getLongIterator();

        System.gc();
    }

    @Benchmark
    public long iterate() {
        long member = iterator.advance();
        if (member == AscendingLongIterator.END) {
            iterator = bitSet.iterator();
            member = iterator.advance();
        }
        return member;
    }

    @Benchmark
    public long iterateRoaring() {
        if (!iteratorRoaring.hasNext()) {
            iteratorRoaring = roaringBitmap.getLongIterator();
        }
        return iteratorRoaring.next();
    }

    public static void main(String[] args) throws RunnerException {
        // @formatter:off
        Options opt = new OptionsBuilder()
                .include(SparseBitSetIterateBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .warmupTime(TimeValue.seconds(1))
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(1))
                .addProfiler(GCProfiler.class)
                .forks(1)
                .threads(1)
                .build();
        // @formatter:on

        new Runner(opt).run();
    }

}
