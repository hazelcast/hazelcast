/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.benchmarks;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-atomiclong")
@BenchmarkHistoryChart(filePrefix = "benchmark-atomiclong-history", labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class AtomicLongBenchmark {
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    private static HazelcastInstance hazelcastInstance;
    private IAtomicLong atomicLong;

    @BeforeClass
    public static void beforeClass() {
        hazelcastInstance = Hazelcast.newHazelcastInstance();
    }

    @Before
    public void before() {
        atomicLong = hazelcastInstance.getAtomicLong("atomicLong");
    }

    @After
    public void after() {
        atomicLong.destroy();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void get() throws Exception {
        long startMs = System.currentTimeMillis();
        int iterations = 1000000;
        for (int k = 0; k < iterations; k++) {
            atomicLong.get();
            if (k % 100000 == 0) {
                System.out.println("at " + k);
            }
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (iterations * 1000d) / durationMs;
        System.out.println("Performance: " + performance);
    }

    public static void main(String[] args) throws Exception {
        AtomicLongBenchmark.beforeClass();
        AtomicLongBenchmark benchmark = new AtomicLongBenchmark();
        benchmark.before();
        benchmark.get();
        benchmark.after();
        AtomicLongBenchmark.afterClass();
    }

    @Test
    public void set() throws Exception {
        for (int k = 0; k < 500000; k++) {
            atomicLong.set(k);
        }
    }
}
