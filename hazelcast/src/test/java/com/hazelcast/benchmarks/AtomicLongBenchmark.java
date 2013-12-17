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
import com.hazelcast.concurrent.atomiclong.AtomicLongBaseOperation;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.*;
import org.junit.rules.TestRule;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-atomiclong")
@BenchmarkHistoryChart(filePrefix = "benchmark-atomiclong-history", labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class AtomicLongBenchmark extends HazelcastTestSupport{
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    private static HazelcastInstance hazelcastInstance;
    private AtomicLongProxy atomicLong;

    @BeforeClass
    public static void beforeClass() {
        hazelcastInstance = Hazelcast.newHazelcastInstance();
    }

    @Before
    public void before() {
        atomicLong = (AtomicLongProxy)hazelcastInstance.getAtomicLong("atomicLong");
    }

    @After
    public void after() {
        atomicLong.destroy();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }


    public class NoResponseOperation extends AtomicLongBaseOperation {

        public NoResponseOperation() {
        }

        public NoResponseOperation(String name) {
            super(name);
        }

        @Override
        public void run() throws Exception {
            getNumber().get();
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }
    }

    @Test
    public void noResponse() throws Exception {
        long startMs = System.currentTimeMillis();
        int iterations = 50*1000*1000;

        OperationService opService = getNode(hazelcastInstance).nodeEngine.getOperationService();
        for (int k = 0; k < iterations; k++) {
            NoResponseOperation no = new NoResponseOperation(atomicLong.getName());
            no.setPartitionId(atomicLong.getPartitionId());
            no.setService(atomicLong.getService());
            opService.invokeOnPartition(AtomicLongService.SERVICE_NAME,no,atomicLong.getPartitionId());

            if (k % 200000 == 0) {
                System.out.println("at " + k);
            }
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (iterations * 1000d) / durationMs;
        System.out.println("Performance: " + String.format("%1$,.2f", performance));
    }

    @Test
    public void get() throws Exception {
        long startMs = System.currentTimeMillis();
        int iterations = 50*1000*1000;
        for (int k = 0; k < iterations; k++) {
            atomicLong.get();
            if (k % 200000 == 0) {
                System.out.println("at " + k);
            }
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (iterations * 1000d) / durationMs;
        System.out.println("Performance: " + String.format("%1$,.2f", performance));
    }

    @Test
    public void getMultipleThreads() throws Exception {
        long startMs = System.currentTimeMillis();
        int threadCount = 2;

        GetThread[] threads = new GetThread[threadCount];
        int iterations = 20000;
        for (int k = 0; k < threads.length; k++) {
            threads[k] = new GetThread(iterations);
            threads[k].start();
        }

        for (GetThread t : threads) {
            t.join();
        }

        long durationMs = System.currentTimeMillis() - startMs;
        double performancePerThread = (iterations * 1000d) / durationMs;
        System.out.println("Performance per thread: " + performancePerThread + " ops/second");
        System.out.println("Performance: " + performancePerThread * threadCount + " ops/second");
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

    private class GetThread extends Thread {
        final int iterations;

        private GetThread(int iterations) {
            this.iterations = iterations;
        }

        public void run() {
            for (int k = 0; k < iterations; k++) {
                atomicLong.get();
                if (k % 1000 == 0) {
                    System.out.println(getName() + "at " + k);
                }
            }
        }
    }
}
