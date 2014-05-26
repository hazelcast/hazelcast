/*
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

package com.hazelcast.mapreduce;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.MultiMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class MapReduceTest
        extends HazelcastTestSupport {

    private static final String MAP_NAME = "default";

    @Test(timeout = 30000)
    public void testPartitionPostpone()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(3, h1.getCluster().getMembers().size());
            }
        });

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        KeyValueSource<Integer, Integer> kvs = KeyValueSource.fromMap(m1);
        KeyValueSource<Integer, Integer> wrapper = new MapKeyValueSourceAdapter<Integer, Integer>(kvs);
        Job<Integer, Integer> job = tracker.newJob(wrapper);
        ICompletableFuture<Map<String, List<Integer>>> future = job.mapper(new TestMapper()).submit();

        Map<String, List<Integer>> result = future.get();

        assertEquals(100, result.size());
        for (List<Integer> value : result.values()) {
            assertEquals(1, value.size());
        }
    }

    @Test(timeout = 30000, expected = ExecutionException.class)
    public void testExceptionDistributionWithCollator()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(3, h1.getCluster().getMembers().size());
            }
        });

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future = job.mapper(new ExceptionThrowingMapper())
                                                                   .submit(new Collator<Map.Entry<String, List<Integer>>, Map<String, List<Integer>>>() {
                                                                       @Override
                                                                       public Map<String, List<Integer>> collate(
                                                                               Iterable<Map.Entry<String, List<Integer>>> values) {
                                                                           return null;
                                                                       }
                                                                   });

        try {
            Map<String, List<Integer>> result = future.get();
            fail();

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getCause() instanceof NullPointerException);
            throw e;
        }
    }

    @Test(timeout = 30000, expected = ExecutionException.class)
    public void testExceptionDistribution()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(3, h1.getCluster().getMembers().size());
            }
        });

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future = job.mapper(new ExceptionThrowingMapper()).submit();

        try {
            Map<String, List<Integer>> result = future.get();
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getCause() instanceof NullPointerException);
            throw e;
        }
    }

    @Test(timeout = 30000, expected = CancellationException.class)
    public void testInProcessCancellation()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(3, h1.getCluster().getMembers().size());
            }
        });

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future = job.mapper(new TimeConsumingMapper()).submit();

        future.cancel(true);

        try {
            Map<String, List<Integer>> result = future.get();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test(timeout = 30000)
    public void testMapper()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(3, h1.getCluster().getMembers().size());
            }
        });

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future = job.mapper(new TestMapper()).submit();

        Map<String, List<Integer>> result = future.get();

        assertEquals(100, result.size());
        for (List<Integer> value : result.values()) {
            assertEquals(1, value.size());
        }
    }

    @Test(timeout = 30000)
    public void testKeyedMapperCollator()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 10000; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future = job.onKeys(50).mapper(new TestMapper()).submit(new GroupingTestCollator());

        int result = future.get();

        assertEquals(50, result);
    }

    @Test(timeout = 30000)
    public void testKeyPredicateMapperCollator()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 10000; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future = job.keyPredicate(new TestKeyPredicate()).mapper(new TestMapper())
                                                .submit(new GroupingTestCollator());

        int result = future.get();

        assertEquals(50, result);
    }

    @Test(timeout = 30000)
    public void testMapperComplexMapping()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future = job.mapper(new GroupingTestMapper(2)).submit();

        Map<String, List<Integer>> result = future.get();

        assertEquals(1, result.size());
        assertEquals(25, result.values().iterator().next().size());
    }

    @Test(timeout = 30000)
    public void testMapperReducer()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, Integer>> future = job.mapper(new GroupingTestMapper()).reducer(new TestReducerFactory())
                                                             .submit();

        Map<String, Integer> result = future.get();

        // Precalculate results
        int[] expectedResults = new int[4];
        for (int i = 0; i < 100; i++) {
            int index = i % 4;
            expectedResults[index] += i;
        }

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResults[i], (int) result.get(String.valueOf(i)));
        }
    }

    @Test(timeout = 60000)
    public void testMapperReducerChunked()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(3, h1.getCluster().getMembers().size());
            }
        });

        final IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 10000; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        JobCompletableFuture<Map<String, Integer>> future = job.chunkSize(10).mapper(new GroupingTestMapper())
                                                               .reducer(new TestReducerFactory()).submit();

        final TrackableJob trackableJob = tracker.getTrackableJob(future.getJobId());
        final JobProcessInformation processInformation = trackableJob.getJobProcessInformation();
        Map<String, Integer> result = future.get();

        // Precalculate results
        int[] expectedResults = new int[4];
        for (int i = 0; i < 10000; i++) {
            int index = i % 4;
            expectedResults[index] += i;
        }

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResults[i], (int) result.get(String.valueOf(i)));
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                if (processInformation.getProcessedRecords() < 10000) {
                    System.err.println(processInformation.getProcessedRecords());
                }
                assertEquals(10000, processInformation.getProcessedRecords());
            }
        });
    }

    @Test(timeout = 30000)
    public void testMapperCollator()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future = job.mapper(new GroupingTestMapper()).submit(new GroupingTestCollator());

        int result = future.get();

        // Precalculate result
        int expectedResult = 0;
        for (int i = 0; i < 100; i++) {
            expectedResult += i;
        }

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResult, result);
        }
    }

    @Test(timeout = 30000)
    public void testMapperReducerCollator()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future = job.mapper(new GroupingTestMapper()).reducer(new TestReducerFactory())
                                                .submit(new TestCollator());

        int result = future.get();

        // Precalculate result
        int expectedResult = 0;
        for (int i = 0; i < 100; i++) {
            expectedResult += i;
        }

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResult, result);
        }
    }

    @Test(timeout = 30000)
    public void testAsyncMapper()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, List<Integer>> listenerResults = new HashMap<String, List<Integer>>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future = job.mapper(new TestMapper()).submit();

        future.andThen(new ExecutionCallback<Map<String, List<Integer>>>() {
            @Override
            public void onResponse(Map<String, List<Integer>> response) {
                try {
                    listenerResults.putAll(response);
                } finally {
                    semaphore.release();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                semaphore.release();
            }
        });

        semaphore.acquire();

        assertEquals(100, listenerResults.size());
        for (List<Integer> value : listenerResults.values()) {
            assertEquals(1, value.size());
        }
    }

    @Test(timeout = 30000)
    public void testKeyedAsyncMapper()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, List<Integer>> listenerResults = new HashMap<String, List<Integer>>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future = job.onKeys(50).mapper(new TestMapper()).submit();

        future.andThen(new ExecutionCallback<Map<String, List<Integer>>>() {
            @Override
            public void onResponse(Map<String, List<Integer>> response) {
                try {
                    listenerResults.putAll(response);
                } finally {
                    semaphore.release();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                semaphore.release();
            }
        });

        semaphore.acquire();

        assertEquals(1, listenerResults.size());
        for (List<Integer> value : listenerResults.values()) {
            assertEquals(1, value.size());
        }
    }

    @Test(timeout = 30000)
    public void testAsyncMapperReducer()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, Integer> listenerResults = new HashMap<String, Integer>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, Integer>> future = job.mapper(new GroupingTestMapper()).reducer(new TestReducerFactory())//
                .submit();

        future.andThen(new ExecutionCallback<Map<String, Integer>>() {
            @Override
            public void onResponse(Map<String, Integer> response) {
                try {
                    listenerResults.putAll(response);
                } finally {
                    semaphore.release();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                semaphore.release();
            }
        });

        // Precalculate results
        int[] expectedResults = new int[4];
        for (int i = 0; i < 100; i++) {
            int index = i % 4;
            expectedResults[index] += i;
        }

        semaphore.acquire();

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResults[i], (int) listenerResults.get(String.valueOf(i)));
        }
    }

    @Test(timeout = 30000)
    public void testAsyncMapperCollator()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future = job.mapper(new GroupingTestMapper()).submit(new GroupingTestCollator());

        future.andThen(new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                try {
                    result[0] = response.intValue();
                } finally {
                    semaphore.release();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                semaphore.release();
            }
        });

        // Precalculate result
        int expectedResult = 0;
        for (int i = 0; i < 100; i++) {
            expectedResult += i;
        }

        semaphore.acquire();

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResult, result[0]);
        }
    }

    @Test(timeout = 30000)
    public void testAsyncMapperReducerCollator()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future = job.mapper(new GroupingTestMapper()).reducer(new TestReducerFactory())
                                                .submit(new TestCollator());

        future.andThen(new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                try {
                    result[0] = response.intValue();
                } finally {
                    semaphore.release();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                semaphore.release();
            }
        });

        // Precalculate result
        int expectedResult = 0;
        for (int i = 0; i < 100; i++) {
            expectedResult += i;
        }

        semaphore.acquire();

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResult, result[0]);
        }
    }

    @Test(timeout = 30000)
    public void testDataSerializableIntermediateObject()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker jobTracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = jobTracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future = job.mapper(new TestMapper())
                                                .combiner(new DataSerializableIntermediateCombinerFactory())
                                                .reducer(new DataSerializableIntermediateReducerFactory())
                                                .submit(new DataSerializableIntermediateCollator());

        // Precalculate result
        int expectedResult = 0;
        for (int i = 0; i < 100; i++) {
            expectedResult += i;
        }

        assertEquals(expectedResult, (int) future.get());
    }

    public static class TupleIntInt
            implements DataSerializable {

        private int count;
        private int amount;

        public TupleIntInt() {
        }

        public TupleIntInt(int count, int amount) {
            this.count = count;
            this.amount = amount;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {

            out.writeInt(count);
            out.writeInt(amount);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {

            count = in.readInt();
            amount = in.readInt();
        }
    }

    public static class DataSerializableIntermediateCombinerFactory
            implements CombinerFactory<String, Integer, TupleIntInt> {

        @Override
        public Combiner<String, Integer, TupleIntInt> newCombiner(String key) {
            return new DataSerializableIntermediateCombiner();
        }
    }

    public static class DataSerializableIntermediateCombiner
            extends Combiner<String, Integer, TupleIntInt> {

        private int count;
        private int amount;

        @Override
        public void combine(String key, Integer value) {
            count++;
            amount += value;
        }

        @Override
        public TupleIntInt finalizeChunk() {
            int count = this.count;
            int amount = this.amount;
            this.count = 0;
            this.amount = 0;
            return new TupleIntInt(count, amount);
        }
    }

    public static class DataSerializableIntermediateReducerFactory
            implements ReducerFactory<String, TupleIntInt, TupleIntInt> {

        @Override
        public Reducer<String, TupleIntInt, TupleIntInt> newReducer(String key) {
            return new DataSerializableIntermediateReducer();
        }
    }

    public static class DataSerializableIntermediateReducer
            extends Reducer<String, TupleIntInt, TupleIntInt> {

        private int count;
        private int amount;

        @Override
        public void reduce(TupleIntInt value) {
            count += value.count;
            amount += value.amount;
        }

        @Override
        public TupleIntInt finalizeReduce() {
            return new TupleIntInt(count, amount);
        }
    }

    public static class DataSerializableIntermediateCollator
            implements Collator<Map.Entry<String, TupleIntInt>, Integer> {

        @Override
        public Integer collate(Iterable<Map.Entry<String, TupleIntInt>> values) {
            int count = 0;
            int amount = 0;
            for (Map.Entry<String, TupleIntInt> value : values) {
                TupleIntInt tuple = value.getValue();
                count += tuple.count;
                amount += tuple.amount;
            }
            return (int) ((double) amount / count);
        }
    }

    public static class ExceptionThrowingMapper
            implements Mapper<Integer, Integer, String, Integer> {

        @Override
        public void map(Integer key, Integer value, Context<String, Integer> context) {
            throw new NullPointerException("BUMM!");
        }
    }

    public static class TimeConsumingMapper
            implements Mapper<Integer, Integer, String, Integer> {

        @Override
        public void map(Integer key, Integer value, Context<String, Integer> collector) {
            try {
                Thread.sleep(1000);
            } catch (Exception ignore) {
            }
            collector.emit(String.valueOf(key), value);
        }
    }

    public static class TestKeyPredicate
            implements KeyPredicate<Integer> {

        @Override
        public boolean evaluate(Integer key) {
            return key == 50;
        }
    }

    public static class TestMapper
            implements Mapper<Integer, Integer, String, Integer> {

        @Override
        public void map(Integer key, Integer value, Context<String, Integer> collector) {
            collector.emit(String.valueOf(key), value);
        }
    }

    public static class GroupingTestMapper
            implements Mapper<Integer, Integer, String, Integer> {

        private int moduleKey = -1;

        public GroupingTestMapper() {
        }

        public GroupingTestMapper(int moduleKey) {
            this.moduleKey = moduleKey;
        }

        @Override
        public void map(Integer key, Integer value, Context<String, Integer> collector) {
            if (moduleKey == -1 || (key % 4) == moduleKey) {
                collector.emit(String.valueOf(key % 4), value);
            }
        }
    }

    public static class TestReducer
            extends Reducer<String, Integer, Integer> {

        private transient int sum = 0;

        @Override
        public void reduce(Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }

    public static class TestReducerFactory
            implements ReducerFactory<String, Integer, Integer> {

        public TestReducerFactory() {
        }

        @Override
        public Reducer<String, Integer, Integer> newReducer(String key) {
            return new TestReducer();
        }
    }

    public static class GroupingTestCollator
            implements Collator<Map.Entry<String, List<Integer>>, Integer> {

        @Override
        public Integer collate(Iterable<Map.Entry<String, List<Integer>>> values) {
            int sum = 0;
            for (Map.Entry<String, List<Integer>> entry : values) {
                for (Integer value : entry.getValue()) {
                    sum += value;
                }
            }
            return sum;
        }
    }

    public static class TestCollator
            implements Collator<Map.Entry<String, Integer>, Integer> {

        @Override
        public Integer collate(Iterable<Map.Entry<String, Integer>> values) {
            int sum = 0;
            for (Map.Entry<String, Integer> entry : values) {
                sum += entry.getValue();
            }
            return sum;
        }
    }

    public static class MapKeyValueSourceAdapter<K, V>
            extends KeyValueSource<K, V>
            implements DataSerializable, PartitionIdAware {

        private volatile KeyValueSource<K, V> keyValueSource;
        private int openCount = 0;

        public MapKeyValueSourceAdapter() {
        }

        public MapKeyValueSourceAdapter(KeyValueSource<K, V> keyValueSource) {
            this.keyValueSource = keyValueSource;
        }

        @Override
        public boolean open(NodeEngine nodeEngine) {
            if (openCount < 2) {
                openCount++;
                return false;
            }
            return keyValueSource.open(nodeEngine);
        }

        @Override
        public boolean hasNext() {
            return keyValueSource.hasNext();
        }

        @Override
        public K key() {
            return keyValueSource.key();
        }

        @Override
        public Map.Entry<K, V> element() {
            return keyValueSource.element();
        }

        @Override
        public boolean reset() {
            return keyValueSource.reset();
        }

        @Override
        public boolean isAllKeysSupported() {
            return keyValueSource.isAllKeysSupported();
        }

        @Override
        public Collection<K> getAllKeys0() {
            return keyValueSource.getAllKeys0();
        }

        public static <K1, V1> KeyValueSource<K1, V1> fromMap(IMap<K1, V1> map) {
            return KeyValueSource.fromMap(map);
        }

        public static <K1, V1> KeyValueSource<K1, V1> fromMultiMap(MultiMap<K1, V1> multiMap) {
            return KeyValueSource.fromMultiMap(multiMap);
        }

        public static <V1> KeyValueSource<String, V1> fromList(IList<V1> list) {
            return KeyValueSource.fromList(list);
        }

        public static <V1> KeyValueSource<String, V1> fromSet(ISet<V1> set) {
            return KeyValueSource.fromSet(set);
        }

        @Override
        public void close()
                throws IOException {
            keyValueSource.close();
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeObject(keyValueSource);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            keyValueSource = in.readObject();
        }

        @Override
        public void setPartitionId(int partitionId) {
            if (keyValueSource instanceof PartitionIdAware) {
                ((PartitionIdAware) keyValueSource).setPartitionId(partitionId);
            }
        }
    }

}
