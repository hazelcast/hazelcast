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

package com.hazelcast.client.mapreduce;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientMapReduceTest extends AbstractClientMapReduceJobTest {

    private static final String MAP_NAME = "default";

    @After
    public void shutdown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }


    @Test(timeout = 60000, expected = ExecutionException.class)
    public void testExceptionDistribution()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future =
                job.mapper(new ExceptionThrowingMapper())
                        .submit();

        try {
            Map<String, List<Integer>> result = future.get();
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getCause() instanceof NullPointerException);
            throw e;
        }
    }

    @Test(timeout = 60000, expected = CancellationException.class)
    public void testInProcessCancellation()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future =
                job.mapper(new TimeConsumingMapper())
                        .submit();

        future.cancel(true);

        try {
            Map<String, List<Integer>> result = future.get();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test(timeout = 60000)
    public void testMapper()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future =
                job.mapper(new TestMapper())
                        .submit();

        Map<String, List<Integer>> result = future.get();

        assertEquals(100, result.size());
        for (List<Integer> value : result.values()) {
            assertEquals(1, value.size());
        }
    }

    @Test(timeout = 60000)
    public void testMapperReducer()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, Integer>> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory())
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
    public void testMapperCollator()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())
                        .submit(new GroupingTestCollator());

        int result = future.get();

        // Precalculate result
        int expectedResult = 0;
        for (int i = 0; i < 100; i++) {
            expectedResult += i;
        }
    }

    @Test(timeout = 60000)
    public void testKeyedMapperCollator()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 10000; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future =
                job.onKeys(50)
                        .mapper(new TestMapper())
                        .submit(new GroupingTestCollator());

        int result = future.get();

        assertEquals(50, result);
    }

    @Test(timeout = 60000)
    public void testKeyPredicateMapperCollator()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 10000; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future =
                job.keyPredicate(new TestKeyPredicate())
                        .mapper(new TestMapper())
                        .submit(new GroupingTestCollator());

        int result = future.get();

        assertEquals(50, result);
    }

    @Test(timeout = 60000)
    public void testMapperReducerCollator()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory())
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

    @Test(timeout = 60000)
    public void testAsyncMapper()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, List<Integer>> listenerResults = new HashMap<String, List<Integer>>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future =
                job.mapper(new TestMapper()).submit();

        future.andThen(new ExecutionCallback<Map<String, List<Integer>>>() {
            @Override
            public void onResponse(Map<String, List<Integer>> response) {
                listenerResults.putAll(response);
                semaphore.release();
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

    @Test(timeout = 60000)
    public void testKeyedAsyncMapper()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, List<Integer>> listenerResults = new HashMap<String, List<Integer>>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, List<Integer>>> future =
                job.onKeys(50)
                        .mapper(new TestMapper())
                        .submit();

        future.andThen(new ExecutionCallback<Map<String, List<Integer>>>() {
            @Override
            public void onResponse(Map<String, List<Integer>> response) {
                listenerResults.putAll(response);
                semaphore.release();
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

    @Test(timeout = 60000)
    public void testAsyncMapperReducer()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, Integer> listenerResults = new HashMap<String, Integer>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, Integer>> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory())
                        .submit();

        future.andThen(new ExecutionCallback<Map<String, Integer>>() {
            @Override
            public void onResponse(Map<String, Integer> response) {
                listenerResults.putAll(response);
                semaphore.release();
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

    @Test(timeout = 60000)
    public void testAsyncMapperCollator()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())//
                        .submit(new GroupingTestCollator());

        future.andThen(new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                result[0] = response.intValue();
                semaphore.release();
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

    @Test(timeout = 60000)
    public void testAsyncMapperReducerCollator()
            throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory())
                        .submit(new TestCollator());

        future.andThen(new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                result[0] = response.intValue();
                semaphore.release();
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
            } catch(Exception ignore) {
            }
            collector.emit(String.valueOf(key), value);
        }
    }

    public static class TestKeyPredicate implements KeyPredicate<Integer> {

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

        @Override
        public void map(Integer key, Integer value, Context<String, Integer> collector) {
            collector.emit(String.valueOf(key % 4), value);
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

}