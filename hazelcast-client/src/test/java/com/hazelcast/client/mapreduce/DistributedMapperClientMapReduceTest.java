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
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class DistributedMapperClientMapReduceTest
        extends AbstractMapReduceJobTest {

    private static final String MAP_NAME = "default";

    @Before
    public void gc() {
        Runtime.getRuntime().gc();
    }

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 30000)
    public void testMapperReducer()
            throws Exception {
        Config config = buildConfig();
        CountingManagedContext context = (CountingManagedContext) config.getManagedContext();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Map<String, Integer>> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory(client, context))
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

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(4, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testMapperReducerCollator()
            throws Exception {
        Config config = buildConfig();
        CountingManagedContext context = (CountingManagedContext) config.getManagedContext();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        IMap<Integer, Integer> m1 = client.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory(client, context))
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

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(4, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testAsyncMapperReducer()
            throws Exception {
        Config config = buildConfig();
        CountingManagedContext context = (CountingManagedContext) config.getManagedContext();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

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
        CompletableFuture<Map<String, Integer>> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory(client, context))
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

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(4, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testAsyncMapperReducerCollator()
            throws Exception {
        Config config = buildConfig();
        CountingManagedContext context = (CountingManagedContext) config.getManagedContext();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

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
        CompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory(client, context))
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

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(4, hazelcastNames.size());
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

        private transient HazelcastInstance hazelcastInstance;
        private transient Set<String> hazelcastNames;

        private transient int sum;

        public TestReducer(HazelcastInstance hazelcastInstance, Set<String> hazelcastNames) {
            this.hazelcastInstance = hazelcastInstance;
            this.hazelcastNames = hazelcastNames;
        }

        @Override
        public void reduce(String key, Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeReduce() {
            hazelcastNames.add(hazelcastInstance.getName());
            return sum;
        }
    }

    public static class TestReducerFactory
            implements ReducerFactory<String, Integer, Integer>,
            CountingAware {

        private transient HazelcastInstance hazelcastInstance;
        private transient Set<String> hazelcastNames;

        public TestReducerFactory() {
        }

        public TestReducerFactory(HazelcastInstance hazelcastInstance, CountingManagedContext context) {
            this.hazelcastInstance = hazelcastInstance;
            this.hazelcastNames = context.getHazelcastNames();
        }

        @Override
        public Reducer<String, Integer, Integer> newReducer(String key) {
            return new TestReducer(hazelcastInstance, hazelcastNames);
        }

        @Override
        public void setCouter(Set<String> hazelcastNames) {
            this.hazelcastNames = hazelcastNames;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }

    public static class TestCollator
            implements Collator<Map<String, Integer>, Integer> {

        @Override
        public Integer collate(Iterable<Map<String, Integer>> values) {
            int sum = 0;
            for (Map<String, Integer> reducedResults : values) {
                for (Integer value : reducedResults.values()) {
                    sum += value;
                }
            }
            return sum;
        }
    }

}