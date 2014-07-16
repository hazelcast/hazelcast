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
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class DistributedMapperClientMapReduceTest extends AbstractClientMapReduceJobTest {

    private static final String MAP_NAME = "default";
    private static final int MAP_SIZE = 100;

    private HazelcastInstance client;
    private IMap<Integer, Integer> map;

    @Before
    public void setup() {
        Config config = buildConfig();

        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        client = HazelcastClient.newHazelcastClient(null);
        map = client.getMap(MAP_NAME);
        populateMap(map, MAP_SIZE);
    }

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 30000)
    public void testMapperReducer() throws Exception {
        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(map));
        ICompletableFuture<Map<String, Integer>> future = job.mapper(new GroupingTestMapper()).combiner(new TestCombinerFactory())
                                                             .reducer(new TestReducerFactory()).submit();

        Map<String, Integer> result = future.get();

        int[] expectedResults = calculateExpectedResult(4, MAP_SIZE);
        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResults[i], (int) result.get(String.valueOf(i)));
        }
    }

    @Test(timeout = 30000)
    public void testMapperReducerCollator() throws Exception {
        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(map));
        ICompletableFuture<Integer> future = job.mapper(new GroupingTestMapper()).combiner(new TestCombinerFactory())
                                                .reducer(new TestReducerFactory()).submit(new TestCollator());

        int result = future.get();

        int expectedResult = calculateExceptedResult(MAP_SIZE);
        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResult, result);
        }
    }

    @Test(timeout = 30000)
    public void testAsyncMapperReducer() throws Exception {
        final Map<String, Integer> listenerResults = new HashMap<String, Integer>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(map));
        ICompletableFuture<Map<String, Integer>> future = job.mapper(new GroupingTestMapper()).combiner(new TestCombinerFactory())
                                                             .reducer(new TestReducerFactory()).submit();

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

        semaphore.acquire();

        int[] expectedResults = calculateExpectedResult(4, MAP_SIZE);
        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResults[i], (int) listenerResults.get(String.valueOf(i)));
        }
    }

    @Test(timeout = 30000)
    public void testAsyncMapperReducerCollator() throws Exception {
        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = client.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(map));
        ICompletableFuture<Integer> future = job.mapper(new GroupingTestMapper()).combiner(new TestCombinerFactory())
                                                .reducer(new TestReducerFactory()).submit(new TestCollator());

        future.andThen(new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                try {
                    result[0] = response;
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

        int expectedResult = calculateExceptedResult(MAP_SIZE);
        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResult, result[0]);
        }
    }

    private static class TestCombiner extends Combiner<Integer, Integer> {
        private transient int sum;

        @Override
        public void combine(Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeChunk() {
            int v = sum;
            sum = 0;
            return v;
        }
    }

    private static class TestCombinerFactory implements CombinerFactory<String, Integer, Integer> {
        public TestCombinerFactory() {
        }

        @Override
        public Combiner<Integer, Integer> newCombiner(String key) {
            return new TestCombiner();
        }
    }

    private static class GroupingTestMapper implements Mapper<Integer, Integer, String, Integer> {
        @Override
        public void map(Integer key, Integer value, Context<String, Integer> collector) {
            collector.emit(String.valueOf(key % 4), value);
        }
    }

    private static class TestReducer extends Reducer<Integer, Integer> {
        private transient int sum;

        @Override
        public void reduce(Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }

    private static class TestReducerFactory implements ReducerFactory<String, Integer, Integer> {
        @Override
        public Reducer<Integer, Integer> newReducer(String key) {
            return new TestReducer();
        }
    }

    private static class TestCollator implements Collator<Map.Entry<String, Integer>, Integer> {
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