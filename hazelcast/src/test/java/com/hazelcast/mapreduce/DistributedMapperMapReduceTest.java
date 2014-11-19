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
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class DistributedMapperMapReduceTest
        extends HazelcastTestSupport {

    private static final String MAP_NAME = "default";

    @Test(timeout = 30000)
    public void testMapperReducer() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, Integer>> future = job.mapper(new GroupingTestMapper()).combiner(new TestCombinerFactory())
                                                             .reducer(new TestReducerFactory()).submit();

        // Precalculate results
        int[] expectedResults = new int[4];
        for (int i = 0; i < 100; i++) {
            int index = i % 4;
            expectedResults[index] += i;
        }

        Map<String, Integer> result = future.get();

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResults[i], (int) result.get(String.valueOf(i)));
        }
    }

    @Test(timeout = 30000)
    public void testMapperCustomIntermediateCombinerReducer() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, Long>> future = job.mapper(new GroupingTestMapper())
                                                          .combiner(new TestIntermediateCombinerFactory())
                                                          .reducer(new TestIntermediateReducerFactory()).submit();

        // Pre-calculate results
        int[] expectedResults = new int[4];
        for (int i = 0; i < 100; i++) {
            int index = i % 4;
            expectedResults[index] += i;
        }

        Map<String, Long> result = future.get();

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResults[i], (long) result.get(String.valueOf(i)));
        }
    }

    @Test(timeout = 30000)
    public void testMapperReducerCollator() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future = job.mapper(new GroupingTestMapper()).combiner(new TestCombinerFactory())
                                                .reducer(new TestReducerFactory()).submit(new TestCollator());

        // Precalculate result
        int expectedResult = 0;
        for (int i = 0; i < 100; i++) {
            expectedResult += i;
        }

        int result = future.get();

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResult, result);
        }
    }

    @Test(timeout = 30000)
    public void testAsyncMapperReducer() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, Integer> listenerResults = new HashMap<String, Integer>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Map<String, Integer>> future = job.mapper(new GroupingTestMapper()).combiner(new TestCombinerFactory())
                                                             .reducer(new TestReducerFactory()).submit();

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

    @Test(timeout = 30000)
    public void testAsyncMapperReducerCollator() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        ICompletableFuture<Integer> future = job.mapper(new GroupingTestMapper()).combiner(new TestCombinerFactory())
                                                .reducer(new TestReducerFactory()).submit(new TestCollator());

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

    public static class TestCombiner
            extends Combiner<Integer, Integer> {

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

    public static class TestCombinerFactory
            implements CombinerFactory<String, Integer, Integer> {

        public TestCombinerFactory() {
        }

        @Override
        public Combiner<Integer, Integer> newCombiner(String key) {
            return new TestCombiner();
        }
    }

    public static class TestReducer
            extends Reducer<Integer, Integer> {

        private int sum;

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
        public Reducer<Integer, Integer> newReducer(String key) {
            return new TestReducer();
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

    public static class TestIntermediateCombiner
            extends Combiner<Integer, Long> {

        private transient int sum;

        @Override
        public void combine(Integer value) {
            sum += value;
        }

        @Override
        public Long finalizeChunk() {
            int v = sum;
            sum = 0;
            return Long.valueOf(v);
        }
    }

    public static class TestIntermediateCombinerFactory
            implements CombinerFactory<String, Integer, Long> {

        public TestIntermediateCombinerFactory() {
        }

        @Override
        public Combiner<Integer, Long> newCombiner(String key) {
            return new TestIntermediateCombiner();
        }
    }

    public static class TestIntermediateReducer
            extends Reducer<Long, Long> {

        private long sum;

        @Override
        public void reduce(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }

    public static class TestIntermediateReducerFactory
            implements ReducerFactory<String, Long, Long> {

        public TestIntermediateReducerFactory() {
        }

        @Override
        public Reducer<Long, Long> newReducer(String key) {
            return new TestIntermediateReducer();
        }
    }

}
