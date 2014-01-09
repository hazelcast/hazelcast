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

import com.hazelcast.config.Config;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class MapReduceTest
        extends AbstractMapReduceJobTest {

    private static final String MAP_NAME = "default";

    @Before
    public void gc() {
        Runtime.getRuntime().gc();
    }

    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 30000)
    public void testMapper()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

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
        CompletableFuture<Map<String, List<Integer>>> future =
                job.mapper(new TestMapper())
                        .submit();

        Map<String, List<Integer>> result = future.get();

        assertEquals(100, result.size());
        for (List<Integer> value : result.values()) {
            assertEquals(1, value.size());
        }

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(0, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testKeyedMapperCollator()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 10000; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Integer> future =
                job.onKeys(50)
                        .mapper(new TestMapper())
                        .submit(new GroupingTestCollator());

        int result = future.get();

        assertEquals(50, result);

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(0, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testKeyPredicateMapperCollator()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 10000; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Integer> future =
                job.keyPredicate(new TestKeyPredicate()).mapper(new TestMapper())
                        .submit(new GroupingTestCollator());

        int result = future.get();

        assertEquals(50, result);

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(0, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testMapperComplexMapping()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Map<String, List<Integer>>> future =
                job.mapper(new GroupingTestMapper(2))
                        .submit();

        Map<String, List<Integer>> result = future.get();

        assertEquals(1, result.size());
        assertEquals(25, result.values().iterator().next().size());

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(0, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testMapperReducer()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> task = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Map<String, Integer>> future =
                task.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory(h1, context))
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
        assertEquals(3, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testMapperCollator()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())
                        .submit(new GroupingTestCollator());

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
        assertEquals(0, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testMapperReducerCollator()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory(h1, context))
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
        assertEquals(3, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testAsyncMapper()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, List<Integer>> listenerResults = new HashMap<String, List<Integer>>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Map<String, List<Integer>>> future =
                job.mapper(new TestMapper())
                        .submit();

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

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(0, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testKeyedAsyncMapper()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, List<Integer>> listenerResults = new HashMap<String, List<Integer>>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Map<String, List<Integer>>> future =
                job.onKeys(50)
                        .mapper(new TestMapper())
                        .submit();

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

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(0, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testAsyncMapperReducer()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final Map<String, Integer> listenerResults = new HashMap<String, Integer>();
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Map<String, Integer>> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory(h1, context))//
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

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(3, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testAsyncMapperCollator()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())
                        .submit(new GroupingTestCollator());

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

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(0, hazelcastNames.size());
    }

    @Test(timeout = 30000)
    public void testAsyncMapperReducerCollator()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext(context);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put(i, i);
        }

        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        JobTracker tracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = tracker.newJob(KeyValueSource.fromMap(m1));
        CompletableFuture<Integer> future =
                job.mapper(new GroupingTestMapper())
                        .reducer(new TestReducerFactory(h1, context))
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

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals(3, hazelcastNames.size());
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
            extends Reducer<String, Integer, Integer>
            implements CountingAware {

        private transient HazelcastInstance hazelcastInstance;
        private transient Set<String> hazelcastNames;

        private transient int sum = 0;

        public TestReducer(HazelcastInstance hazelcastInstance, Set<String> hazelcastNames) {
            this.hazelcastInstance = hazelcastInstance;
            this.hazelcastNames = hazelcastNames;
        }

        @Override
        public void reduce(Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeReduce() {
            hazelcastNames.add(hazelcastInstance.getName());
            return sum;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public void setCouter(Set<String> hazelcastNames) {
            this.hazelcastNames = hazelcastNames;
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
