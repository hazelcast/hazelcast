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

package com.hazelcast.mapreduce;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class MultiMapMapReduceTest
        extends HazelcastTestSupport {

    private static final Comparator<Map.Entry<String, Integer>> ENTRYSET_COMPARATOR = new Comparator<Map.Entry<String, Integer>>() {
        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            Integer i1 = Integer.parseInt(o1.getKey());
            Integer i2 = Integer.parseInt(o2.getKey());
            return i1.compareTo(i2);
        }
    };

    @Test(timeout = 60000)
    public void testMapReduceWithMultiMap()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertClusterSizeEventually(3, h1);

        MultiMap<Integer, Integer> multiMap = h1.getMultiMap("default");
        for (int i = 0; i < 1000; i++) {
            for (int o = 0; o < 100; o++) {
                multiMap.put(i, o);
            }
        }

        int expectedResult = 0;
        for (int o = 0; o < 100; o++) {
            expectedResult += o;
        }

        JobTracker jobTracker = h1.getJobTracker("default");
        Job<Integer, Integer> job = jobTracker.newJob(KeyValueSource.fromMultiMap(multiMap));
        ICompletableFuture<Map<String, Integer>> ICompletableFuture = job.chunkSize(10).mapper(new MultiMapMapper())
                                                                         .combiner(new MultiMapCombinerFactory())
                                                                         .reducer(new MultiMapReducerFactory()).submit();

        Map<String, Integer> result = ICompletableFuture.get();

        assertEquals(1000, result.size());

        List<Map.Entry<String, Integer>> entrySet = new ArrayList(result.entrySet());
        Collections.sort(entrySet, ENTRYSET_COMPARATOR);

        int count = 0;
        for (Map.Entry<String, Integer> entry : entrySet) {
            assertEquals(String.valueOf(count++), entry.getKey());
            assertEquals(expectedResult, (int) entry.getValue());
        }
    }

    public static class MultiMapMapper
            implements Mapper<Integer, Integer, String, Integer> {

        @Override
        public void map(Integer key, Integer value, Context<String, Integer> context) {
            context.emit(String.valueOf(key), value);
        }
    }

    public static class MultiMapCombiner
            extends Combiner<Integer, Integer> {

        private int value;

        @Override
        public void combine(Integer value) {
            this.value += value;
        }

        @Override
        public Integer finalizeChunk() {
            int oldValue = value;
            value = 0;
            return oldValue;
        }
    }

    public static class MultiMapCombinerFactory
            implements CombinerFactory<String, Integer, Integer> {

        @Override
        public Combiner<Integer, Integer> newCombiner(String key) {
            return new MultiMapCombiner();
        }
    }

    public static class MultiMapReducer
            extends Reducer<Integer, Integer> {

        private int value;

        @Override
        public void reduce(Integer value) {
            this.value += value;
        }

        @Override
        public Integer finalizeReduce() {
            return value;
        }
    }

    public static class MultiMapReducerFactory
            implements ReducerFactory<String, Integer, Integer> {

        @Override
        public Reducer<Integer, Integer> newReducer(String key) {
            return new MultiMapReducer();
        }
    }

}
