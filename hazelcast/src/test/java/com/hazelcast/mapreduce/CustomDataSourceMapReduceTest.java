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
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class CustomDataSourceMapReduceTest
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
    public void testMapReduceWithCustomKeyValueSource()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        JobTracker jobTracker = h1.getJobTracker("default");
        Job<String, Integer> job = jobTracker.newJob(new CustomKeyValueSource());
        ICompletableFuture<Map<String, Integer>> completableFuture = job.chunkSize(10).mapper(new CustomMapper())
                                                                        .combiner(new CustomCombinerFactory())
                                                                        .reducer(new CustomReducerFactory()).submit();

        Map<String, Integer> result = completableFuture.get();

        assertEquals(1000, result.size());

        List<Map.Entry<String, Integer>> entrySet = new ArrayList(result.entrySet());
        Collections.sort(entrySet, ENTRYSET_COMPARATOR);

        int count = 0;
        for (Map.Entry<String, Integer> entry : entrySet) {
            assertEquals(String.valueOf(count), entry.getKey());
            assertEquals(count++ * 6, (int) entry.getValue());
        }
    }

    public static class CustomKeyValueSource
            extends KeyValueSource<String, Integer>
            implements Serializable {

        private transient List<Map.Entry<String, Integer>> entries;

        private transient Iterator<Map.Entry<String, Integer>> iterator;
        private transient Map.Entry<String, Integer> nextElement;

        @Override
        public boolean open(NodeEngine nodeEngine) {
            entries = new ArrayList<Map.Entry<String, Integer>>();
            for (int i = 0; i < 1000; i++) {
                entries.add(new AbstractMap.SimpleEntry<String, Integer>(String.valueOf(i), i));
            }
            iterator = entries.iterator();
            return true;
        }

        @Override
        public boolean hasNext() {
            if (iterator == null) {
                return false;
            }
            boolean hasNext = iterator.hasNext();
            nextElement = hasNext ? iterator.next() : null;
            return hasNext;
        }

        @Override
        public String key() {
            if (nextElement == null) {
                throw new IllegalStateException("no more elements");
            }
            return nextElement.getKey();
        }

        @Override
        public Map.Entry<String, Integer> element() {
            if (nextElement == null) {
                throw new IllegalStateException("no more elements");
            }
            return nextElement;
        }

        @Override
        public boolean reset() {
            iterator = null;
            nextElement = null;
            entries = new ArrayList<Map.Entry<String, Integer>>();
            return true;
        }

        @Override
        public void close()
                throws IOException {
        }
    }

    public static class CustomMapper
            implements Mapper<String, Integer, String, Integer> {

        @Override
        public void map(String key, Integer value, Context<String, Integer> context) {
            context.emit(key, value);
            context.emit(key, value);
        }
    }

    public static class CustomCombiner
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

    public static class CustomCombinerFactory
            implements CombinerFactory<String, Integer, Integer> {

        @Override
        public Combiner<Integer, Integer> newCombiner(String key) {
            return new CustomCombiner();
        }
    }

    public static class CustomReducer
            extends Reducer<Integer, Integer> {

        private volatile int value;

        @Override
        public void reduce(Integer value) {
            this.value += value;
        }

        @Override
        public Integer finalizeReduce() {
            return value;
        }
    }

    public static class CustomReducerFactory
            implements ReducerFactory<String, Integer, Integer> {

        @Override
        public Reducer<Integer, Integer> newReducer(String key) {
            return new CustomReducer();
        }
    }

}
