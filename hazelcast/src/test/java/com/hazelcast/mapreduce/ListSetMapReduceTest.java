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
import com.hazelcast.core.IList;
import com.hazelcast.core.ISet;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class ListSetMapReduceTest
        extends HazelcastTestSupport {

    @Test(timeout = 60000)
    public void testMapReduceWithList()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        int expectedResult = 0;
        IList<Integer> list = h1.getList("default");
        for (int o = 0; o < 100; o++) {
            list.add(o);
            expectedResult += o;
        }

        JobTracker jobTracker = h1.getJobTracker("default");
        Job<String, Integer> job = jobTracker.newJob(KeyValueSource.fromList(list));
        ICompletableFuture<Map<String, Integer>> ICompletableFuture =
                job.chunkSize(10)
                        .mapper(new ListSetMapper())
                        .combiner(new ListSetCombinerFactory())
                        .reducer(new ListSetReducerFactory())
                        .submit();

        Map<String, Integer> result = ICompletableFuture.get();

        assertEquals(1, result.size());

        int count = 0;
        for (Map.Entry<String, Integer> entry : result.entrySet()) {
            assertEquals(list.getName(), entry.getKey());
            assertEquals(expectedResult, (int) entry.getValue());
        }
    }


    @Test(timeout = 60000)
    public void testMapReduceWithSet()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        int expectedResult = 0;
        ISet<Integer> set = h1.getSet("default");
        for (int o = 0; o < 100; o++) {
            set.add(o);
            expectedResult += o;
        }

        JobTracker jobTracker = h1.getJobTracker("default");
        Job<String, Integer> job = jobTracker.newJob(KeyValueSource.fromSet(set));
        ICompletableFuture<Map<String, Integer>> ICompletableFuture =
                job.chunkSize(10)
                        .mapper(new ListSetMapper())
                        .combiner(new ListSetCombinerFactory())
                        .reducer(new ListSetReducerFactory())
                        .submit();

        Map<String, Integer> result = ICompletableFuture.get();

        assertEquals(1, result.size());

        int count = 0;
        for (Map.Entry<String, Integer> entry : result.entrySet()) {
            assertEquals(set.getName(), entry.getKey());
            assertEquals(expectedResult, (int) entry.getValue());
        }
    }

    public static class ListSetMapper
            implements Mapper<String, Integer, String, Integer> {

        @Override
        public void map(String key, Integer value, Context<String, Integer> context) {
            context.emit(key, value);
        }
    }

    public static class ListSetCombiner
            extends Combiner<String, Integer, Integer> {

        private int value;

        @Override
        public void combine(String key, Integer value) {
            this.value += value;
        }

        @Override
        public Integer finalizeChunk() {
            int oldValue = value;
            value = 0;
            return oldValue;
        }
    }

    public static class ListSetCombinerFactory
            implements CombinerFactory<String, Integer, Integer> {

        @Override
        public Combiner<String, Integer, Integer> newCombiner(String key) {
            return new ListSetCombiner();
        }
    }

    public static class ListSetReducer extends Reducer<String, Integer, Integer> {

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

    public static class ListSetReducerFactory
            implements ReducerFactory<String, Integer, Integer> {

        @Override
        public Reducer<String, Integer, Integer> newReducer(String key) {
            return new ListSetReducer();
        }
    }

}
