/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.benchmark;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.stream.AbstractStreamTest;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueMapName;
import static org.junit.Assert.assertEquals;

@Category(NightlyTest.class)
public class WordCountTest extends AbstractStreamTest implements Serializable {

    private static final int COUNT = 1_000_000;
    private static final int DISTINCT = 100_000;

    private IStreamMap<Integer, String> map;

    @Before
    public void setUp() {
        map = getMap();

        int row = 0;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < COUNT; i++) {
            sb.append(i % DISTINCT);
            if (i % 20 == 0) {
                map.put(row++, sb.toString());
                sb.setLength(0);
            } else {
                sb.append(" ");
            }
        }
        map.put(row, sb.toString());
    }

    @Test
    @Ignore
    public void testMapReduce() throws Exception {
        long start = System.currentTimeMillis();

        JobTracker tracker = instance.getHazelcastInstance().getJobTracker("default");
        KeyValueSource<Integer, String> source = KeyValueSource.fromMap(map);
        Job<Integer, String> job = tracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new TokenizerMapper())
                .reducer(new WordcountReducerFactory())
                .submit();

        Map<String, Long> wordCounts = future.get();

        System.out.println("mapreduce: totalTime=" + (System.currentTimeMillis() - start));

        assertCounts(wordCounts);
    }

    @Test
    public void testWordCount() throws Exception {
        final Pattern space = Pattern.compile("\\s+");
        IMap<String, Long> wordCounts;

        List<Long> times = new ArrayList<>();
        final int warmupCount = 10;
        for (int i = 0; i < 20; i++) {
            long start = System.currentTimeMillis();
            wordCounts = map.stream()
                            .flatMap(m -> Stream.of(space.split(m.getValue())))
                            .collect(DistributedCollectors.groupingByToIMap(uniqueMapName(), m -> m, DistributedCollectors.counting()));
            long time = System.currentTimeMillis() - start;
            times.add(time);
            System.out.println("java.util.stream: totalTime=" + time);
            assertCounts(wordCounts);
            wordCounts.clear();
        }

        System.out.println(times.stream()
                                .skip(warmupCount).mapToLong(l -> l).summaryStatistics());
    }

    private void assertCounts(Map<String, Long> wordCounts) {
        for (int i = 0; i < DISTINCT; i++) {
            Long count = wordCounts.get(Integer.toString(i));
            assertEquals(COUNT / DISTINCT, (long) count);
        }
    }

    private static class TokenizerMapper
            implements Mapper<Integer, String, String, Long> {

        private static final Long ONE = Long.valueOf(1);

        @Override
        public void map(Integer key, String value, Context<String, Long> context) {
            StringTokenizer tokenizer = new StringTokenizer(value);
            while (tokenizer.hasMoreTokens()) {
                context.emit(tokenizer.nextToken(), ONE);
            }
        }
    }

    private static class WordcountReducerFactory
            implements ReducerFactory<String, Long, Long> {

        @Override
        public Reducer<Long, Long> newReducer(String key) {
            return new WordcountReducer();
        }

        private static class WordcountReducer
                extends Reducer<Long, Long> {

            private volatile long count;

            @Override
            public void reduce(Long value) {
                // Use with and without Combiner to show combining phase!
                // System.out.println("Retrieved value: " + value);
                count += value;
            }

            @Override
            public Long finalizeReduce() {
                return count == 0 ? null : count;
            }
        }
    }
}
