/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.jet.sink.MapSink;
import com.hazelcast.jet.source.MapSource;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WordCountTest extends HazelcastTestSupport implements Serializable {

    private static final int COUNT = 1_000_000;
    private static final int DISTINCT = 1_000_000;

    private static IMap<Integer, String> map;

    private static TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance;

    @BeforeClass
    public static void setupFactory() {
        factory = new TestHazelcastInstanceFactory();
    }

    @AfterClass
    public static void shutdownFactory() {
        factory.shutdownAll();
    }

    @Before
    public void setUp() {
        instance = factory.newHazelcastInstance();
        map = instance.getMap("words");
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
    public void test() throws ExecutionException, InterruptedException {
        DAG dag = new DAG();
        Vertex generator = new Vertex("generator", Generator.class)
                .parallelism(Runtime.getRuntime().availableProcessors());
        generator.addSource(new MapSource("words"));
        Vertex combiner = new Vertex("combiner", Combiner.class)
                .parallelism(Runtime.getRuntime().availableProcessors());
        combiner.addSink(new MapSink("counts"));
        dag
                .addVertex(generator)
                .addVertex(combiner)
                .addEdge(new Edge("edge", generator, combiner).partitioned());

        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();
            Job job = JetEngine.getJob(instance, "word count", dag);
            job.execute().get();
            System.out.println("jet: totalTime=" + (System.currentTimeMillis() - start));
        }
        IMap<String, Long> consumerMap = instance.getMap("counts");
        assertCounts(consumerMap);

    }

    private void assertCounts(Map<String, Long> wordCounts) {
        for (int i = 0; i < DISTINCT; i++) {
            Long count = wordCounts.get(Integer.toString(i));
            assertEquals(COUNT / DISTINCT, (long) count);
        }
    }

    public static class Generator implements Processor<Pair<Long, String>, Pair<String, Long>> {

        static final Pattern PATTERN = Pattern.compile("\\W+");

        @Override
        public boolean process(InputChunk<Pair<Long, String>> input,
                               OutputCollector<Pair<String, Long>> output,
                               String sourceName) throws Exception {
            for (Pair<Long, String> pair : input) {

                // split each line into lowercase words
                String[] split = PATTERN.split(pair.getValue().toLowerCase());

                for (String word : split) {
                    // emit each word with count of 1
                    output.collect(new JetPair<>(word, 1L));
                }
            }
            return true;
        }

    }

    public static class Combiner implements Processor<Pair<String, Long>, Pair<String, Long>> {

        private Map<String, Long> cache = new HashMap<>();
        private Iterator<Map.Entry<String, Long>> finalizationIterator;
        private int chunkSize;

        @Override
        public void before(TaskContext taskContext) {
            chunkSize = taskContext.getJobContext().getJobConfig().getChunkSize();
        }

        @Override
        public boolean process(InputChunk<Pair<String, Long>> input,
                               OutputCollector<Pair<String, Long>> output,
                               String sourceName) throws Exception {

            // increment the count in the cache if word exists, otherwise create new entry in cache
            for (Pair<String, Long> word : input) {
                Long value = this.cache.get(word.getKey());
                if (value == null) {
                    cache.put(word.getKey(), word.getValue());
                } else {
                    cache.put(word.getKey(), value + word.getValue());
                }
            }
            return true;
        }

        @Override
        public boolean complete(OutputCollector<Pair<String, Long>> output) throws Exception {
            boolean finalized = false;
            try {
                if (finalizationIterator == null) {
                    finalizationIterator = cache.entrySet().iterator();
                }

                int idx = 0;
                while (finalizationIterator.hasNext()) {
                    Map.Entry<String, Long> next = finalizationIterator.next();
                    output.collect(new JetPair<>(next.getKey(), next.getValue()));
                    if (idx == chunkSize - 1) {
                        break;
                    }
                    idx++;
                }
                finalized = !finalizationIterator.hasNext();
            } finally {
                if (finalized) {
                    finalizationIterator = null;
                    cache.clear();
                }
            }
            return finalized;
        }

        @Override
        public void after() {
            // free up memory after execution
            cache.clear();
        }
    }
}
