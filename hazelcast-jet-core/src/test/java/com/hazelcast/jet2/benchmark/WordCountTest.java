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
package com.hazelcast.jet2.benchmark;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.jet2.impl.AbstractProcessor;
import com.hazelcast.jet2.impl.IMapReader;
import com.hazelcast.jet2.impl.IMapWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

@Category(NightlyTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WordCountTest extends HazelcastTestSupport implements Serializable {

    private static final int COUNT = 1_000_000;
    private static final int DISTINCT = 100_000;

    private static TestHazelcastInstanceFactory factory;
    private JetEngine jetEngine;
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
        jetEngine = JetEngine.get(instance, "jetEngine");
        IMap<Integer, String> map = instance.getMap("words");
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


    @Test @Ignore
    public void test() {
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", IMapReader.supplier("words"));
        Vertex generator = new Vertex("generator", Generator::new);
        Vertex combiner = new Vertex("combiner", Combiner::new);
        Vertex consumer = new Vertex("consumer", IMapWriter.supplier("counts"));

        dag
                .addVertex(producer)
                .addVertex(generator)
                .addVertex(combiner)
                .addVertex(consumer)
                .addEdge(new Edge(producer, generator))
                .addEdge(new Edge(generator, combiner)
                        .partitioned((o, n) -> ((Map.Entry<String, Integer>) o).getKey().hashCode() % n))
                .addEdge(new Edge(combiner, consumer));

        List<Long> times = new ArrayList<>();
        final int warmupCount = 10;
        for (int i = 0; i < 50; i++) {
            long start = System.currentTimeMillis();
            jetEngine.newJob(dag).execute();
            long time = System.currentTimeMillis() - start;
            if (i > warmupCount) {
                times.add(time);
            }
            System.out.println("jet2: totalTime=" + time);
        }
        System.out.println(times.stream().mapToLong(l -> l).summaryStatistics());
        IMap<String, Long> consumerMap = instance.getMap("counts");
        assertCounts(consumerMap);

    }

    private void assertCounts(Map<String, Long> wordCounts) {
        for (int i = 0; i < DISTINCT; i++) {
            Long count = wordCounts.get(Integer.toString(i));
            assertEquals(COUNT / DISTINCT, (long) count);
        }
    }

    private static class Generator extends AbstractProcessor {

        private static final Pattern PATTERN = Pattern.compile("\\w+");

        @Override
        public boolean process(int ordinal, Object item) {
            String text = ((Entry<Integer, String>) item).getValue().toLowerCase();
            Matcher m = PATTERN.matcher(text);
            while (m.find()) {
                emit(new SimpleImmutableEntry<>(m.group(), 1L));
            }
            return true;
        }
    }

    private static class Combiner extends AbstractProcessor {
        private Map<String, Long> counts = new HashMap<>();
        private Iterator<Map.Entry<String, Long>> iterator;

        @Override
        public boolean process(int ordinal, Object item) {
            Map.Entry<String, Long> entry = (Map.Entry<String, Long>) item;

            Long value = this.counts.get(entry.getKey());
            if (value == null) {
                counts.put(entry.getKey(), entry.getValue());
            } else {
                counts.put(entry.getKey(), value + entry.getValue());
            }
            return true;
        }

        @Override
        public boolean complete() {
            if (iterator == null) {
                iterator = counts.entrySet().iterator();
            }

            for (int i = 0; i < 1024 && iterator.hasNext(); i++) {
                emit(iterator.next());
            }
            return !iterator.hasNext();
        }
    }
}
