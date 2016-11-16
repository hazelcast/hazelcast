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

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
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
import static org.junit.Assert.assertNotNull;

@Category(NightlyTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WordCountTest extends HazelcastTestSupport implements Serializable {

    private static final int NODE_COUNT = 2;
    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors() / NODE_COUNT;

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
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() {
        Config hazelcastConfig = new Config();
        hazelcastConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        hazelcastConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        hazelcastConfig.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setImplementation(new StreamSerializer<Map.Entry>() {
            @Override
            public int getTypeId() {
                return 60000;
            }

            @Override
            public void destroy() {

            }

            @Override
            public void write(ObjectDataOutput out, Entry object) throws IOException {
                out.writeObject(object.getKey());
                out.writeObject(object.getValue());
            }

            @Override
            public Entry read(ObjectDataInput in) throws IOException {
                Object key = in.readObject();
                Object value = in.readObject();
                return new SimpleImmutableEntry(key, value);
            }
        }).setTypeClass(Map.Entry.class);

        hazelcastConfig.getSerializationConfig().addSerializerConfig(serializerConfig);
        for (int i = 0; i < NODE_COUNT; i++) {
            instance = Hazelcast.newHazelcastInstance(hazelcastConfig);
        }

        JetEngineConfig config = new JetEngineConfig().setParallelism(PARALLELISM);
        jetEngine = JetEngine.get(instance, "jetEngine", config);
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


    @Test
    public void test() {
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", IMapReader.supplier("words"));
        Vertex generator = new Vertex("generator", Generator::new);
        Vertex accumulator = new Vertex("accumulator", Combiner::new);
        Vertex combiner = new Vertex("combiner", Combiner::new);
        Vertex consumer = new Vertex("consumer", IMapWriter.supplier("counts"));

        dag
                .addVertex(producer)
                .addVertex(generator)
                .addVertex(accumulator)
                .addVertex(combiner)
                .addVertex(consumer)
                .addEdge(new Edge(producer, generator))
                .addEdge(new Edge(generator, accumulator)
                        .partitioned(item -> ((Entry) item).getKey()))
                .addEdge(new Edge(accumulator, combiner)
                        .distributed()
                        .partitioned(item -> ((Entry) item).getKey()))
                .addEdge(new Edge(combiner, consumer));

        List<Long> times = new ArrayList<>();
        long testStart = System.currentTimeMillis();
        int warmupCount = 0;
        boolean warmupEnded = false;
        ILogger logger = instance.getLoggingService().getLogger(WordCountTest.class);
        logger.info("Starting test..");
        logger.info("Warming up...");
        while (true) {
            long start = System.currentTimeMillis();
            jetEngine.newJob(dag).execute();
            long end = System.currentTimeMillis();
            long time = end - start;
            times.add(time);
            logger.info("jet2: totalTime=" + time);
            long sinceTestStart = end - testStart;
            if (sinceTestStart < 60000) {
                warmupCount++;
            }

            if (!warmupEnded && sinceTestStart > 60000) {
                logger.info("Warm up ended");
                warmupEnded = true;
            }

            if (sinceTestStart > 180000) {
                break;
            }
//            IMap<String, Long> consumerMap = instance.getMap("counts");
//            assertCounts(consumerMap);
//            consumerMap.clear();
        }
        logger.info("Test complete");
        System.out.println(times.stream()
                                .skip(warmupCount).mapToLong(l -> l).summaryStatistics());


    }

    private void assertCounts(Map<String, Long> wordCounts) {
        for (int i = 0; i < DISTINCT; i++) {
            Long count = wordCounts.get(Integer.toString(i));
            assertNotNull("Missing count for " + i, count);
            assertEquals("The count for " + i + " is not correct", COUNT / DISTINCT, (long) count);
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

            while (iterator.hasNext() && !getOutbox().isHighWater()) {
                emit(iterator.next());
            }
            return !iterator.hasNext();
        }
    }
}
