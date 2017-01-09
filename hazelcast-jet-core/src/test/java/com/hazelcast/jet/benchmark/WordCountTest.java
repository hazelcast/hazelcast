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

package com.hazelcast.jet.benchmark;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.connector.IMapReader;
import com.hazelcast.jet.impl.connector.IMapWriter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
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

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.impl.util.Util.uncheckedGet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(NightlyTest.class)
@RunWith(HazelcastParallelClassRunner.class)
@Ignore
public class WordCountTest extends HazelcastTestSupport implements Serializable {

    private static final int NODE_COUNT = 2;
    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors() / NODE_COUNT;

    private static final int COUNT = 1_000_000;
    private static final int DISTINCT = 100_000;

    private JetInstance instance;

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() {
        JetConfig config = new JetConfig().setExecutionThreadCount(PARALLELISM);
        Config hazelcastConfig = config.getHazelcastConfig();
        final JoinConfig join = hazelcastConfig.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");

        for (int i = 0; i < NODE_COUNT; i++) {
            instance = Jet.newJetInstance(config);
        }

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
    public void testAggregations() {
        final Map<String, Long>[] counts = new Map[1];
        benchmark("aggregations", () -> {
            counts[0] = instance.<Integer, String>getMap("words").aggregate(new WordCountAggregator());
        });
        assertCounts(counts[0]);
    }

    @Test
    public void testJet() {
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", IMapReader.supplier("words"));
        Vertex generator = new Vertex("generator", Generator::new);
        Vertex accumulator = new Vertex("accumulator", Combiner::new);
        Vertex combiner = new Vertex("combiner", Combiner::new);
        Vertex consumer = new Vertex("consumer", IMapWriter.supplier("counts"));

        dag
                .vertex(producer)
                .vertex(generator)
                .vertex(accumulator)
                .vertex(combiner)
                .vertex(consumer)
                .edge(between(producer, generator))
                .edge(between(generator, accumulator)
                        .partitionedByCustom((item, n) -> Math.abs(((Entry) item).getKey().hashCode()) % n))
                .edge(between(accumulator, combiner)
                        .distributed()
                        .partitionedByKey(item -> ((Entry) item).getKey()))
                .edge(between(combiner, consumer));

        benchmark("jet", () -> {
            uncheckedGet(instance.newJob(dag).execute());
        });

        assertCounts(instance.getMap("counts"));
    }

    @Test
    public void testJetTwoPhaseAggregation() {
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", IMapReader.supplier("words"));
        Vertex generator = new Vertex("generator", Mapper::new);
        Vertex accumulator = new Vertex("accumulator", Reducer::new)
                .localParallelism(1);
        Vertex combiner = new Vertex("combiner", Reducer::new)
                .localParallelism(1);
        Vertex consumer = new Vertex("consumer", IMapWriter.supplier("counts"))
                .localParallelism(1);
        dag
                .vertex(producer)
                .vertex(generator)
                .vertex(accumulator)
                .vertex(combiner)
                .vertex(consumer)
                .edge(between(producer, generator))
                .edge(between(generator, accumulator))
                .edge(between(accumulator, combiner)
                        .distributed()
                        .allToOne())
                .edge(between(combiner, consumer));

        benchmark("jet", () -> {
                    uncheckedGet(instance.newJob(dag).execute());
                }
        );

        assertCounts((Map<String, Long>) instance.getMap("counts").get("result"));
    }

    private void benchmark(String label, Runnable run) {
        List<Long> times = new ArrayList<>();
        long testStart = System.currentTimeMillis();
        int warmupCount = 0;
        boolean warmupEnded = false;
        ILogger logger = instance.getHazelcastInstance().getLoggingService().getLogger(WordCountTest.class);
        logger.info("Starting test..");
        logger.info("Warming up...");
        while (true) {
            long start = System.currentTimeMillis();
            run.run();
            long end = System.currentTimeMillis();
            long time = end - start;
            times.add(time);
            logger.info(label + ": totalTime=" + time);
            long sinceTestStart = end - testStart;
            if (sinceTestStart < 30000) {
                warmupCount++;
            }

            if (!warmupEnded && sinceTestStart > 30000) {
                logger.info("Warm up ended");
                warmupEnded = true;
            }

            if (sinceTestStart > 90000) {
                break;
            }
        }
        logger.info("Test complete");
        System.out.println(times.stream()
                .skip(warmupCount).mapToLong(l -> l).summaryStatistics());
    }

    private static void assertCounts(Map<String, Long> wordCounts) {
        for (int i = 0; i < DISTINCT; i++) {
            Long count = wordCounts.get(Integer.toString(i));
            assertNotNull("Missing count for " + i, count);
            assertEquals("The count for " + i + " is not correct", COUNT / DISTINCT, (long) count);
        }
    }

    private static class WordCountAggregator extends Aggregator<Map.Entry<Integer, String>, Map<String, Long>> {
        private static final Pattern PATTERN = Pattern.compile("\\w+");

        private Map<String, Long> counts = new HashMap<>();

        @Override
        public void accumulate(Entry<Integer, String> input) {
            String text = input.getValue().toLowerCase();
            Matcher m = PATTERN.matcher(text);
            while (m.find()) {
                accumulate(m.group(), 1L);
            }
        }

        @Override
        public void combine(Aggregator aggregator) {
            Map<String, Long> counts = ((WordCountAggregator) aggregator).counts;
            for (Entry<String, Long> entry : counts.entrySet()) {
                accumulate(entry.getKey(), entry.getValue());
            }
        }

        private void accumulate(String key, long addition) {
            counts.compute(key, (k, v) -> v == null ? addition : v + addition);
        }

        @Override
        public Map<String, Long> aggregate() {
            return counts;
        }
    }

    private static class Generator extends AbstractProcessor {

        private static final Pattern PATTERN = Pattern.compile("\\w+");

        private final TryProcessor<Entry<Integer, String>, Entry<String, Long>> p = tryProcessor(entry -> {
            String text = entry.getValue().toLowerCase();
            Matcher m = PATTERN.matcher(text);
            return () -> m.find() ? new SimpleImmutableEntry<>(m.group(), 1L) : null;
        });

        @Override
        public boolean tryProcess(int ordinal, Object item) {
            return p.tryProcess(ordinal, (Entry<Integer, String>) item);
        }
    }

    static class Combiner extends AbstractProcessor {
        private Map<String, Long> counts = new HashMap<>();
        private Iterator<Map.Entry<String, Long>> iterator;

        @Override
        public boolean tryProcess(int ordinal, Object item) {
            Map.Entry<String, Long> entry = (Map.Entry<String, Long>) item;
            counts.compute(entry.getKey(), (k, v) -> v == null ? entry.getValue() : v + entry.getValue());
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


    private static class Mapper extends AbstractProcessor {

        private static final Pattern PATTERN = Pattern.compile("\\w+");
        private Map<String, Long> counts = new HashMap<>();

        @Override
        public boolean tryProcess(int ordinal, Object item) {
            String text = ((Entry<Integer, String>) item).getValue().toLowerCase();
            Matcher m = PATTERN.matcher(text);
            while (m.find()) {
                accumulate(m.group(), 1);
            }
            return true;
        }

        @Override
        public boolean complete() {
            emit(new SimpleImmutableEntry<>("result", counts));
            return true;
        }

        private void accumulate(String key, long addition) {
            counts.compute(key, (k, v) -> v == null ? addition : v + addition);
        }
    }

    private static class Reducer extends AbstractProcessor {

        private Map<String, Long> counts = new HashMap<>();

        @Override
        public boolean tryProcess(int ordinal, Object item) {
            Map<String, Long> counts = ((Entry<String, Map<String, Long>>) item).getValue();
            for (Entry<String, Long> entry : counts.entrySet()) {
                accumulate(entry.getKey(), entry.getValue());
            }
            return true;
        }

        @Override
        public boolean completeEdge(int ordinal) {
            emit(new SimpleImmutableEntry<>("result", counts));
            return true;
        }

        private void accumulate(String key, long addition) {
            counts.compute(key, (k, v) -> v == null ? addition : v + addition);
        }
    }
}
