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
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.jet.AggregateOperations.counting;
import static com.hazelcast.jet.AggregateOperations.summingLong;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.processor.Processors.flatMap;
import static com.hazelcast.jet.processor.Processors.aggregateByKey;
import static com.hazelcast.jet.processor.Processors.noop;
import static com.hazelcast.jet.processor.Sources.readMap;
import static com.hazelcast.jet.processor.Sinks.writeMap;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(NightlyTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class WordCountTest extends HazelcastTestSupport implements Serializable {

    private static final int NODE_COUNT = 2;
    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors() / NODE_COUNT;

    private static final int COUNT = 1_000_000;
    private static final int DISTINCT = 100_000;
    private static final int WORDS_PER_ROW = 20;

    private static final int WARMUP_TIME = 20_000;
    private static final int TOTAL_TIME = 60_000;

    private JetInstance instance;
    private ILogger logger;

    @AfterClass
    public static void afterClass() {
        Jet.shutdownAll();
    }

    @Before
    public void before() throws Exception {
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(PARALLELISM);
        Config hazelcastConfig = config.getHazelcastConfig();
        final JoinConfig join = hazelcastConfig.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");

        for (int i = 1; i < NODE_COUNT; i++) {
            instance = Jet.newJetInstance(config);
        }
        logger = instance.getHazelcastInstance().getLoggingService().getLogger(WordCountTest.class);
        generateMockInput();
    }

    private void generateMockInput() throws Exception {
        logger.info("Generating input");
        final DAG dag = new DAG();
        Vertex source = dag.newVertex("source",
                (List<Address> addrs) -> (Address addr) -> ProcessorSupplier.of(
                        addr.equals(addrs.get(0)) ? MockInputP::new : noop()));
        Vertex sink = dag.newVertex("sink", writeMap("words"));
        dag.edge(between(source.localParallelism(1), sink.localParallelism(1)));
        instance.newJob(dag).execute().get();
        logger.info("Input generated.");
    }

    private static class MockInputP extends AbstractProcessor {
        private int row;
        private int counter;
        private final StringBuilder sb = new StringBuilder();

        private final Traverser<Entry<Integer, String>> trav = () -> {
            if (counter == COUNT) {
                return null;
            }
            sb.setLength(0);
            String delimiter = "";
            for (int i = 0; i < WORDS_PER_ROW && counter < COUNT; i++, counter++) {
                sb.append(delimiter).append(counter % DISTINCT);
                delimiter = " ";
            }
            return entry(row++, sb.toString());
        };

        @Override
        public boolean complete() {
            return emitFromTraverser(trav);
        }
    }

    @Test
    @Ignore
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

        Vertex source = dag.newVertex("source", readMap("words"));
        Vertex tokenize = dag.newVertex("tokenize",
                flatMap((Map.Entry<?, String> line) -> {
                    StringTokenizer s = new StringTokenizer(line.getValue());
                    return () -> s.hasMoreTokens() ? s.nextToken() : null;
                })
        );
        // word -> (word, count)
        Vertex aggregateStage1 = dag.newVertex("aggregateStage1", aggregateByKey(wholeItem(), counting()));
        // (word, count) -> (word, count)
        Vertex aggregateStage2 = dag.newVertex("aggregateStage2",
                aggregateByKey(entryKey(), summingLong(Entry<String, Long>::getValue)));
        Vertex sink = dag.newVertex("sink", writeMap("counts"));

        dag.edge(between(source.localParallelism(1), tokenize))
           .edge(between(tokenize, aggregateStage1)
                   .partitioned(wholeItem(), HASH_CODE))
           .edge(between(aggregateStage1, aggregateStage2)
                   .distributed()
                   .partitioned(entryKey()))
           .edge(between(aggregateStage2, sink.localParallelism(1)));

        benchmark("jet", () -> uncheckCall(instance.newJob(dag).execute()::get));
        assertCounts(instance.getMap("counts"));
    }

    @Test
    @Ignore
    public void testJetTwoPhaseAggregation() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readMap("words"));
        Vertex mapReduce = dag.newVertex("map-reduce", MapReduceP::new);
        Vertex combineLocal = dag.newVertex("combine-local", CombineP::new);
        Vertex combineGlobal = dag.newVertex("combine-global", CombineP::new);
        Vertex sink = dag.newVertex("sink", writeMap("counts"));

        dag.edge(between(source, mapReduce))
           .edge(between(mapReduce, combineLocal))
           .edge(between(combineLocal, combineGlobal).distributed().allToOne())
           .edge(between(combineGlobal, sink));

        benchmark("jet", () -> uncheckCall(instance.newJob(dag).execute()::get));

        assertCounts((Map<String, Long>) instance.getMap("counts").get("result"));
    }

    private void benchmark(String label, Runnable run) {
        List<Long> times = new ArrayList<>();
        long testStart = System.currentTimeMillis();
        int warmupCount = 0;
        boolean warmupEnded = false;
        logger.info("Starting test..");
        logger.info("Warming up...");
        while (true) {
            System.gc();
            System.gc();
            long start = System.currentTimeMillis();
            run.run();
            long end = System.currentTimeMillis();
            long time = end - start;
            times.add(time);
            logger.info(label + ": totalTime=" + time);
            long sinceTestStart = end - testStart;
            if (sinceTestStart < WARMUP_TIME) {
                warmupCount++;
            }

            if (!warmupEnded && sinceTestStart > WARMUP_TIME) {
                logger.info("Warm up ended");
                warmupEnded = true;
            }

            if (sinceTestStart > TOTAL_TIME) {
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


    private static class MapReduceP extends AbstractProcessor {

        private static final Pattern PATTERN = Pattern.compile("\\w+");
        private Map<String, Long> counts = new HashMap<>();

        @Override
        public boolean tryProcess(int ordinal, @Nonnull Object item) {
            String text = ((Entry<Integer, String>) item).getValue().toLowerCase();
            Matcher m = PATTERN.matcher(text);
            while (m.find()) {
                accumulate(m.group());
            }
            return true;
        }

        @Override
        public boolean complete() {
            return tryEmit(entry("result", counts));
        }

        private void accumulate(String key) {
            counts.compute(key, (k, v) -> (v != null ? v : 0) + 1);
        }
    }

    private static class CombineP extends AbstractProcessor {

        private Map<String, Long> counts = new HashMap<>();

        @Override
        public boolean tryProcess(int ordinal, @Nonnull Object item) {
            Map<String, Long> counts = ((Entry<String, Map<String, Long>>) item).getValue();
            for (Entry<String, Long> entry : counts.entrySet()) {
                accumulate(entry.getKey(), entry.getValue());
            }
            return true;
        }

        @Override
        public boolean complete() {
            return tryEmit(entry("result", counts));
        }

        private void accumulate(String key, long addition) {
            counts.compute(key, (k, v) -> v == null ? addition : v + addition);
        }
    }
}
