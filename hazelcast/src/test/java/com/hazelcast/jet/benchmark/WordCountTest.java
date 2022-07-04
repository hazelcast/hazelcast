/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastSerialClassRunner;
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
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.Processors.aggregateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.flatMapP;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class WordCountTest extends JetTestSupport implements Serializable {

    private static final int NODE_COUNT = 1;
    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors() / NODE_COUNT;

    private static final int COUNT = 1_000_000;
    private static final int DISTINCT = 100_000;
    private static final int WORDS_PER_ROW = 20;

    private static final int WARMUP_TIME = 10_000;
    private static final int TOTAL_TIME = 30_000;

    private HazelcastInstance instance;
    private ILogger logger;

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void before() {
        Config config = defaultInstanceConfigWithJetEnabled();
        config.getJetConfig().setCooperativeThreadCount(PARALLELISM);
        config.setClusterName(randomName());
        final JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");

        for (int i = 0; i < NODE_COUNT; i++) {
            instance = Hazelcast.newHazelcastInstance(config);
        }
        logger = instance.getLoggingService().getLogger(WordCountTest.class);
        generateMockInput();
    }

    private void generateMockInput() {
        logger.info("Generating input");
        final DAG dag = new DAG();
        Vertex source = dag.newVertex("source",
                (List<Address> addrs) -> (Address addr) -> ProcessorSupplier.of(
                        addr.equals(addrs.get(0)) ? MockInputP::new : noopP()));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeMapP("words"));
        dag.edge(between(source.localParallelism(1), sink.localParallelism(1)));
        instance.getJet().newJob(dag).join();
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
        benchmark("aggregations", () ->
                counts[0] = instance.<Integer, String>getMap("words").aggregate(new WordCountAggregator()));
        assertCounts(counts[0]);
    }

    @Test
    public void testJet() {
        DAG dag = new DAG();

        Vertex source = dag.newVertex("source", SourceProcessors.readMapP("words"));
        Vertex tokenize = dag.newVertex("tokenize",
                flatMapP((Map.Entry<?, String> line) -> {
                    StringTokenizer s = new StringTokenizer(line.getValue());
                    return () -> s.hasMoreTokens() ? s.nextToken() : null;
                })
        );
        // word -> (word, count)
        Vertex aggregateStage1 = dag.newVertex("aggregateStage1",
                aggregateByKeyP(singletonList(wholeItem()), counting(), Util::entry));
        // (word, count) -> (word, count)
        FunctionEx<Entry, ?> getEntryKeyFn = Entry::getKey;
        Vertex aggregateStage2 = dag.newVertex("aggregateStage2",
                aggregateByKeyP(
                        singletonList(getEntryKeyFn),
                        summingLong(Entry<String, Long>::getValue),
                        Util::entry
                )
        );
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeMapP("counts"));

        dag.edge(between(source, tokenize))
           .edge(between(tokenize, aggregateStage1)
                   .partitioned(wholeItem(), HASH_CODE))
           .edge(between(aggregateStage1, aggregateStage2)
                   .distributed()
                   .partitioned(entryKey()))
           .edge(between(aggregateStage2, sink));

        benchmark("jet", () -> instance.getJet().newJob(dag).join());
        assertCounts(instance.getMap("counts"));
    }

    @Test
    @Ignore
    public void testJetTwoPhaseAggregation() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SourceProcessors.readMapP("words"));
        Vertex mapReduce = dag.newVertex("map-reduce", MapReduceP::new);
        Vertex combineLocal = dag.newVertex("combine-local", CombineP::new);
        Vertex combineGlobal = dag.newVertex("combine-global", CombineP::new);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeMapP("counts"));

        dag.edge(between(source, mapReduce))
           .edge(between(mapReduce, combineLocal))
           .edge(between(combineLocal, combineGlobal).distributed().allToOne("ALL"))
           .edge(between(combineGlobal, sink));

        benchmark("jet", () -> instance.getJet().newJob(dag).join());

        assertCounts((Map<String, Long>) instance.getMap("counts").get("result"));
    }

    private void benchmark(String label, Runnable run) {
        List<Long> times = new ArrayList<>();
        long testStart = System.nanoTime();
        int warmupCount = 0;
        boolean warmupEnded = false;
        logger.info("Starting test..");
        logger.info("Warming up...");
        while (true) {
            System.gc();
            System.gc();
            long start = System.nanoTime();
            run.run();
            long end = System.nanoTime();
            long time = end - start;
            times.add(TimeUnit.NANOSECONDS.toMillis(time));
            logger.info(label + ": totalTime=" + time);
            long sinceTestStart = TimeUnit.NANOSECONDS.toMillis(end - testStart);
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

    private static class WordCountAggregator implements Aggregator<Map.Entry<Integer, String>, Map<String, Long>> {
        private static final Pattern PATTERN = Pattern.compile("\\w+");

        private Map<String, Long> counts = new HashMap<>();

        @Override
        public void accumulate(Entry<Integer, String> input) {
            String text = input.getValue().toLowerCase(Locale.ROOT);
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
            String text = ((Entry<Integer, String>) item).getValue().toLowerCase(Locale.ROOT);
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
