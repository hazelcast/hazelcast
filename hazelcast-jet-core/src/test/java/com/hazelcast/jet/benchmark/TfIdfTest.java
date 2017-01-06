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

import com.hazelcast.internal.util.ThreadLocalRandom;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.EdgeConfig;
import com.hazelcast.jet.JetConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.Distributed.Function;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Processors.accumulator;
import static com.hazelcast.jet.Processors.distinctCounter;
import static com.hazelcast.jet.Processors.groupingAccumulator;
import static com.hazelcast.jet.Processors.listReader;
import static com.hazelcast.jet.Processors.mapWriter;
import static com.hazelcast.jet.Suppliers.lazyIterate;
import static com.hazelcast.jet.Suppliers.peek;
import static com.hazelcast.jet.TestUtil.executeAndPeel;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparingDouble;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.summingDouble;
import static org.junit.Assert.assertEquals;

@Category(NightlyTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class TfIdfTest extends JetTestSupport {

    private static final String DOCS_WORDS = "docId_words";
    private static final String WORD_DOCSCORES = "word -> doc_score";
    private static final int CLUSTER_SIZE = 2;
    private static final int TOTAL_PARALLELISM = Runtime.getRuntime().availableProcessors();
    private static final int PARALLELISM_PER_MEMBER = TOTAL_PARALLELISM / CLUSTER_SIZE;

    private static final int WORD_LENGTH = 2;
    private static final int CHARSET_SIZE = 20;
    private static final int DOC_COUNT = 800;
    private static final int WORDS_PER_DOC = 1000;

    private final StringBuilder sb = new StringBuilder();

    private JetTestInstanceFactory factory;
    private JetInstance jet;
    private IStreamList<Entry<Long, String>> docWords;
    private IStreamMap<String, List<Entry<Long, Double>>> docIndex;

    @Before
    public void before() {
        JetConfig config = new JetConfig().setExecutionThreadCount(PARALLELISM_PER_MEMBER);
        factory = new JetTestInstanceFactory();
        jet = factory.newMember(config);
        final JetInstance jet2 = factory.newMember(config);
        warmUpPartitions(asList(jet.getHazelcastInstance(), jet2.getHazelcastInstance()));
        assertEquals(2, jet.getCluster().getMembers().size());
        docWords = jet.getList(DOCS_WORDS);
        docIndex = jet.getMap(WORD_DOCSCORES);
        prepareKnownData();
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    private static <T> T print(T t) {
        System.out.println(t);
        return t;
    }

    private void prepareRandomData() {
        for (long docId = 0; docId < DOC_COUNT; docId++) {
            for (int i = 0; i < WORDS_PER_DOC; i++) {
                docWords.add(new SimpleImmutableEntry<>(docId, randomWord()));
            }
        }
    }

    private String randomWord() {
        final Random rnd = ThreadLocalRandom.current();
        sb.setLength(0);
        for (int i = 0; i < WORD_LENGTH; i++) {
            sb.append((char) ('a' + rnd.nextInt(CHARSET_SIZE)));
        }
        return sb.toString();
    }

    private void prepareKnownData() {
        long[] i = {0};
        Stream.of(
                Stream.of("1", "12", "13", "123"),
                Stream.of("2", "12", "12", "23", "123"),
                Stream.of("13", "23", "123")
        )
              .peek(x -> i[0]++)
              .forEach(s -> s.forEach(w -> docWords.add(new SimpleImmutableEntry<>(i[0], w))));
    }

    @Test
    public void tfIdfTest() throws Throwable {
        final Distributed.Function<Entry<Entry<Long, String>, Long>, String> wordFromTfTuple =
                (Entry<Entry<Long, String>, Long> e) -> e.getKey().getValue();

        // nil -> (docId, word)
        final Vertex source = new Vertex("source", listReader(DOCS_WORDS)).localParallelism(1);
        // (docId, word) -> ((docId, word), count)
        final Vertex tf = new Vertex("tf",
                groupingAccumulator((Long count, Entry<Long, String> x) -> (count == null ? 0L : count) + 1));
        // ((docId, word), count) -> localDocCount
        final Vertex localDistinctDocs = new Vertex("local-distinct-docids", groupingAccumulator(
                (Entry<Entry<Long, String>, Long> e) -> e.getKey().getKey(),
                (acc, item) -> true,
                (docId, acc) -> docId))
                .localParallelism(1);
        final Vertex docCount = new Vertex("total-doc-count", distinctCounter()).localParallelism(1);
        // ((docId, word), Long ignored) -> (word, docCount)
        final Vertex df = new Vertex("df", groupingAccumulator(wordFromTfTuple,
                (Long count, Entry<Entry<Long, String>, Long> e) -> (count == null ? 0L : count) + 1));
        // 0: totalDocCount, 1: (word, docCount) -> (word, idf)
        final Vertex idf = new Vertex("idf", IdfP::new);
        // 0: (word, idf), 1: ((docId, word), count) -> (word, list of (docId, tf-idf-score))
        final Vertex tfidf = new Vertex("tf-idf", TfIdfP::new);
        final Vertex sink = new Vertex("sink", mapWriter(WORD_DOCSCORES));

        executeAndPeel(jet.newJob(new DAG()
                .addVertex(source)
                .addVertex(tf)
                .addVertex(localDistinctDocs)
                .addVertex(docCount)
                .addVertex(df)
                .addVertex(idf)
                .addVertex(tfidf)
                .addVertex(sink)

                .addEdge(new Edge(source, tf).distributed().partitionedByKey(Entry<Long, String>::getValue))
                .addEdge(new Edge(tf, localDistinctDocs).allToOne())
                .addEdge(new Edge(localDistinctDocs, docCount).distributed().broadcast())
                .addEdge(new Edge(tf, 1, df, 0).partitionedByKey(wordFromTfTuple))
                .addEdge(new Edge(docCount, 0, idf, 0).priority(0).broadcast())
                .addEdge(new Edge(df, 0, idf, 1).priority(1).partitionedByKey(Entry<String, Long>::getKey))
                .addEdge(new Edge(idf, 0, tfidf, 0).priority(0).partitionedByKey(Entry<String, Long>::getKey))
                .addEdge(new Edge(tf, 2, tfidf, 1).priority(1).partitionedByKey(wordFromTfTuple)
                                                  .setConfig(new EdgeConfig().setHighWaterMark(Integer.MAX_VALUE)))
                .addEdge(new Edge(tfidf, sink))
        ));

//        docIndex.entrySet().forEach(System.out::println);
        search("12", "23");
    }

    void search(String... terms) {
        System.out.println("\n\n Searching for " + Arrays.stream(terms).collect(joining(" ")));
        Arrays.stream(terms)
              .flatMap(term -> docIndex.get(term).stream())
              .collect(groupingBy(Entry::getKey, summingDouble(Entry::getValue)))
              .entrySet().stream()
              .sorted(comparingDouble(Entry<Long, Double>::getValue).reversed())
              .forEach(System.out::println);
        System.out.println("\n\n");
    }

    private static class IdfP extends AbstractProcessor {
        private Double totalDocCount;

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            if (ordinal == 0) {
                totalDocCount = (double) (Long) item;
                return true;
            }
            assert totalDocCount != null : "Failed to obtain the total doc count in time";
            final Entry<String, Long> entry = (Entry<String, Long>) item;
            final String word = entry.getKey();
            final long docCount = entry.getValue();
            emit(new SimpleImmutableEntry<>(word, Math.log(totalDocCount / docCount)));
            return true;
        }
    }

    private static class TfIdfP extends AbstractProcessor {
        private Map<String, Double> wordIdf = new HashMap<>();
        private Map<String, List<Entry<Long, Double>>> wordDocScore = new HashMap<>();
        private Supplier<Entry<String, List<Entry<Long, Double>>>> docScoreSupplier =
                peek(lazyIterate(() -> wordDocScore.entrySet().iterator()),
                        e -> e.getValue().sort(comparingDouble(Entry<Long, Double>::getValue).reversed()));

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            if (ordinal == 0) {
                final Entry<String, Double> e = (Entry<String, Double>) item;
                wordIdf.put(e.getKey(), e.getValue());
                return true;
            }
            assert ordinal == 1;
            final Entry<Entry<Long, String>, Long> e = (Entry<Entry<Long, String>, Long>) item;
            final long docId = e.getKey().getKey();
            final String word = e.getKey().getValue();
            final long tf = e.getValue();
            final double idf = wordIdf.get(word);
            wordDocScore.computeIfAbsent(word, w -> new ArrayList<>()).add(
                    new SimpleImmutableEntry<>(docId, tf * idf));
            return true;
        }

        @Override
        public boolean complete() {
            final boolean done = emitCooperatively(docScoreSupplier);
            if (done) {
                wordIdf = null;
                wordDocScore = null;
                docScoreSupplier = null;
            }
            return done;
        }
    }
}
