/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.Processors.aggregateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingContextP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.function.Functions.wholeItem;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class TfIdfCoreApi {

    private static final Pattern DELIMITER = Pattern.compile("\\W+");
    private static final String DOCID_NAME = "docId_name";
    private static final String INVERTED_INDEX = "inverted-index";

    private static DAG createDag() {
        FunctionEx<Entry<Entry<?, String>, ?>, String> byWord = item -> item.getKey().getValue();
        BiFunctionEx<Long, Object, Long> counter = (count, x) -> count + 1;

        DAG dag = new DAG();

        Vertex stopwordSource =
        //tag::s2[]
        dag.newVertex("stopword-source", StopwordsP::new);
        //end::s2[]

        Vertex docSource =
        //tag::s1[]
        dag.newVertex("doc-source", readMapP(DOCID_NAME));
        //end::s1[]

        Vertex docCount =
        //tag::s4[]
        dag.newVertex("doc-count", Processors.aggregateP(counting()));
        //end::s4[]

        //tag::s5[]
        Vertex docLines = dag.newVertex("doc-lines", flatMapUsingContextP(
                ContextFactory.withCreateFn(jet -> null).toNonCooperative(),
                (Object ctx, Entry<Long, String> e) ->
                traverseStream(docLines("books/" + e.getValue())
                    .map(line -> entry(e.getKey(), line)))));
        //end::s5[]

        Vertex tokenize =
        //tag::s6[]
        dag.newVertex("tokenize", TokenizeP::new);
        //end::s6[]

        Vertex tf =
        //tag::s9[]
        dag.newVertex("tf", aggregateByKeyP(
                singletonList(wholeItem()), counting(), Util::entry));
        //end::s9[]

        Vertex tfidf =
        //tag::s10[]
        dag.newVertex("tf-idf", TfIdfP::new);
        //end::s10[]

        Vertex sink =
        //tag::s12[]
        dag.newVertex("sink", SinkProcessors.writeMapP(INVERTED_INDEX));
        //end::s12[]

        stopwordSource.localParallelism(1);
        docSource.localParallelism(1);
        docCount.localParallelism(1);
        docLines.localParallelism(1);

        //tag::s8[]
        dag.edge(between(stopwordSource, tokenize).broadcast().priority(-1))
           .edge(from(docLines).to(tokenize, 1));
        //end::s8[]

        return dag
                .edge(between(docSource, docCount).distributed().broadcast())
                .edge(from(docSource, 1).to(docLines))
                .edge(between(tokenize, tf).partitioned(wholeItem(), HASH_CODE))
                .edge(between(docCount, tfidf).broadcast().priority(-1))
                .edge(from(tf).to(tfidf, 1).distributed().partitioned(byWord, HASH_CODE))
                .edge(between(tfidf, sink));
    }

    private static Stream<String> docLines(String name) {
        try {
            return Files.lines(Paths.get(TfIdfCoreApi.class.getResource(name).toURI()));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    //tag::s3[]
    private static class StopwordsP extends AbstractProcessor {
        @Override
        public boolean complete() {
            return tryEmit(docLines("stopwords.txt").collect(toSet()));
        }
    }
    //end::s3[]

    //tag::s7[]
    private static class TokenizeP extends AbstractProcessor {
        private Set<String> stopwords;
        private final FlatMapper<Entry<Long, String>, Entry<Long, String>>
                flatMapper = flatMapper(e -> traverseStream(
                Arrays.stream(DELIMITER.split(e.getValue().toLowerCase()))
                      .filter(word -> !stopwords.contains(word))
                      .map(word -> entry(e.getKey(), word))));

        @Override
        @SuppressWarnings("unchecked")
        protected boolean tryProcess0(@Nonnull Object item) {
            stopwords = (Set<String>) item;
            return true;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean tryProcess1(@Nonnull Object item) {
            return flatMapper.tryProcess((Entry<Long, String>) item);
        }
    }
    //end::s7[]

    //tag::s11[]
    private static class TfIdfP extends AbstractProcessor {
        private double logDocCount;

        private final Map<String, List<Entry<Long, Double>>> wordDocTf =
                new HashMap<>();
        private final Traverser<Entry<String, List<Entry<Long, Double>>>>
                invertedIndexTraverser = lazy(() ->
                traverseIterable(wordDocTf.entrySet())
                        .map(this::toInvertedIndexEntry));

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            logDocCount = Math.log((Long) item);
            return true;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean tryProcess1(@Nonnull Object item) {
            Entry<Entry<Long, String>, Long> e =
                    (Entry<Entry<Long, String>, Long>) item;
            long docId = e.getKey().getKey();
            String word = e.getKey().getValue();
            long tf = e.getValue();
            wordDocTf.computeIfAbsent(word, w -> new ArrayList<>())
                     .add(entry(docId, (double) tf));
            return true;
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(invertedIndexTraverser);
        }

        private Entry<String, List<Entry<Long, Double>>> toInvertedIndexEntry(
                Entry<String, List<Entry<Long, Double>>> wordDocTf
        ) {
            String word = wordDocTf.getKey();
            List<Entry<Long, Double>> docidTfs = wordDocTf.getValue();
            return entry(word, docScores(docidTfs));
        }

        private List<Entry<Long, Double>> docScores(
                List<Entry<Long, Double>> docidTfs
        ) {
            double logDf = Math.log(docidTfs.size());
            return docidTfs.stream()
                           .map(tfe -> tfidfEntry(logDf, tfe))
                           .collect(toList());
        }

        private Entry<Long, Double> tfidfEntry(
                double logDf, Entry<Long, Double> docidTf
        ) {
            Long docId = docidTf.getKey();
            double tf = docidTf.getValue();
            double idf = logDocCount - logDf;
            return entry(docId, tf * idf);
        }
    }
    //end::s11[]
}
