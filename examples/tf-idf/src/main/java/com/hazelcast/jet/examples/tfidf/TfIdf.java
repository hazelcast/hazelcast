/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.tfidf;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.function.Functions.entryValue;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.toMap;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Builds, for a given set of text documents, an <em>inverted index</em>
 * that maps each word to the set of documents that contain it. It assigns
 * to each document in the set a TF-IDF score which tells how relevant the
 * document is to the search term. In short,
 * <ul><li>
 *     {@code TF(document, word)} is <em>term frequency</em>: the number of
 *     occurrences of a given word in a given document. {@code TF} is expected
 *     to correlate with the relevance of the word to the document.
 * </li><li>
 *     Let {@code DF(word)} be the <em>document frequency</em> of a word: the
 *     number of documents a given word occurs in.
 * </li><li>
 *     {@code IDF(word)} is the <em>inverse document frequency</em> of a word:
 *     {@code log(D/DF)} where {@code D} is the overall number of documents.
 *     IDF is expected to correlate with the salience of the word: a high value
 *     means it's highly specific to the documents it occurs in. For example,
 *     words like "in" and "the" have an IDF of zero because they occur
 *     everywhere.
 * </li><li>
 *     {@code TF-IDF(document, word)} is the product of {@code TF * IDF} for a
 *     given word in a given document.
 * </li><li>
 *     A word that occurs in all documents has an IDF score of zero, therefore
 *     its TF-IDF score is also zero for any document. Such words are called
 *     <em>stopwords</em> and can be eliminated both from the inverted index and
 *     from the search phrase as an optimization.
 * </li></ul>
 * When you run the program, it will first spend a couple of seconds
 * building the inverted index using Hazelcast Jet and then a GUI window
 * will show up where you can use the index to perform text searches over
 * the documents.
 * <p>
 * When you enter a search phrase, the program first crosses out the
 * stopwords, then looks up each remaining search term in the inverted
 * index, resulting in a set of documents for each search term. It takes an
 * intersection of all these sets, which gives us only the documents that
 * contain all the search terms. For each combination of document and search
 * term there will be an associated TF-IDF score. It sums up these scores
 * per document to retrieve the total score of each document. Finally, it
 * sorts the list of documents by score (descending) and presents them to
 * the user as the search result.
 **/
public class TfIdf {

    private static final Pattern DELIMITER = Pattern.compile("\\W+");
    private static final String INVERTED_INDEX = "inverted-index";
    private static final String CONSTANT_KEY = "constant";

    private JetInstance jet;

    private static Pipeline buildPipeline() {
        Path bookDirectory = getClasspathDirectory("/books");
        // This set will be serialized as a part of the Pipeline definition that is
        // sent for execution.
        Set<String> stopwords = docLines("/stopwords.txt").collect(toSet());
        Pipeline p = Pipeline.create();

        // (filename, line) pairs
        BatchStage<Entry<String, String>> bookLines = p.readFrom(
                Sources.filesBuilder(bookDirectory.toString())
                       .build(Util::entry));

        // Contains a single item: logarithm of the number of files being indexed
        BatchStage<Double> logDocCountStage = bookLines
                .groupingKey(Entry::getKey)
                .distinct()
                .aggregate(counting())
                .map(Math::log);

        // pairs (filename, word)
        BatchStage<Entry<String, String>> bookWords = bookLines
                .flatMap(entry -> {
                    // split the line to words, convert to lower case, filter out stopwords
                    // and emit as entry(fileName, word)
                    String filename = entry.getKey();
                    return traverseArray(DELIMITER.split(entry.getValue()))
                            .map(rawWord -> {
                                String word = rawWord.toLowerCase();
                                return stopwords.contains(word) ? null : entry(filename, word);
                            });
                });

        // pairs (filename, map(word -> count))
        BatchStage<Entry<String, Map<String, Long>>> tf = bookWords
                .groupingKey(entryValue()) // entry value is the word
                .aggregate(toMap(entryKey(), e -> 1L, Long::sum));

        // pairs (word, pairs (filename, score))
        BatchStage<Entry<String, Collection<Entry<String, Double>>>> idf = tf.hashJoin(
                logDocCountStage,
                JoinClause.onKeys(x -> CONSTANT_KEY, x -> CONSTANT_KEY),
                (tfPair, logDocCount) -> entry(
                        tfPair.getKey(),
                        docScores(logDocCount, tfPair.getValue().entrySet()))
        );
        // INVERTED_INDEX is a map of (word -> pairs (filename, score))
        idf.writeTo(Sinks.map(INVERTED_INDEX));
        return p;
    }

    private static List<Entry<String, Double>> docScores(
            double logDocCount, Collection<Entry<String, Long>> filenameAndTFs
    ) {
        double logDf = Math.log(filenameAndTFs.size());
        return filenameAndTFs.stream()
                       .map(tfe -> tfidfEntry(logDocCount, logDf, tfe))
                       .collect(toList());
    }

    private static Entry<String, Double> tfidfEntry(double logDocCount, double logDf, Entry<String, Long> docIdTf) {
        String docId = docIdTf.getKey();
        double tf = docIdTf.getValue();
        double idf = logDocCount - logDf;
        return entry(docId, tf * idf);
    }

    public static void main(String[] args) {
        try {
            new TfIdf().go();
        } catch (Throwable t) {
            Jet.shutdownAll();
            throw t;
        }
    }

    private void go() {
        System.out.println("Creating Jet instance 1");
        jet = Jet.bootstrappedInstance();
        buildInvertedIndex();
        System.out.println("size=" + jet.getMap(INVERTED_INDEX).size());
        new SearchGui(jet.getMap(INVERTED_INDEX), docLines("/stopwords.txt").collect(toSet()));
    }

    private void buildInvertedIndex() {
        Job job = jet.newJob(buildPipeline());
        long start = System.nanoTime();
        job.join();
        System.out.println("Indexing took " + NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
    }

    private static Path getClasspathDirectory(String name) {
        try {
            return Paths.get(TfIdf.class.getResource(name).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<String> docLines(String name) {
        try {
            return Files.lines(Paths.get(TfIdf.class.getResource(name).toURI()));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
