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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of TF-IDF without Jet, with just the JDK code.
 */
public class TfIdfJdkStreams {

    static final Pattern DELIMITER = Pattern.compile("\\W+");

    private Set<String> stopwords;
    private Map<String, List<Entry<String, Double>>> invertedIndex;
    private Set<String> docIds;

    public static void main(String[] args) {
        new TfIdfJdkStreams().go();
    }

    private void go() {
        stopwords = readStopwords();
        docIds = buildDocumentInventory();
        final long start = System.nanoTime();
        buildInvertedIndex();
        System.out.println("Done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
        new SearchGui(invertedIndex, stopwords);
    }

    private void buildInvertedIndex() {
        final double logDocCount = Math.log(docIds.size());

        // stream of (docId, word)
        Stream<Entry<String, String>> docWords = docIds
                .parallelStream()
                .flatMap(TfIdfJdkStreams::docLines)
                .flatMap(this::tokenize);

        System.out.println("Building TF");
        // TF: (docId, word) -> count
        Map<Entry<String, String>, Long> tfMap = docWords
                .collect(groupingBy(identity(), counting()));

        System.out.println("Building inverted index");
        // Inverted index: word -> list of (docId, TF-IDF_score)
        invertedIndex = tfMap
                .entrySet()
                .parallelStream()
                .collect(groupingBy(
                        e -> e.getKey().getValue(),
                        collectingAndThen(
                                toList(),
                                entries -> {
                                    double idf = logDocCount - Math.log(entries.size());
                                    return entries.stream().map(e -> tfidfEntry(e, idf)).collect(toList());
                                }
                        )
                ));
    }

    private static Set<String> readStopwords() {
        try (BufferedReader r = resourceReader("stopwords.txt")) {
            return r.lines().collect(toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Set<String> buildDocumentInventory() {
        try (BufferedReader r = resourceReader("books")) {
            System.out.println("These books will be indexed:");
            return r.lines()
                    .peek(System.out::println)
                    .collect(toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Stream<Entry<String, String>> docLines(String name) {
        try {
            return Files.lines(Paths.get(TfIdfJdkStreams.class.getResource("/books/" + name).toURI()))
                        .map(String::toLowerCase)
                        .map(line -> entry(name, line));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static BufferedReader resourceReader(String resourceName) {
        final ClassLoader cl = TfIdfJdkStreams.class.getClassLoader();
        return new BufferedReader(new InputStreamReader(cl.getResourceAsStream(resourceName), UTF_8));
    }

    private Stream<Entry<String, String>> tokenize(Entry<String, String> docLine) {
        return Arrays.stream(DELIMITER.split(docLine.getValue()))
                     .filter(token -> !token.isEmpty())
                     .filter(token -> !stopwords.contains(token))
                     .map(word -> entry(docLine.getKey(), word));
    }

    // ((docId, word), count) -> (docId, tfIdf)
    private static Entry<String, Double> tfidfEntry(Entry<Entry<String, String>, Long> tfEntry, Double idf) {
        final Long tf = tfEntry.getValue();
        return entry(tfEntry.getKey().getKey(), tf * idf);
    }
}
