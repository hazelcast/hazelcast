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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * Implementation of TF-IDF without Jet, with just the JDK code.
 */
public class TfIdfJdkStreams {

    static final Pattern DELIMITER = Pattern.compile("\\W+");

    Set<String> stopwords;
    Map<String, List<Entry<Long, Double>>> invertedIndex;
    Map<Long, String> docId2Name;

    void buildInvertedIndex() {

        // tag::s1[]
        Stream<Entry<Long, String>> docWords = docId2Name
                .entrySet()
                .parallelStream()
                .flatMap(TfIdfJdkStreams::docLines)
                .flatMap(this::tokenize);
        // end::s1[]

        // tag::s2[]
        final double logDocCount = Math.log(docId2Name.size());
        // end::s2[]

        // tag::s3[]
        Map<Entry<Long, String>, Long> tfMap = docWords
                .parallel()
                .collect(groupingBy(identity(), counting()));
        // end::s3[]

        // tag::s4[]
        invertedIndex = tfMap
            .entrySet()
            .parallelStream()
            .collect(groupingBy(
                e -> e.getKey().getValue(),
                collectingAndThen(
                    toList(),
                    entries -> {
                        double idf = logDocCount - Math.log(entries.size());
                        return entries.stream().map(e ->
                                tfidfEntry(e, idf)).collect(toList());
                    }
                )
            ));
        // end::s4[]
    }

    //tag::s5[]
    private static Entry<Long, Double> tfidfEntry(
            Entry<Entry<Long, String>, Long> tfEntry, Double idf
    ) {
        Long tf = tfEntry.getValue();
        return entry(tfEntry.getKey().getKey(), tf * idf);
    }
    //end::s5[]

    static Stream<Entry<Long, String>> docLines(Entry<Long, String> idAndName) {
        try {
            return Files.lines(Paths.get(""))
                        .map(String::toLowerCase)
                        .map(line -> entry(idAndName.getKey(), line));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Stream<Entry<Long, String>> tokenize(Entry<Long, String> docLine) {
        return Arrays.stream(DELIMITER.split(docLine.getValue()))
                     .filter(token -> !token.isEmpty())
                     .filter(token -> !stopwords.contains(token))
                     .map(word -> entry(docLine.getKey(), word));
    }
}
