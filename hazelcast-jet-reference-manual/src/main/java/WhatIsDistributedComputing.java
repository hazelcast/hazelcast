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

import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.function.Functions.wholeItem;

public class WhatIsDistributedComputing {
    static void s1() {
        //tag::s1[]
        Pattern delimiter = Pattern.compile("\\W+");
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>map("book-lines"))
         .flatMap(e -> traverseArray(delimiter.split(e.getValue().toLowerCase())))
         .filter(word -> !word.isEmpty())
         .groupingKey(wholeItem())
         .aggregate(AggregateOperations.counting())
         .drainTo(Sinks.map("counts"));
        //end::s1[]
    }

    static void s2() {
        List<String> lines = new ArrayList<>();
        //tag::s2[]
        Map<String, Long> counts =
                lines.stream()
                     .flatMap(line -> Arrays.stream(line.toLowerCase().split("\\W+")))
                     .filter(word -> !word.isEmpty())
                     .collect(Collectors.groupingBy(word -> word, Collectors.counting()));
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        List<String> lines = someExistingList();
        Map<String, Long> counts = new HashMap<>();
        for (String line : lines) {
            for (String word : line.toLowerCase().split("\\W+")) {
                if (!word.isEmpty()) {
                    counts.merge(word, 1L, (count, one) -> count + one);
                }
            }
        }
        //end::s3[]
    }

    static void s4() {
        //tag::s4[]
        // Source thread
        for (String line : readLines()) {
            emit(line);
        }
        //end::s4[]
    }
    private static Iterable<String> readLines() { return null; }
    private static void emit(Object x) {}

    private static class S5 {
        static void s5() {
            //tag::s5[]
            // Tokenizer thread
            for (String line : receive()) {
                for (String word : line.toLowerCase().split("\\W+")) {
                    if (!word.isEmpty()) {
                        emit(word);
                    }
                }
            }
            //end::s5[]
        }
        private static Iterable<String> receive() { return null; }
    }

    private static class S6 {
        static void s6() {
            //tag::s6[]
            // Accumulator thread
            Map<String, Long> counts = new HashMap<>();
            for (String word : receive()) {
                counts.merge(word, 1L, (count, one) -> count + one);
            }
            // finally, when done receiving:
            for (Entry<String, Long> wordAndCount : counts.entrySet()) {
                emit(wordAndCount);
            }
            //end::s6[]
        }
        private static Iterable<String> receive() { return null; }
    }

    private static class S7 {
        static void s7() {
            //tag::s7[]
            // Combining vertex
            Map<String, Long> combined = new HashMap<>();
            for (Entry<String, Long> wordAndCount : receive()) {
                combined.merge(wordAndCount.getKey(), wordAndCount.getValue(),
                        (accCount, newCount) -> accCount + newCount);
            }
            // finally, when done receiving:
            for (Entry<String, Long> wordAndCount : combined.entrySet()) {
                emit(wordAndCount);
            }
            //end::s7[]
        }
        private static Iterable<? extends Entry<String, Long>> receive() { return null; }
    }

    static void s8() {
        //tag::s8[]
        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        //end::s9[]
    }

    private static List<String> someExistingList() {
        return null;
    }
}
