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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Jet.newJetInstance;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class GetStarted {
    void s1a() {
        //tag::s1a[]
        List<String> strings = Arrays.asList("a", "b");

        List<String> uppercased = strings
                .stream()
                .map(String::toUpperCase)
                .collect(toList());

        uppercased.forEach(System.out::println);
        //end::s1a[]
    }

    void s1b() {
        //tag::s1b[]
        JetInstance jet = newJetInstance();
        IList<String> strings = jet.getList("strings");
        strings.addAll(Arrays.asList("a", "b"));
        IList<String> uppercased = jet.getList("uppercased");

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.list(strings))
                .map(String::toUpperCase)
                .drainTo(Sinks.list(uppercased));
        jet.newJob(pipeline).join();

        uppercased.forEach(System.out::println);
        //end::s1b[]
    }

    void s2a() {
        //tag::s2a[]
        List<String> strings = Arrays.asList("a", "b", "aa", "bb");
        Map<Integer, Long> histogram = strings
                .stream()
                .collect(groupingBy(String::length, Collectors.counting()));

        histogram.forEach((length, count) -> System.out.format(
                "%d chars: %d occurrences%n", length, count));
        //end::s2a[]
    }

    void s2b() {
        //tag::s2b[]
        JetInstance jet = newJetInstance();
        IList<String> strings = jet.getList("strings");
        strings.addAll(Arrays.asList("a", "b", "aa", "bb"));
        IMap<Integer, Long> histogram = jet.getMap("histogram");

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.list(strings))
                .groupingKey(String::length)
                .aggregate(AggregateOperations.counting())
                .drainTo(Sinks.map(histogram));
        jet.newJob(pipeline).join();

        histogram.forEach((length, count) -> System.out.format(
                "%d chars: %d occurrences%n", length, count));
        //end::s2b[]
    }

    void s3a() {
        //tag::s3a[]
        Collector<Long, LongAccumulator, Long> summingLong = Collector.of(
                LongAccumulator::new,
                (LongAccumulator acc, Long t) -> acc.add(t),
                (acc0, acc1) -> acc0.add(acc1),
                LongAccumulator::get
        );
        //end::s3a[]
    }

    void s3b() {
        //tag::s3b[]
        AggregateOperation1<Long, LongAccumulator, Long> summingLong = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((LongAccumulator acc, Long t) -> acc.add(t))
                .andCombine((acc0, acc1) -> acc0.add(acc1))
                .andDeduct((acc0, acc1) -> acc0.subtract(acc1))
                .andExportFinish(LongAccumulator::get);
        //end::s3b[]
    }
}
