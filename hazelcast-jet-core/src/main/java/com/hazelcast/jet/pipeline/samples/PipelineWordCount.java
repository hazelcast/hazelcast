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

package com.hazelcast.jet.pipeline.samples;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.ComputeStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.stream.IStreamMap;

import java.util.Map.Entry;
import java.util.regex.Pattern;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.pipeline.samples.PipelineJoinAndCoGroup.assertEquals;
import static com.hazelcast.jet.pipeline.samples.PipelineJoinAndCoGroup.printImap;

public class PipelineWordCount {

    public static void main(String[] args) throws Exception {

        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            final Pattern delimiter = Pattern.compile("\\W+");

            Pipeline p = Pipeline.create();

            ComputeStage<String> books = p.drawFrom(Sources.readFiles("books"));

            ComputeStage<Entry<String, Long>> wordCounts = books
                    .flatMap((String line) -> traverseArray(delimiter.split(line.toLowerCase()))
                            .filter(word -> !word.isEmpty()))
                    .groupBy(wholeItem(), AggregateOperations.counting());

            wordCounts.drainTo(Sinks.writeMap("counts"));

            wordCounts
                    .groupBy(e -> true, AggregateOperations.summingLong(Entry::getValue))
                    .drainTo(Sinks.writeMap("totals"));

            p.execute(jet).get();

            IMap<String, Long> counts = jet.getMap("counts");
            printImap(jet.getMap("totals"));
            assertEquals(2 * 14417, counts.get("the"));
            System.err.println("Count of 'the' is valid");
        } finally {
            Jet.shutdownAll();
        }
    }
}
