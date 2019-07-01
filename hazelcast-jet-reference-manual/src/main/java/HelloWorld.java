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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.Functions.wholeItem;

public class HelloWorld {
    public static void main(String[] args) {
        // Create the specification of the computation pipeline. Note
        // it's a pure POJO: no instance of Jet needed to create it.
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String>list("text"))
         .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
         .filter(word -> !word.isEmpty())
         .groupingKey(wholeItem())
         .aggregate(counting())
         .drainTo(Sinks.map("counts"));

        // Start Jet, populate the input list
        JetInstance jet = Jet.newJetInstance();
        try {
            List<String> text = jet.getList("text");
            text.add("hello world hello hello world");
            text.add("world world hello world");

            // Perform the computation
            jet.newJob(p).join();

            // Check the results
            Map<String, Long> counts = jet.getMap("counts");
            System.out.println("Count of hello: "
                    + counts.get("hello"));
            System.out.println("Count of world: "
                    + counts.get("world"));
        } finally {
            Jet.shutdownAll();
        }
    }
}
