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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Metrics;
import com.hazelcast.jet.core.metrics.Unit;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.ServiceFactories.nonSharedService;
import static java.util.Arrays.asList;

public class LogDebug {
    static void s1() {
        //tag::s1[]
        System.setProperty("hazelcast.logging.type", "log4j");
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        JetConfig config = new JetConfig();
        config.getHazelcastConfig()
              .setProperty("hazelcast.logging.type", "log4j");
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list("inputList"))
         .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
         .filter(word -> !word.isEmpty())
         .peek() // <1>
         .groupingKey(wholeItem())
         .aggregate(counting())
         .writeTo(Sinks.map("counts"));
        //end::s3[]

        //tag::s4[]
        JetInstance jet = Jet.newJetInstance();
        try {
            jet.getList("inputList")
               .addAll(asList("The quick brown fox", "jumped over the lazy dog"));
            jet.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
        //end::s4[]
    }

    static void s5() {
        BatchSource<Long> source = TestSources.items(0L, 1L, 2L, 3L, 4L);
        Sink<Long> sink = Sinks.logger();
        Pipeline p = Pipeline.create();

        //tag::s5[]
        p.readFrom(source)
         .filter(l -> {
             boolean pass = l % 2 == 0;
             if (!pass) {
                 Metrics.metric("dropped").increment();
             }
             Metrics.metric("total").increment();
             return pass;
         })
         .writeTo(sink);
        //end::s5[]

        JetInstance jet = Jet.newJetInstance();
        Job job = jet.newJob(p);
        job.join();
    }

    static void s6() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(0, 1, 2, 3))
            //tag::s6[]
            .mapUsingServiceAsync(
                nonSharedService(pctx -> 10L),
                (ctx, item) -> CompletableFuture.supplyAsync(
                    () -> {
                        Metrics.metric("mapped").increment();
                        return item * ctx;
                    }
                )
            )
            //end::s6[]
            .writeTo(Sinks.logger());
    }

    static void s7() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(0, 1, 2, 3))
            //tag::s7[]
            .mapUsingServiceAsync(
                nonSharedService(pctx -> 10L),
                (ctx, item) -> {
                    // need to use thread-safe metric since it will be mutated from another thread
                    Metric mapped = Metrics.threadSafeMetric("mapped", Unit.COUNT);
                    return CompletableFuture.supplyAsync(
                        () -> {
                            mapped.increment();
                            return item * ctx;
                        }
                    );
                }
            )
            //end::s7[]
            .writeTo(Sinks.logger());
    }

    static void s8() {
        //tag::s8[]
        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        //end::s9[]
    }

    static void s10() {
        //tag::s10[]
        //end::s10[]
    }

    static void s11() {
        //tag::s11[]
        //end::s11[]
    }

}
