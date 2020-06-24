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

package com.hazelcast.jet.examples.helloworld;

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Demonstrates a simple job which calculates the top 10 numbers from a
 * stream of random numbers. This code is included in Jet's distribution
 * package as {@code examples/hello-world.jar}, ready to be submitted to
 * a running Jet cluster with {@code bin/jet submit examples/hello-world.jar}.
 * <p>
 * It also uses an {@link Observable} to print the results on the client side.
 */
public class HelloWorld {

    public static final int TOP = 10;
    private static final String RESULTS = "top10_results";

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(100, (ts, seq) -> nextRandomNumber()))
                .withIngestionTimestamps()
                .window(WindowDefinition.tumbling(1000))
                .aggregate(AggregateOperations.topN(TOP, ComparatorEx.comparingLong(l -> l)))
                .map(WindowResult::result)
                .writeTo(Sinks.observable(RESULTS));
        return p;
    }

    private static long nextRandomNumber() {
        return ThreadLocalRandom.current().nextLong();
    }

    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();

        Observable<List<Long>> observable = jet.getObservable(RESULTS);
        observable.addObserver(Observer.of(HelloWorld::printResults));

        Pipeline p = buildPipeline();

        JobConfig config = new JobConfig();
        config.setName("hello-world");
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jet.newJobIfAbsent(p, config).join();
    }

    private static void printResults(List<Long> topNumbers) {
        StringBuilder sb = new StringBuilder(String.format("\nTop %d random numbers in the latest window: ", TOP));
        for (int i = 0; i < topNumbers.size(); i++) {
            sb.append(String.format("\n\t%d. %,d", i + 1, topNumbers.get(i)));
        }
        System.out.println(sb.toString());
    }

}
