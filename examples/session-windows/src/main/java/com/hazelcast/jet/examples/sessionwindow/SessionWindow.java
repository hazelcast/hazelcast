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

package com.hazelcast.jet.examples.sessionwindow;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Set;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.mapping;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

/**
 * Demonstrates the usage of a {@link WindowDefinition#session session
 * window} to track the behavior of the users in an online shop. It
 * handles two kinds of events:
 * <ol><li>
 *     user opened a product listing page
 * </li><li>
 *     user bought a product
 * </li></ol>
 * It identifies a user bi her {@code userId} and infers the duration of
 * a single user session from the spread between adjacent events by the
 * same user. Any period longer than the session timeout without further
 * events from the same user ends the session window and allows its results
 * to be emitted. The aggregated results of a session consist of two items:
 * the total number of product listing views and the set of purchased items.
 */
public class SessionWindow {

    private static final long JOB_DURATION_MS = 60_000;
    private static final int SESSION_TIMEOUT = 5_000;

    private static Pipeline buildPipeline() {
        // The composite aggregate operation computes two metrics for each user session:
        // 1. How many times the user opened a product page
        // 2. How many items the user purchased
        AggregateOperation1<ProductEvent, ?, Tuple2<Long, Set<String>>> aggrOp = allOf(
                summingLong(e -> e.getProductEventType() == ProductEventType.VIEW_LISTING ? 1 : 0),
                mapping(e -> e.getProductEventType() == ProductEventType.PURCHASE ? e.getProductId() : null, toSet())
        );

        Pipeline p = Pipeline.create();
        p.readFrom(eventsSource())
         .withTimestamps(ProductEvent::getTimestamp, 0)
         .groupingKey(ProductEvent::getUserId)
         .window(WindowDefinition.session(SESSION_TIMEOUT))
         .aggregate(aggrOp)
         .writeTo(Sinks.logger(SessionWindow::sessionToString));
        return p;
    }

    private static StreamSource<ProductEvent> eventsSource() {
        return Sources.streamFromProcessor("generator", preferLocalParallelismOne(GenerateEventsP::new));
    }

    @Nonnull
    private static String sessionToString(KeyedWindowResult<String, Tuple2<Long, Set<String>>> wr) {
        return String.format("Session{userId=%s, start=%s, duration=%2ds, value={viewed=%2d, purchases=%s}",
                wr.key(), // userId
                Instant.ofEpochMilli(wr.start()).atZone(ZoneId.systemDefault()).toLocalTime(), // session start
                Duration.ofMillis(wr.end() - wr.start()).getSeconds(), // session duration
                wr.result().f0(),  // number of viewed listings
                wr.result().f1()); // set of purchased products
    }

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            jet.newJob(buildPipeline());
            Thread.sleep(JOB_DURATION_MS);
        } finally {
            Jet.shutdownAll();
        }
    }
}
