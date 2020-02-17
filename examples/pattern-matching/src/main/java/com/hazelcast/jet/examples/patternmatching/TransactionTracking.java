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

package com.hazelcast.jet.examples.patternmatching;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.examples.patternmatching.support.TransactionEvent;
import com.hazelcast.jet.examples.patternmatching.support.TransactionStatusGui;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.examples.patternmatching.support.TransactionGenerator.transactionEventSource;
import static com.hazelcast.jet.examples.patternmatching.support.TransactionStatusGui.PENDING_CODE;
import static com.hazelcast.jet.examples.patternmatching.support.TransactionStatusGui.TIMED_OUT_CODE;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This code sample shows you how to approach pattern matching with
 * Hazelcast Jet. It simulates a stream of transaction-related events:
 * <em>start-transaction</em> and <em>end-transaction</em>. It partitions
 * the stream by transaction ID and applies the <em>stateful mapping</em>
 * transform to match up the start and end events and find the duration of
 * each transaction.
 * <p>
 * To detect a transaction timeout (a case where the <em>end-transaction</em>
 * event doesn't occur within a specified period) it relies on the TTL
 * feature of the stateful mapping stage and reacts to the state eviction
 * event by emitting a special TIMED_OUT value.
 * <p>
 * The sample opens a GUI window that visualizes the output of the pipeline.
 */
public final class TransactionTracking {

    private static final int EVENTS_PER_SECOND = 20;
    private static final String STATUS_MAP_NAME = "transactionStatus";
    private static final int TRANSACTION_TIMEOUT_SECONDS = 2;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        StreamSource<TransactionEvent> source = transactionEventSource(EVENTS_PER_SECOND);
        p.readFrom(source).withTimestamps(TransactionEvent::timestamp, 0)
         .groupingKey(TransactionEvent::transactionId)
         .mapStateful(
                 SECONDS.toMillis(TRANSACTION_TIMEOUT_SECONDS),
                 () -> new TransactionEvent[2],
                 (startEnd, transactionId, transactionEvent) -> {
                     switch (transactionEvent.type()) {
                         case START:
                             startEnd[0] = transactionEvent;
                             break;
                         case END:
                             startEnd[1] = transactionEvent;
                             break;
                         default:
                             System.out.println("Wrong event in the stream: " + transactionEvent.type());
                     }
                     TransactionEvent startEvent = startEnd[0];
                     TransactionEvent endEvent = startEnd[1];
                     return (startEvent != null && endEvent != null) ? entry(
                             transactionId, endEvent.timestamp() - startEvent.timestamp())
                         : (startEvent != null) ? entry(transactionId, PENDING_CODE)
                         : null;
                 },
                 (startEnd, transactionId, wm) -> (startEnd[0] != null && startEnd[1] == null)
                         ? entry(transactionId, TIMED_OUT_CODE)
                         : null
         ).writeTo(Sinks.map(STATUS_MAP_NAME));
        return p;
    }

    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        try {
            new TransactionStatusGui(jet.getMap(STATUS_MAP_NAME));
            jet.newJob(buildPipeline()).join();
        } finally {
            Jet.shutdownAll();
        }
    }
}
