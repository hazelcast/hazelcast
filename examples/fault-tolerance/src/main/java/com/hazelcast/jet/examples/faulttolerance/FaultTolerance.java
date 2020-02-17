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

package com.hazelcast.jet.examples.faulttolerance;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.map.IMap;

import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;

/**
 * A simple application which uses Jet with the event journal reader for
 * {@code IMap} to perform rolling average calculations and
 * illustrates the differences in processing guarantees.
 * <p>
 * A price updater thread keeps updating the current price of each stock
 * and writes the new value, along with a timestamp into a distributed
 * {@code IMap}.
 * <p>
 * A price analyzer DAG consumes events from this map, and performs a
 * rolling average calculation per ticker using the given window sizes. The
 * output is written to stdout via the logger sink.
 * <p>
 * Initially, there are two nodes in the cluster. After a given delay one
 * of the nodes will be shut down and the job automatically restarted.
 * The output after restart should be observed to see what happens when
 * different kinds of {@link ProcessingGuarantee}s are given.
 */
public class FaultTolerance {

    private static final String PRICES_MAP_NAME = "prices";
    private static final String[] TICKER_LIST = {"AAPL", "AMZN", "EBAY", "GOOG", "MSFT", "TSLA"};

    private static final long LAG_SECONDS = 5;
    private static final long WINDOW_SIZE_SECONDS = 10;
    private static final long SHUTDOWN_DELAY_SECONDS = 20;

    public static void main(String[] args) throws Exception {
        JobConfig config = new JobConfig();

        ////////////////////////////////////////////////////////////////////////////////
        // Configure this option to observe the output with different processing
        // guarantees
        //
        // Here we will lose data after shutting down the second node. You will see
        // a gap in the sequence number.
        config.setProcessingGuarantee(ProcessingGuarantee.NONE);
        //
        // Here we will not lose data after shutting down the second node but you might
        // see duplicates
        // config.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        //
        // Here we will not lose data after shutting down the second node and you will
        // not see duplicates.
        // config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        //
        ////////////////////////////////////////////////////////////////////////////////

        //default is 10 seconds, we are using 2 seconds
        config.setSnapshotIntervalMillis(2000);

        // create two instances
        JetInstance instance1 = Jet.newJetInstance();
        JetInstance instance2 = Jet.newJetInstance();

        // create a client and submit the price analyzer pipeline
        JetInstance client = Jet.newJetClient();
        Job job = client.newJob(buildPipeline(), config);

        Thread.sleep(1000);

        System.out.println("******************************************");
        System.out.println("Starting price updater. You should start seeing the output after "
                + LAG_SECONDS + " seconds");
        System.out.println("After " + SHUTDOWN_DELAY_SECONDS + " seconds, one of the nodes will be shut down.");
        System.out.println("******************************************");
        // start price updater thread to start generating events
        new Thread(() -> updatePrices(instance1)).start();

        Thread.sleep(SHUTDOWN_DELAY_SECONDS * 1000);
        instance2.shutdown();

        System.out.println("Member shut down, the job will now restart and you can inspect the output again.");
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<PriceUpdateEvent, String, Tuple2<Integer, Long>>mapJournal(
                "prices",
                START_FROM_CURRENT,
                e -> new PriceUpdateEvent(e.getKey(), e.getNewValue().f0(), e.getNewValue().f1()), mapPutEvents()
        ))
         .withTimestamps(PriceUpdateEvent::timestamp, LAG_SECONDS * 1000)
         .setLocalParallelism(1)
         .groupingKey(PriceUpdateEvent::ticker)
         .window(WindowDefinition.sliding(WINDOW_SIZE_SECONDS * 1000, 1000))
         .aggregate(AggregateOperations.counting())
         .writeTo(Sinks.logger());
        return p;
    }

    private static void updatePrices(JetInstance jet) {
        IMap<String, Tuple2<Integer, Long>> prices = jet.getMap(PRICES_MAP_NAME);
        int price = 100;
        long timestamp = 0;
        while (true) {
            for (String ticker : TICKER_LIST) {
                prices.put(ticker, tuple2(price, timestamp));
            }
            price++;
            timestamp += 1000;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }
}
