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

package com.hazelcast.jet.examples.earlyresults;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.examples.tradesource.Trade;
import com.hazelcast.jet.examples.tradesource.TradeSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Showcases the Early Window Results feature of the Pipeline API.
 * <p>
 * The sample Jet pipeline uses a mock data source that generates random
 * trade events. The source also simulates the disorder in the event stream,
 * so some events come on time and some are late by up to five seconds.
 * <p>
 * The pipeline calculates for each 2-second period the total amount of
 * stocks that changed hands over that period. Since there's disorder in the
 * event stream, the final result for a given time period becomes known
 * only after a five-second delay.
 * <p>
 * We want to see some results immediately, even if incomplete, so we
 * enable early result emission on Jet's windowing operator. With this
 * change, Jet will keep emitting the current incomplete results as the
 * events are still coming in.
 * <p>
 * The sample also starts a GUI window that visualizes the progress of
 * early results gradually becoming complete. Each bar in the chart
 * represents a two-second window, so with the disorder of 5 seconds,
 * there are three to four bars rising at any point in time.
 */
public final class TradingVolumeOverTime {

    private static final String VOLUME_LIST_NAME = "trading-volume";
    private static final int TRADES_PER_SEC = 3_000;
    private static final int MAX_LAG = 5000;
    private static final int DURATION_SECONDS = 55;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TradeSource.tradeStream(1, TRADES_PER_SEC, MAX_LAG))
         .withNativeTimestamps(MAX_LAG)
         .window(tumbling(SECONDS.toMillis(2))
                 // comment out this line to see how the chart behaves without early results:
                 .setEarlyResultsPeriod(20)
         )
         .aggregate(summingLong(Trade::getQuantity))
         .writeTo(Sinks.list(VOLUME_LIST_NAME));
        return p;
    }

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.bootstrappedInstance();
        new TradingVolumeGui(jet.getList(VOLUME_LIST_NAME));
        try {
            Job job = jet.newJob(buildPipeline());
            SECONDS.sleep(DURATION_SECONDS);
            job.cancel();
            job.join();
        } finally {
            Jet.shutdownAll();
        }
    }
}
