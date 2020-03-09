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

package com.hazelcast.jet.examples.rollingaggregation;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.examples.tradesource.Trade;
import com.hazelcast.jet.examples.tradesource.TradeSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Showcases the Rolling Aggregation operator of the Pipeline API.
 * <p>
 * The sample Jet pipeline uses a mock data source that generates random
 * trade events. It calculates for each stock the rolling sum of the amount
 * of stock that changed hands trading it (i.e., the current traded volume
 * on that stock). The sample also starts a GUI window that visualizes the
 * rising traded volume of all stocks.
 */
public class TradingVolume {

    private static final String VOLUME_MAP_NAME = "volume-by-stock";
    private static final int TRADES_PER_SEC = 3_000;
    private static final int NUMBER_OF_TICKERS = 20;
    private static final int DURATION_SECONDS = 60;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TradeSource.tradeStream(NUMBER_OF_TICKERS, TRADES_PER_SEC))
         .withoutTimestamps()
         .groupingKey(Trade::getTicker)
         .rollingAggregate(summingLong(Trade::getQuantity))
         .writeTo(Sinks.map(VOLUME_MAP_NAME));
        return p;
    }

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.bootstrappedInstance();
        new TradingVolumeGui(jet.getMap(VOLUME_MAP_NAME));
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
