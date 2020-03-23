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

package com.hazelcast.jet.examples.slidingwindow;

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.examples.tradesource.Trade;
import com.hazelcast.jet.examples.tradesource.TradeSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;

import java.util.List;
import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.aggregate.AggregateOperations.topN;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

/**
 * Shows how perform nested aggregation. The sample allows you to monitor
 * the current best and worst performers of a stock market by tracking the
 * movement of all the stock prices. It takes two levels of aggregation to
 * achieve this:
 * <ol><li>
 *     calculate the linear trend of stock price over a sliding window
 * </li><li>
 *     take each sliding window result (one item per traded stock) and
 *     find the items with the top/bottom values of the linear trend
 * </li></ol>
 * All the items belonging to a given position of the sliding window have
 * the same timestamp (time when the window ends). The second-level
 * aggregation must set up its window so that a single window position
 * captures all the results of the first level with the same timestamp. The
 * time difference between the consecutive results is equal to the sliding
 * step we configured. This is why the second level uses a tumbling window
 * with size equal to the first level's sliding step.
 */
public class TopNStocks {

    private static final int JOB_DURATION = 15;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        ComparatorEx<KeyedWindowResult<String, Double>> comparingValue =
                ComparatorEx.comparing(KeyedWindowResult<String, Double>::result);
        // Apply two functions in a single step: top-n largest and top-n smallest values
        AggregateOperation1<KeyedWindowResult<String, Double>, ?, TopNResult> aggrOpTopN = allOf(
                topN(5, comparingValue),
                topN(5, comparingValue.reversed()),
                TopNResult::new);

        p.readFrom(TradeSource.tradeStream(500, 6_000))
         .withNativeTimestamps(1_000)
         .groupingKey(Trade::getTicker)
         .window(sliding(10_000, 1_000))
         // aggregate to create trend for each ticker
         .aggregate(linearTrend(Trade::getTime, Trade::getPrice))
         .window(tumbling(1_000))
         // 2nd aggregation: choose top-N trends from previous aggregation
         .aggregate(aggrOpTopN)
         .writeTo(Sinks.logger(wr -> String.format("%nAt %s...%n%s", toLocalTime(wr.end()), wr.result())));

        return p;
    }

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.bootstrappedInstance();
        try {
            Job job = jet.newJob(buildPipeline());
            SECONDS.sleep(JOB_DURATION);
            job.cancel();
            job.join();
        } catch (CancellationException ignored) {
        } finally {
            Jet.shutdownAll();
        }
    }

    static final class TopNResult {
        final List<KeyedWindowResult<String, Double>> topIncrease;
        final List<KeyedWindowResult<String, Double>> topDecrease;

        TopNResult(
                List<KeyedWindowResult<String, Double>> topIncrease,
                List<KeyedWindowResult<String, Double>> topDecrease
        ) {
            this.topIncrease = topIncrease;
            this.topDecrease = topDecrease;
        }

        @Override
        public String toString() {
            return String.format(
                    "Top rising stocks:%n%s\nTop falling stocks:%n%s",
                    topIncrease.stream().map(kwr -> String.format("   %s by %.2f%%", kwr.key(), 100d * kwr.result()))
                               .collect(joining("\n")),
                    topDecrease.stream().map(kwr -> String.format("   %s by %.2f%%", kwr.key(), 100d * kwr.result()))
                               .collect(joining("\n"))
            );
        }
    }
}
