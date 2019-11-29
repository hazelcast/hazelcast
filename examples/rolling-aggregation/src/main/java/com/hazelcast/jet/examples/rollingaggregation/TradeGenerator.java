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

package com.hazelcast.jet.examples.rollingaggregation;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class TradeGenerator {

    private static final long NASDAQLISTED_ROWCOUNT = 3170;

    private final List<String> tickers;
    private final long emitPeriodNanos;
    private final long startTimeMillis;
    private final long startTimeNanos;
    private final long endTimeNanos;
    private long scheduledTimeNanos;

    private TradeGenerator(long numTickers, int tradesPerSec, long timeoutSeconds) {
        this.tickers = loadTickers(numTickers);
        this.emitPeriodNanos = SECONDS.toNanos(1) / tradesPerSec;
        this.startTimeNanos = this.scheduledTimeNanos = System.nanoTime();
        this.endTimeNanos = startTimeNanos + SECONDS.toNanos(timeoutSeconds);
        this.startTimeMillis = System.currentTimeMillis();
    }

    public static StreamSource<Trade> tradeSource(int numTickers, int tradesPerSec, long timeoutSeconds) {
        return SourceBuilder
                .timestampedStream("trade-source",
                        x -> new TradeGenerator(numTickers, tradesPerSec, timeoutSeconds))
                .fillBufferFn(TradeGenerator::generateTrades)
                .build();
    }

    private void generateTrades(TimestampedSourceBuffer<Trade> buf) {
        if (scheduledTimeNanos >= endTimeNanos) {
            return;
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long nowNanos = System.nanoTime();
        while (scheduledTimeNanos <= nowNanos) {
            String ticker = tickers.get(rnd.nextInt(tickers.size()));
            long tradeTimeMillis = startTimeMillis + NANOSECONDS.toMillis(scheduledTimeNanos - startTimeNanos);
            Trade trade = new Trade(tradeTimeMillis, ticker, 1, rnd.nextInt(5000));
            buf.add(trade, tradeTimeMillis);
            scheduledTimeNanos += emitPeriodNanos;
            if (scheduledTimeNanos > nowNanos) {
                // Refresh current time before checking against scheduled time
                nowNanos = System.nanoTime();
            }
        }
    }

    private static List<String> loadTickers(long numTickers) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TradeGenerator.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))
        ) {
            final List<String> result = new ArrayList<>();
            final long strideLength = NASDAQLISTED_ROWCOUNT / numTickers;
            int rowCount = 0;
            for (String line; (line = reader.readLine()) != null; ) {
                if (++rowCount % strideLength != 0) {
                    continue;
                }
                result.add(line.substring(0, line.indexOf('|')));
                if (result.size() == numTickers) {
                    break;
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
