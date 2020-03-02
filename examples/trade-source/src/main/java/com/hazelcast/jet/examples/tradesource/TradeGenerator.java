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

package com.hazelcast.jet.examples.tradesource;

import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class TradeGenerator {
    private static final int LOT = 100;
    private static final int PRICE_UNITS_PER_CENT = 100;
    private static final int NO_TIMEOUT = 0;

    private final List<String> tickers;
    private final long emitPeriodNanos;
    private final long startTimeMillis;
    private final long startTimeNanos;
    private final long endTimeNanos;
    private final int maxLag;
    private final Map<String, LongLongAccumulator> pricesAndTrends;

    private long scheduledTimeNanos;

    private TradeGenerator(long numTickers, int tradesPerSec, int maxLag, long timeoutSeconds) {
        this.tickers = loadTickers(numTickers);
        this.maxLag = maxLag;
        this.pricesAndTrends = tickers.stream()
                .collect(toMap(t -> t, t -> new LongLongAccumulator(500, 10)));
        this.emitPeriodNanos = SECONDS.toNanos(1) / tradesPerSec;
        this.startTimeNanos = this.scheduledTimeNanos = System.nanoTime();
        this.endTimeNanos = timeoutSeconds <= 0 ? Long.MAX_VALUE :
                startTimeNanos + SECONDS.toNanos(timeoutSeconds);
        this.startTimeMillis = System.currentTimeMillis();
    }

    public static StreamSource<Trade> tradeSource(int numTickers, int tradesPerSec, int maxLag) {
        return tradeSource(numTickers, tradesPerSec, maxLag, NO_TIMEOUT);
    }

    public static StreamSource<Trade> tradeSource(int numTickers, int tradesPerSec, int maxLag, long timeoutSeconds) {
        return SourceBuilder
                .timestampedStream("trade-source",
                        x -> new TradeGenerator(numTickers, tradesPerSec, maxLag, timeoutSeconds))
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
            LongLongAccumulator priceAndDelta = pricesAndTrends.get(ticker);
            int price = (int) (priceAndDelta.get1() / PRICE_UNITS_PER_CENT);
            priceAndDelta.add1(priceAndDelta.get2());
            priceAndDelta.add2(rnd.nextLong(101) - 50);
            long tradeTimeNanos = scheduledTimeNanos - rnd.nextLong(maxLag);
            long tradeTimeMillis = startTimeMillis + NANOSECONDS.toMillis(tradeTimeNanos - startTimeNanos);
            Trade trade = new Trade(tradeTimeMillis, ticker, rnd.nextInt(10) * LOT, price);
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
                TradeGenerator.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))) {
            return reader.lines()
                    .skip(1)
                    .limit(numTickers)
                    .map(l -> l.split("\\|")[0])
                    .collect(toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
