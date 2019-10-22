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

package com.hazelcast.jet.examples.slidingwindow;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.core.EventTimePolicy.DEFAULT_IDLE_TIMEOUT;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class TradeGenerator {
    private static final int MAX_LAG = 1000;
    private static final int QUANTITY = 100;
    private static final int PRICE_UNITS_PER_CENT = 100;

    private final List<String> tickers;
    private final long emitPeriodNanos;
    private final long startTimeMillis;
    private final long startTimeNanos;

    private final Map<String, LongLongAccumulator> pricesAndTrends;
    private long scheduledTimeNanos;

    private TradeGenerator(long numTickers, int tradesPerSec) {
        this.tickers = loadTickers(numTickers);
        this.pricesAndTrends = tickers
                .stream()
                //                         (last price in 100ths of a cent, last price delta in 100ths of cent)
                .collect(toMap(t -> t, t -> new LongLongAccumulator(10_000, 10)));
        this.emitPeriodNanos = SECONDS.toNanos(1) / tradesPerSec;
        this.startTimeNanos = this.scheduledTimeNanos = System.nanoTime();
        this.startTimeMillis = System.currentTimeMillis();
    }

    public static StreamSource<Trade> tradeSource(int numTickers, int tradesPerSec) {
        return SourceBuilder
                .timestampedStream("trade-source",
                        x -> new TradeGenerator(numTickers, tradesPerSec))
                .fillBufferFn(TradeGenerator::generateTrades)
                .build();
    }

    public static ProcessorMetaSupplier tradeSourceP(
            int numTickers, int tradesPerSec, long watermarkStride
    ) {
        return SourceProcessors.convenientTimestampedSourceP(
                procCtx -> new TradeGenerator(numTickers, tradesPerSec),
                TradeGenerator::generateTrades,
                eventTimePolicy(Trade::getTime, (trade, x) -> trade, limitingLag(MAX_LAG),
                        watermarkStride, 0, DEFAULT_IDLE_TIMEOUT),
                ctx -> null,
                (ctx, states) -> { },
                ConsumerEx.noop(),
                0
        );
    }

    private void generateTrades(TimestampedSourceBuffer<Trade> buf) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long nowNanos = System.nanoTime();
        while (scheduledTimeNanos <= nowNanos) {
            String ticker = tickers.get(rnd.nextInt(tickers.size()));
            LongLongAccumulator priceAndDelta = pricesAndTrends.get(ticker);
            int price = (int) (priceAndDelta.get1() / PRICE_UNITS_PER_CENT);
            priceAndDelta.add1(priceAndDelta.get2());
            priceAndDelta.add2(rnd.nextLong(101) - 50);
            long tradeTimeNanos = scheduledTimeNanos - rnd.nextLong(MAX_LAG);
            long tradeTimeMillis = startTimeMillis + NANOSECONDS.toMillis(tradeTimeNanos - startTimeNanos);
            Trade trade = new Trade(tradeTimeMillis, ticker, QUANTITY, price);
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
