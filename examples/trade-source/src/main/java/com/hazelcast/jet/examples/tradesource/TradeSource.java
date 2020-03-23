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

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class TradeSource {

    /**
     * Returns a source of an unbounded stream of trade events. The
     * quantity of the returned trades will be a random multiple of an
     * internal lot size ({@value TradeGenerator#LOT}). The price will also be
     * randomized, but not as an absolute value, but as a value with
     * randomly varying change trends up or down. Both the price and the
     * quantity are always positive.
     * <p>
     * The trades will be generated at the specified rate per second and
     * their tickers will be from a limited set of names.
     * <p>
     * The event time of the generated trades will be evenly spaced
     * across physical time, as dictated by the trades-per-second
     * generation rate (ie. no disorder will be introduced by the source).
     *
     * @param tradesPerSec number of trades generated per second
     *                     (globally, for all tickers)
     * @return streaming source of trades
     * @throws IllegalArgumentException if tradePerSec is smaller than 1
     */
    @Nonnull
    public static StreamSource<Trade> tradeStream(int tradesPerSec) {
        return tradeStream(Integer.MAX_VALUE, tradesPerSec, 0);
    }

    /**
     * Returns a source of an unbounded stream of trade events. The
     * quantity of the returned trades will be a random multiple of an
     * internal lot size ({@value TradeGenerator#LOT}). The price will also be
     * randomized, but not as an absolute value, but as a value with
     * randomly varying change trends up or down. Both the price and the
     * quantity are always positive.
     * <p>
     * The trades will be generated at the specified rate per second and
     * their tickers will be from a limited set of names, no more than
     * the specified limit.
     * <p>
     * The event time of the generated trades will be evenly spaced
     * across physical time, as dictated by the trades-per-second
     * generation rate (ie. no disorder will be introduced by the source).
     *
     * @param numTickers   maximum number of ticker names for which
     *                     trades will be generated
     * @param tradesPerSec number of trades generated per second
     *                     (globally, for all tickers)
     * @return streaming source of trades
     * @throws IllegalArgumentException if numTickers or tradePerSec is
     *                                  smaller than 1
     */
    @Nonnull
    public static StreamSource<Trade> tradeStream(int numTickers, int tradesPerSec) {
        return tradeStream(numTickers, tradesPerSec, 0);
    }

    /**
     * Returns a source of an unbounded stream of trade events. The
     * quantity of the returned trades will be a random multiple of an
     * internal lot size ({@value TradeGenerator#LOT}). The price will also be
     * randomized, but not as an absolute value, but as a value with
     * randomly varying change trends up or down. Both the price and the
     * quantity are always positive.
     * <p>
     * The trades will be generated at the specified rate per second and
     * their tickers will be from a limited set of names, no more than
     * the specified limit.
     * <p>
     * The event time of the generated trades would normally be evenly
     * spaced across physical time, as dictated by the trades-per-second
     * generation rate. However the trade source will introduce
     * randomness in these timestamps, but no more than the specified
     * upper limit.
     *
     * @param numTickers   maximum number of ticker names for which
     *                     trades will be generated
     * @param tradesPerSec number of trades generated per second
     *                     (globally, for all tickers)
     * @param maxLag       maximum random variance in the event time of
     *                     the trades; if negative or zero event time
     *                     disorder will be disabled
     * @return streaming source of trades
     * @throws IllegalArgumentException if numTickers or tradePerSec is
     *                                  smaller than 1
     */
    @Nonnull
    public static StreamSource<Trade> tradeStream(int numTickers, int tradesPerSec, int maxLag) {
        if (numTickers <= 0) {
            throw new IllegalArgumentException("Number of tickers has to be at least 1");
        }
        if (tradesPerSec <= 0) {
            throw new IllegalArgumentException("Number of trades per second has to be at least one");
        }
        return SourceBuilder
                .timestampedStream("trade-source",
                        x -> new TradeGenerator(numTickers, tradesPerSec, maxLag))
                .fillBufferFn(TradeGenerator::generateTrades)
                .build();
    }

    private static final class TradeGenerator {

        private static final int LOT = 100;
        private static final long MONEY_SCALE_FACTOR = 1000L;

        private final List<String> tickers;
        private final long emitPeriodNanos;
        private final long startTimeMillis;
        private final long startTimeNanos;
        private final long maxLagNanos;
        private final Map<String, LongLongAccumulator> pricesAndTrends;

        private long scheduledTimeNanos;

        private TradeGenerator(long numTickers, int tradesPerSec, int maxLagMillis) {
            this.tickers = loadTickers(numTickers);
            this.maxLagNanos = MILLISECONDS.toNanos(maxLagMillis);
            this.pricesAndTrends = tickers.stream()
                    .collect(toMap(t -> t, t -> new LongLongAccumulator(50 * MONEY_SCALE_FACTOR,
                            MONEY_SCALE_FACTOR / 10)));
            this.emitPeriodNanos = SECONDS.toNanos(1) / tradesPerSec;
            this.startTimeNanos = this.scheduledTimeNanos = System.nanoTime();
            this.startTimeMillis = System.currentTimeMillis();
        }

        private void generateTrades(TimestampedSourceBuffer<Trade> buf) {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            long nowNanos = System.nanoTime();
            while (scheduledTimeNanos <= nowNanos) {
                String ticker = tickers.get(rnd.nextInt(tickers.size()));
                LongLongAccumulator priceAndDelta = pricesAndTrends.get(ticker);
                long price = getNextPrice(priceAndDelta, rnd) / MONEY_SCALE_FACTOR;
                long tradeTimeNanos = scheduledTimeNanos - (maxLagNanos > 0 ? rnd.nextLong(maxLagNanos) : 0L);
                long tradeTimeMillis = startTimeMillis + NANOSECONDS.toMillis(tradeTimeNanos - startTimeNanos);
                Trade trade = new Trade(tradeTimeMillis, ticker, rnd.nextInt(1, 10) * LOT, price);
                buf.add(trade, tradeTimeMillis);
                scheduledTimeNanos += emitPeriodNanos;
                if (scheduledTimeNanos > nowNanos) {
                    // Refresh current time before checking against scheduled time
                    nowNanos = System.nanoTime();
                }
            }
        }

        private static long getNextPrice(LongLongAccumulator priceAndDelta, ThreadLocalRandom rnd) {
            long price = priceAndDelta.get1();
            long delta = priceAndDelta.get2();
            if (price + delta <= 0) {
                //having a negative price doesn't make sense for most financial instruments
                delta = -delta;
            }
            price = price + delta;
            delta = delta + rnd.nextLong(MONEY_SCALE_FACTOR + 1) - MONEY_SCALE_FACTOR / 2;

            priceAndDelta.set1(price);
            priceAndDelta.set2(delta);

            return price;
        }

        private static List<String> loadTickers(long numTickers) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    TradeSource.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))) {
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
}
