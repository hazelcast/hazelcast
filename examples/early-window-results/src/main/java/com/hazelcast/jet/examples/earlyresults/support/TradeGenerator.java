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

package com.hazelcast.jet.examples.earlyresults.support;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;

import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class TradeGenerator {

    public static final long MAX_LAG = 5000;
    private static final String TICKER = "TSLA";

    private final long emitPeriodNanos;
    private final long startTimeNanos;
    private final long endTimeNanos;
    private long scheduledTimeNanos;

    private TradeGenerator(int tradesPerSec, long timeoutSeconds) {
        this.emitPeriodNanos = SECONDS.toNanos(1) / tradesPerSec;
        this.startTimeNanos = this.scheduledTimeNanos = System.nanoTime();
        this.endTimeNanos = startTimeNanos + SECONDS.toNanos(timeoutSeconds);
    }

    public static StreamSource<Trade> tradeSource(int tradesPerSec, long timeoutSeconds) {
        return SourceBuilder
                .timestampedStream("trade-source", x -> new TradeGenerator(tradesPerSec, timeoutSeconds))
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
            long tradeTimeMillis = NANOSECONDS.toMillis(scheduledTimeNanos - startTimeNanos) - rnd.nextLong(MAX_LAG);
            if (tradeTimeMillis >= 0) {
                //                                               Stock quantity   Stock price in cents
                Trade trade = new Trade(tradeTimeMillis, TICKER, rnd.nextInt(30), 280_00 + rnd.nextInt(50_00));
                buf.add(trade, tradeTimeMillis);
            }
            scheduledTimeNanos += emitPeriodNanos;
            if (scheduledTimeNanos > nowNanos) {
                // Refresh current time before checking against scheduled time
                nowNanos = System.nanoTime();
            }
        }
    }
}
