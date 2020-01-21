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

package com.hazelcast.jet.examples.patternmatching.support;

import com.hazelcast.jet.examples.patternmatching.support.TransactionEvent.Type;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class TransactionGenerator {

    private final long emitPeriodNanos;
    private final long startTimeNanos;
    private long scheduledTimeNanos;
    private final Set<Long> transactionsInProgress = new HashSet<>();
    private long nextTransactionId;

    private TransactionGenerator(int tradesPerSec) {
        this.emitPeriodNanos = SECONDS.toNanos(1) / tradesPerSec;
        this.startTimeNanos = this.scheduledTimeNanos = System.nanoTime();
    }

    public static StreamSource<TransactionEvent> transactionEventSource(int txPerSec) {
        return SourceBuilder
                .stream("trade-source", x -> new TransactionGenerator(txPerSec))
                .fillBufferFn(TransactionGenerator::generateTrades)
                .build();
    }

    @SuppressWarnings("checkstyle:avoidnestedblocks")
    private void generateTrades(SourceBuffer<TransactionEvent> buf) {
        TransactionEvent.Type[] eventTypes = Type.values();
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long nowNanos = System.nanoTime();
        while (scheduledTimeNanos <= nowNanos) {
            long timeMillis = NANOSECONDS.toMillis(scheduledTimeNanos - startTimeNanos);
            Type eventType = eventTypes[rnd.nextInt(eventTypes.length)];
            switch (eventType) {
                case START: {
                    long transactionId = nextTransactionId++;
                    transactionsInProgress.add(transactionId);
                    buf.add(new TransactionEvent(timeMillis, transactionId, eventType));
                    break;
                }
                case END: {
                    Iterator<Long> it = transactionsInProgress.iterator();
                    if (!it.hasNext()) {
                        break;
                    }
                    long transactionId = it.next();
                    it.remove();
                    buf.add(new TransactionEvent(timeMillis, transactionId, eventType));
                    break;
                }
                default:
            }
            scheduledTimeNanos += emitPeriodNanos;
            if (scheduledTimeNanos > nowNanos) {
                // Refresh current time before checking against scheduled time
                nowNanos = System.nanoTime();
            }
        }
    }
}
