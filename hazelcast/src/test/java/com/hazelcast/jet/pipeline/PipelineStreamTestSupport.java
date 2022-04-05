/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.Long.max;

@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class PipelineStreamTestSupport extends PipelineTestSupport {
    static final int ASSERT_TIMEOUT_SECONDS = 30;
    static final long EARLY_RESULTS_PERIOD = 200L;
    private static final int SOURCE_EVENT_PERIOD_NANOS = 100_000;

    /**
     * The returned stream stage emits all the items from the supplied list
     * of integers. It uses the integer as both timestamp and data.
     * <p>
     * When it exhausts the input, the source completes. This allows the
     * entire Jet job to complete.
     *
     */
    StreamStage<Integer> streamStageFromList(List<Integer> input) {
        return p.readFrom(TestSources.items(input)).addTimestamps(ts -> ts, 0);
    }

    /**
     * The returned stream stage emits the items from the supplied list of
     * integers. It uses the integer as both timestamp and data.
     * <p>
     * The stage emits (1e9 / {@value SOURCE_EVENT_PERIOD_NANOS}) items per
     * second. After it exhausts the input, it remains idle forever.
     */
    StreamStage<Integer> streamStageFromList(List<Integer> input, long earlyResultsPeriod) {
        Preconditions.checkPositive(earlyResultsPeriod, "earlyResultsPeriod must greater than zero");
        StreamSource<Integer> source = SourceBuilder
                .timestampedStream("sequence", x -> new LongLongAccumulator(System.nanoTime(), 0))
                .<Integer>fillBufferFn((deadline_emittedCount, buf) -> {
                    int emittedCount = (int) deadline_emittedCount.get2();
                    if (emittedCount == input.size() || System.nanoTime() < deadline_emittedCount.get1()) {
                        return;
                    }
                    int item = input.get(emittedCount);
                    buf.add(item, (long) item);
                    deadline_emittedCount.add1(SOURCE_EVENT_PERIOD_NANOS);
                    deadline_emittedCount.add2(1);
                })
                .build();
        return p.readFrom(source).withNativeTimestamps(2 * itemCount);
    }

    @SuppressWarnings("unchecked")
    <T> Stream<KeyedWindowResult<String, T>> sinkStreamOfKeyedWinResult() {
        return sinkList.stream().map(KeyedWindowResult.class::cast);
    }

    @SuppressWarnings("unchecked")
    <T> Stream<WindowResult<T>> sinkStreamOfWinResult() {
        return sinkList.stream().map(WindowResult.class::cast);
    }

    /**
     * Generates the expected results of sliding window aggregation. The input
     * must be timestamped {@code int}s and the aggregate function is summing.
     */
    static class SlidingWindowSimulator {
        // window end -> sum of integer items
        final NavigableMap<Long, Long> windowSums = new TreeMap<>();
        private final long winSize;
        private final long frameSize;
        private long topTimestamp;

        SlidingWindowSimulator(SlidingWindowDefinition winDef) {
            this.winSize = winDef.windowSize();
            this.frameSize = winDef.slideBy();
        }

        void accept(long timestamp, int item) {
            long frameStart = roundDown(timestamp, frameSize);
            long earliestWindowEnd = frameStart + frameSize;
            long latestWindowEnd = frameStart + winSize;
            for (long winEnd = earliestWindowEnd; winEnd <= latestWindowEnd; winEnd += frameSize) {
                windowSums.merge(winEnd, (long) item, Long::sum);
            }
            topTimestamp = max(topTimestamp, timestamp);
        }

        /**
         * Uses the integers in the stream as both timestamps and data.
         */
        SlidingWindowSimulator acceptStream(Stream<Integer> input) {
            input.forEach(i -> accept(i, i));
            return this;
        }

        /**
         * Returns a string representation of the aggregation output, one result
         * per line.
         */
        String stringResults(Function<? super Entry<Long, Long>, ? extends String> formatFn) {
            Stream<Entry<Long, Long>> winStream = windowSums.entrySet().stream();
            return streamToString(winStream, formatFn, null);
        }
    }

    /**
     * Generates the expected results of session window aggregation. The input
     * must be timestamped {@code int}s and the aggregate function is summing.
     * The input must come in sorted by timestamp. The logic to detect session
     * timeout assumes this.
     */
    static class SessionWindowSimulator {
        final NavigableMap<Long, Long> windowSums = new TreeMap<>();
        private final long sessionTimeout;
        private final long expectedWindowSize;
        private long prevTimestamp = Long.MIN_VALUE;

        SessionWindowSimulator(SessionWindowDefinition winDef, long expectedWindowSize) {
            this.sessionTimeout = winDef.sessionTimeout();
            this.expectedWindowSize = expectedWindowSize;
        }

        void accept(long timestamp, int item) {
            long winStart = (timestamp <= prevTimestamp + sessionTimeout)
                    ? windowSums.lastKey()
                    : timestamp;
            windowSums.merge(winStart, (long) item, Long::sum);
            prevTimestamp = timestamp;
        }

        /**
         * Uses the integers in the stream as both timestamps and data.
         */
        SessionWindowSimulator acceptStream(Stream<Integer> input) {
            input.forEach(i -> accept(i, i));
            return this;
        }

        /**
         * Returns a string representation of the aggregation output, one result
         * per line.
         */
        String stringResults(Function<? super Entry<Long, Long>, ? extends String> formatFn) {
            SortedMap<Long, Long> results = windowSums;
            if (prevTimestamp % expectedWindowSize != expectedWindowSize - sessionTimeout - 1) {
                results = results.headMap(windowSums.lastKey());
            }
            return streamToString(results.entrySet().stream(), formatFn, null);
        }
    }
}
