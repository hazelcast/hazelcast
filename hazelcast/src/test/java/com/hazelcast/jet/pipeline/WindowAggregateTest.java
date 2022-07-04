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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.datamodel.WindowResult;
import org.junit.Test;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.pipeline.WindowDefinition.session;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class WindowAggregateTest extends PipelineStreamTestSupport {

    private static final BiFunctionEx<Long, Tuple2<Long, Long>, String> FORMAT_FN_2 =
            (timestamp, sums) -> String.format("(%04d: %04d, %04d)", timestamp, sums.f0(), sums.f1());

    private static final BiFunctionEx<Long, Tuple3<Long, Long, Long>, String> FORMAT_FN_3 =
            (timestamp, sums) -> String.format("(%04d: %04d, %04d, %04d)",
            timestamp, sums.f0(), sums.f1(), sums.f2());

    private static final AggregateOperation1<Integer, LongAccumulator, Long> SUMMING = summingLong(i -> i);

    @Test
    public void when_setWindowDefinition_then_windowDefinitionReturnsIt() {
        // Given
        SlidingWindowDefinition tumbling = tumbling(2);

        // When
        StageWithWindow<Integer> stage = streamStageFromList(emptyList()).window(tumbling);

        // Then
        assertEquals(tumbling, stage.windowDefinition());
    }

    @Test
    public void distinct() {
        // Given
        int winSize = itemCount / 2;
        // timestamps: [0, 0, 1, 1, 2, 2, ...]
        List<Integer> timestamps = IntStream.range(0, itemCount)
                                            .flatMap(i -> IntStream.of(i, i))
                                            .boxed()
                                            .collect(toList());
        StageWithWindow<Integer> windowed = streamStageFromList(timestamps)
                .window(tumbling(winSize));

        // When
        StreamStage<WindowResult<Integer>> distinct = windowed.distinct();

        // Then
        distinct.writeTo(sink);
        execute();
        assertEquals(
                streamToString(
                        IntStream.range(0, itemCount)
                                 .mapToObj(i -> String.format("(%04d, %04d)", roundUp(i + 1, winSize), i))
                                 .distinct(),
                        identity()),
                streamToString(
                        this.<Integer>sinkStreamOfWinResult(),
                        wr -> String.format("(%04d, %04d)", wr.end(), wr.result()))
        );
    }

    @Test
    public void tumblingWindow() {
        // Given
        int winSize = 4;
        BiFunction<Long, Long, String> formatFn =
                (timestamp, item) -> String.format("(%04d, %04d)", timestamp, item);

        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> stage = streamStageFromList(input);

        // When
        SlidingWindowDefinition wDef = tumbling(winSize);
        StageWithWindow<Integer> windowed = stage.window(wDef);

        // Then
        windowed.aggregate(summingLong(i -> i))
                .writeTo(sink);
        execute();
        assertEquals(
                new SlidingWindowSimulator(wDef)
                        .acceptStream(input.stream())
                        .stringResults(e -> formatFn.apply(e.getKey(), e.getValue())),
                streamToString(this.<Long>sinkStreamOfWinResult(),
                        wr -> formatFn.apply(wr.end(), wr.result()))
        );
    }

    @Test
    public void tumblingWindow_withEarlyResults() {
        // Given
        int winSize = 4;
        BiFunction<Long, Long, String> formatFn =
                (timestamp, item) -> String.format("(%04d, %04d)", timestamp, item);

        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> stage = streamStageFromList(input, EARLY_RESULTS_PERIOD);

        // When
        SlidingWindowDefinition wDef = tumbling(winSize).setEarlyResultsPeriod(EARLY_RESULTS_PERIOD);
        StageWithWindow<Integer> windowed = stage.window(wDef);

        // Then
        windowed.aggregate(summingLong(i -> i))
                .writeTo(sink);
        hz().getJet().newJob(p);
        String expectedString = new SlidingWindowSimulator(wDef)
                .acceptStream(input.stream())
                .stringResults(e -> formatFn.apply(e.getKey(), e.getValue()));
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(this.<Long>sinkStreamOfWinResult(),
                        wr -> formatFn.apply(wr.end(), wr.result()),
                        WindowResult::end
                )),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void slidingWindow() {
        // Given
        int winSize = 4;
        int slideBy = 2;
        List<Integer> input = sequence(itemCount);
        BiFunction<Long, Long, String> formatFn =
                (timestamp, item) -> String.format("(%04d, %04d)", timestamp, item);
        // If emitting early results, keep the watermark behind all input
        StreamStage<Integer> stage = streamStageFromList(input);

        // When
        SlidingWindowDefinition wDef = sliding(winSize, slideBy);
        StreamStage<WindowResult<Long>> aggregated = stage.window(wDef)
                                                             .aggregate(summingLong(i -> i));

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(
                new SlidingWindowSimulator(wDef)
                        .acceptStream(input.stream())
                        .stringResults(e -> formatFn.apply(e.getKey(), e.getValue())),
                streamToString(this.<Long>sinkStreamOfWinResult(),
                        wr -> formatFn.apply(wr.end(), wr.result()))
        );
    }

    @Test
    public void slidingWindow_withEarlyResults() {
        // Given
        int winSize = 4;
        int slideBy = 2;
        List<Integer> input = sequence(itemCount);
        BiFunction<Long, Long, String> formatFn =
                (timestamp, item) -> String.format("(%04d, %04d)", timestamp, item);
        // If emitting early results, keep the watermark behind all input
        StreamStage<Integer> stage = streamStageFromList(input, EARLY_RESULTS_PERIOD);

        // When
        SlidingWindowDefinition wDef = sliding(winSize, slideBy).setEarlyResultsPeriod(EARLY_RESULTS_PERIOD);
        StreamStage<WindowResult<Long>> aggregated = stage.window(wDef)
                                                             .aggregate(summingLong(i -> i));

        // Then
        aggregated.writeTo(sink);
        hz().getJet().newJob(p);
        String expectedString = new SlidingWindowSimulator(wDef)
                .acceptStream(input.stream())
                .stringResults(e -> formatFn.apply(e.getKey(), e.getValue()));
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(this.<Long>sinkStreamOfWinResult(),
                        wr -> formatFn.apply(wr.end(), wr.result()),
                        WindowResult::end
                )),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void sessionWindow() {
        // Given
        int sessionLength = 4;
        int sessionTimeout = 2;
        // Sample input: [0, 1, 2, 3,   6, 7, 8, 9,   12, 13, 14, 15,  ...]
        List<Integer> input = sequence(itemCount / sessionLength * sessionLength)
                .stream()
                .map(ts -> ts + (ts / sessionLength) * sessionTimeout)
                .collect(toList());
        BiFunction<Long, Long, String> formatFn = (timestamp, sum) -> String.format("(%04d, %04d)", timestamp, sum);

        // When
        SessionWindowDefinition wDef = session(sessionTimeout);
        StageWithWindow<Integer> windowed = streamStageFromList(input).window(wDef);

        // Then
        windowed.aggregate(summingLong(i -> i))
                .writeTo(sink);
        execute();

        assertEquals(
                new SessionWindowSimulator(wDef, sessionLength + sessionTimeout)
                        .acceptStream(input.stream())
                        .stringResults(e -> formatFn.apply(e.getKey(), e.getValue())),
                streamToString(
                        this.<Long>sinkStreamOfWinResult(),
                        wr -> formatFn.apply(wr.start(), wr.result()))
        );
    }

    @Test
    public void sessionWindow_withEarlyResults() {
        // Given
        int sessionLength = 4;
        int sessionTimeout = 2;
        // Sample input: [0, 1, 2, 3,   6, 7, 8, 9,   12, 13, 14, 15,  ...]
        List<Integer> input = sequence(itemCount).stream()
                                                 .map(ts -> ts + (ts / sessionLength) * sessionTimeout)
                                                 .collect(toList());
        BiFunction<Long, Long, String> formatFn = (timestamp, sum) -> String.format("(%04d, %04d)", timestamp, sum);
        // Keep the watermark behind all input
        StreamStage<Integer> stage = streamStageFromList(input, EARLY_RESULTS_PERIOD);

        // When
        SessionWindowDefinition wDef = session(sessionTimeout).setEarlyResultsPeriod(EARLY_RESULTS_PERIOD);
        StageWithWindow<Integer> windowed = stage.window(wDef);

        // Then
        windowed.aggregate(summingLong(i -> i))
                // suppress incomplete windows to get predictable results
                .filter(wr -> wr.end() - wr.start() == sessionLength + sessionTimeout - 1)
                .writeTo(sink);
        hz().getJet().newJob(p);

        String expectedString = new SessionWindowSimulator(wDef, sessionLength + sessionTimeout)
                .acceptStream(input.stream())
                .stringResults(e -> formatFn.apply(e.getKey(), e.getValue()));
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(
                        this.<Long>sinkStreamOfWinResult(),
                        wr -> formatFn.apply(wr.start(), wr.result()),
                        WindowResult::end
                )),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void when_tumblingWinWithEarlyResults_then_emitRepeatedly() {
        assertEarlyResultsEmittedRepeatedly(tumbling(10));
    }

    @Test
    public void when_sessionWinWithEarlyResults_then_emitRepeatedly() {
        assertEarlyResultsEmittedRepeatedly(session(10));
    }

    private void assertEarlyResultsEmittedRepeatedly(WindowDefinition wDef) {
        // Given
        long earlyResultPeriod = 50;
        StreamStage<Integer> srcStage = streamStageFromList(singletonList(0), earlyResultPeriod);

        // When
        StageWithWindow<Integer> stage = srcStage.window(wDef.setEarlyResultsPeriod(earlyResultPeriod));

        // Then
        stage.aggregate(counting()).writeTo(Sinks.list(sinkList));
        hz().getJet().newJob(p);
        assertTrueEventually(() -> assertGreaterOrEquals("sinkList.size()", sinkList.size(), 10));
        WindowResult<Long> expected = new WindowResult<>(0L, 10L, 1L, true);
        sinkList.forEach(it -> assertEquals(expected, it));
    }

    private class CoAggregateFixture {
        final SlidingWindowDefinition wDef = tumbling(4);

        final List<Integer> input = sequence(itemCount);

        final StageWithWindow<Integer> stage0 = newStage().window(wDef);

        final String expectedString2 = new SlidingWindowSimulator(wDef)
                .acceptStream(input.stream())
                .stringResults(e -> FORMAT_FN_2.apply(e.getKey(), tuple2(e.getValue(), e.getValue())));

        final String expectedString3 = new SlidingWindowSimulator(wDef)
                .acceptStream(input.stream())
                .stringResults(e -> FORMAT_FN_3.apply(e.getKey(), tuple3(e.getValue(), e.getValue(), e.getValue())));

        StreamStage<Integer> newStage() {
            return streamStageFromList(input);
        }
    }

    @Test
    public void aggregate2_withSeparateAggrOps() {
        // Given
        CoAggregateFixture fx = new CoAggregateFixture();

        // When
        StreamStage<WindowResult<Tuple2<Long, Long>>> aggregated =
                fx.stage0.aggregate2(SUMMING, fx.newStage(), SUMMING);

        //Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString2,
                streamToString(
                        this.<Tuple2<Long, Long>>sinkStreamOfWinResult(),
                        wr -> FORMAT_FN_2.apply(wr.end(), wr.result())
                ));
    }

    @Test
    public void aggregate2_withAggrOp2() {
        // Given
        CoAggregateFixture fx = new CoAggregateFixture();

        // When
        StreamStage<WindowResult<Tuple2<Long, Long>>> aggregated =
                fx.stage0.aggregate2(fx.newStage(), aggregateOperation2(SUMMING, SUMMING));

        //Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString2,
                streamToString(
                        this.<Tuple2<Long, Long>>sinkStreamOfWinResult(),
                        wr -> FORMAT_FN_2.apply(wr.end(), wr.result())
                ));
    }

    @Test
    public void aggregate3_withSeparateAggrOps() {
        // Given
        CoAggregateFixture fx = new CoAggregateFixture();

        // When
        StreamStage<WindowResult<Tuple3<Long, Long, Long>>> aggregated =
                fx.stage0.aggregate3(SUMMING, fx.newStage(), SUMMING, fx.newStage(), SUMMING);

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString3,
                streamToString(this.<Tuple3<Long, Long, Long>>sinkStreamOfWinResult(),
                        wr -> FORMAT_FN_3.apply(wr.end(), wr.result())
                ));
    }

    @Test
    public void aggregate3_withAggrOp3() {
        // Given
        CoAggregateFixture fx = new CoAggregateFixture();

        // When
        StreamStage<WindowResult<Tuple3<Long, Long, Long>>> aggregated =
                fx.stage0.aggregate3(fx.newStage(), fx.newStage(),
                        aggregateOperation3(SUMMING, SUMMING, SUMMING));

        //Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString3,
                streamToString(this.<Tuple3<Long, Long, Long>>sinkStreamOfWinResult(),
                        wr -> FORMAT_FN_3.apply(wr.end(), wr.result())
                ));
    }

    @Test
    public void aggregateBuilder_withSeparateAggrOps() {
        // Given
        CoAggregateFixture fx = new CoAggregateFixture();

        // When
        WindowAggregateBuilder<Long> b = fx.stage0.aggregateBuilder(SUMMING);
        Tag<Long> tag0 = b.tag0();
        Tag<Long> tag1 = b.add(fx.newStage(), SUMMING);
        StreamStage<WindowResult<ItemsByTag>> aggregated = b.build();

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString2,
                streamToString(this.<ItemsByTag>sinkStreamOfWinResult(),
                        wr -> FORMAT_FN_2.apply(wr.end(), tuple2(wr.result().get(tag0), wr.result().get(tag1)))));
    }

    @Test
    public void aggregateBuilder_withComplexAggrOp() {
        // Given
        CoAggregateFixture fx = new CoAggregateFixture();

        // When
        WindowAggregateBuilder1<Integer> b = fx.stage0.aggregateBuilder();
        Tag<Integer> tag0_in = b.tag0();
        Tag<Integer> tag1_in = b.add(fx.newStage());

        CoAggregateOperationBuilder b2 = coAggregateOperationBuilder();
        Tag<Long> tag0 = b2.add(tag0_in, SUMMING);
        Tag<Long> tag1 = b2.add(tag1_in, SUMMING);

        StreamStage<WindowResult<ItemsByTag>> aggregated = b.build(b2.build());

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString2,
                streamToString(this.<ItemsByTag>sinkStreamOfWinResult(),
                        wr -> FORMAT_FN_2.apply(wr.end(), tuple2(wr.result().get(tag0), wr.result().get(tag1))))
        );
    }
}
