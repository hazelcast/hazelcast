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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.QuadFunction;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.pipeline.WindowDefinition.session;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.junit.Assert.assertEquals;

public class WindowAggregateTest extends PipelineStreamTestSupport {

    private static final long FILTERED_OUT_WINDOW_START = 0;

    @Test
    public void when_setWindowDefinition_then_windowDefinitionReturnsIt() {
        // Given
        SlidingWindowDefinition tumbling = tumbling(2);

        // When
        StageWithWindow<Integer> stage = srcStage.withoutTimestamps().window(tumbling);

        // Then
        assertEquals(tumbling, stage.windowDefinition());
    }

    @Test
    public void distinct() {
        // Given
        int winSize = itemCount / 2;
        List<Integer> timestamps = sequence(itemCount).stream()
                                                      .flatMap(i -> Stream.of(i, i))
                                                      .collect(toList());
        addToSrcMapJournal(timestamps);
        addToSrcMapJournal(closingItems);

        // For window size 2, streamInput looks like this (timestamp, item):
        // (0, 0), (0, 0), (1, 1), (1, 1), (2, 0), (2, 0), (3, 1), (3, 1), ...
        // I.e., there are duplicate items 0 and 1 in each window.
        StreamStage<Integer> streamInput = srcStage.withTimestamps(i -> i, maxLag)
                                                             .map(i -> i % winSize);

        // When
        StreamStage<TimestampedItem<Integer>> distinct = streamInput
                .window(tumbling(winSize))
                .distinct();

        // Then
        distinct.drainTo(sink);
        jet().newJob(p);
        // The expected output for window size 2 is (2, 0), (2, 1), (4, 0), (4, 1), ...
        Map<TimestampedItem<Integer>, Integer> expected = toBag(timestamps
                .stream()
                .map(i -> new TimestampedItem<>(winSize + i - i % winSize, i % winSize))
                .distinct()
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()), 10);
    }

    @Test
    public void distinct_withOutputFn() {
        // Given
        int winSize = itemCount;
        DistributedFunction<Integer, Integer> keyFn = i -> i / 2;
        DistributedBiFunction<Long, Integer, String> formatFn =
                (timestamp, item) -> String.format("(%03d, %03d)", timestamp, keyFn.apply(item));
        List<Integer> timestamps = sequence(2 * itemCount);
        addToSrcMapJournal(timestamps);
        addToSrcMapJournal(closingItems);

        // When
        StreamStage<String> distinct = srcStage
                .withTimestamps(i -> i, maxLag)
                .window(tumbling(winSize))
                .groupingKey(keyFn)
                .distinct((start, end, item) -> start == FILTERED_OUT_WINDOW_START ? null
                        : formatFn.apply(end, item));

        // Then
        distinct.drainTo(sink);
        jet().newJob(p);
        // The expected output for window size 4 (timestamp, key):  (4, 0), (4, 1), (8, 2), (8, 3), ...
        Map<String, Integer> expectedKeys = toBag(timestamps
                .stream()
                .map(i -> {
                    long end = (long) winSize + i - i % winSize;
                    return end - winSize == FILTERED_OUT_WINDOW_START ? null : formatFn.apply(end, i);
                })
                .filter(Objects::nonNull)
                .distinct()
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expectedKeys, sinkToBag()), 10);
    }

    @Test
    public void tumblingWindow() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Long, Long, String> formatFn =
                (timestamp, item) -> String.format("(%03d, %03d)", timestamp, item);

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        // When
        final int winSize = 4;
        StreamStage<String> aggregated = srcStage
                .withTimestamps(i -> i, maxLag)
                .window(tumbling(winSize))
                .aggregate(summingLong(i -> i), (start, end, sum) -> formatFn.apply(end, sum));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<String> expected = input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> formatFn.apply((long) start + winSize, expectedWindowSum.apply(start)))
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()), 10);
    }

    @Test
    public void slidingWindow() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Long, Long, String> formatFn =
                (timestamp, item) -> String.format("(%03d, %03d)", timestamp, item);

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        // When
        final int winSize = 4;
        final int slideBy = 2;
        StreamStage<String> aggregated = srcStage
                .withTimestamps(i -> i, maxLag)
                .window(sliding(winSize, slideBy))
                .aggregate(summingLong(i -> i), (start, end, sum) -> start == FILTERED_OUT_WINDOW_START ? null
                        : formatFn.apply(end, sum));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        // Window covers [start, end)
        BiFunction<Integer, Integer, Long> expectedWindowSum =
                (start, end) -> (end - start) * (start + end - 1) / 2L;
        Stream<String> headOfStream = IntStream
                .range(-winSize + slideBy, 0)
                .map(i -> i + i % slideBy) // result of % is negative because i is negative
                .distinct()
                .mapToObj(start -> formatFn.apply((long) start + winSize,
                        expectedWindowSum.apply(0, start + winSize)));
        Stream<String> restOfStream = input
                .stream()
                .map(i -> i - i % slideBy)
                .distinct()
                .map(start -> start == FILTERED_OUT_WINDOW_START ? null
                        : formatFn.apply((long) start + winSize,
                                expectedWindowSum.apply(start, min(start + winSize, itemCount))))
                .filter(Objects::nonNull);
        List<String> expected = concat(headOfStream, restOfStream).collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()), 10);
    }

    @Test
    public void sessionWindow() {
        // Given
        final int sessionLength = 4;
        final int sessionTimeout = 2;
        List<Integer> input = sequence(itemCount).stream()
                                                 .map(ts -> ts + (ts / sessionLength) * sessionTimeout)
                                                 .collect(toList());
        DistributedBiFunction<Long, Long, String> formatFn =
                (timestamp, item) -> String.format("(%03d, %03d)", timestamp, item);

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        // When
        StreamStage<String> aggregated = srcStage
                .withTimestamps(i -> i, maxLag)
                .window(session(sessionTimeout))
                .aggregate(summingLong(i -> i), (start, end, sum) -> start == FILTERED_OUT_WINDOW_START ? null
                        : formatFn.apply(start, sum));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> sessionLength * (2 * start + sessionLength - 1) / 2L;
        Map<String, Integer> expectedBag = toBag(input
                .stream()
                .map(i -> i - i % (sessionLength + sessionTimeout))
                .distinct()
                .map(start -> start == FILTERED_OUT_WINDOW_START ? null
                        : formatFn.apply((long) start, expectedWindowSum.apply(start)))
                .filter(Objects::nonNull)
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()), 10);
    }

    @Test
    public void aggregate2_withSeparateAggrOps() {
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        testAggregate2((stage0, stage1) -> stage0.aggregate2(aggrOp, stage1, aggrOp));
    }

    @Test
    public void aggregate2_withAggrOp2() {
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        testAggregate2((stage0, stage1) -> stage0.aggregate2(stage1, aggregateOperation2(aggrOp, aggrOp)));
    }

    private void testAggregate2(
            BiFunction<
                    StageWithWindow<Integer>,
                    StreamStage<Integer>,
                    StreamStage<TimestampedItem<Tuple2<Long, Long>>>>
                attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        String srcName1 = journaledMapName();
        Map<String, Integer> srcMap1 = jet().getMap(srcName1);
        addToMapJournal(srcMap1, input);
        addToMapJournal(srcMap1, closingItems);

        StreamStage<Integer> stage0 = srcStage.withTimestamps(i -> i, maxLag);
        StreamStage<Integer> stage1 = drawEventJournalValues(srcName1).withTimestamps(i -> i, maxLag);

        // When
        final int winSize = 4;
        StreamStage<TimestampedItem<Tuple2<Long, Long>>> aggregated =
                attachAggregatingStageFn.apply(stage0.window(tumbling(winSize)), stage1);

        //Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<TimestampedItem<Tuple2<Long, Long>>> expected = input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return new TimestampedItem<>((long) start + winSize, tuple2(sum, sum));
                })
                .collect(toList());
        Map<TimestampedItem<Tuple2<Long, Long>>, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()), 10);
    }

    @Test
    public void aggregate2_withSeparateAggrOps_withOutputFn() {
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        testAggregate2_withOutputFn((stage0, stage1, formatFn) ->
                stage0.aggregate2(aggrOp, stage1, aggrOp,
                        (start, end, sum0, sum1) -> start == FILTERED_OUT_WINDOW_START ? null
                                : formatFn.apply(end, tuple2(sum0, sum1))));
    }

    @Test
    public void aggregate2_withAggrOp2_withOutputFn() {
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        testAggregate2_withOutputFn((stage0, stage1, formatFn) ->
                stage0.aggregate2(stage1, aggregateOperation2(aggrOp, aggrOp),
                        (start, end, sums) -> start == FILTERED_OUT_WINDOW_START ? null
                                : formatFn.apply(end, sums)));
    }

    private void testAggregate2_withOutputFn(
            TriFunction<
                    StageWithWindow<Integer>,
                    StreamStage<Integer>,
                    DistributedBiFunction<Long, Tuple2<Long, Long>, String>,
                    StreamStage<String>
                > attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Long, Tuple2<Long, Long>, String> formatFn =
                (timestamp, sums) -> String.format("(%03d: %03d, %03d)", timestamp, sums.f0(), sums.f1());

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        String srcName1 = journaledMapName();
        Map<String, Integer> srcMap1 = jet().getMap(srcName1);
        addToMapJournal(srcMap1, input);
        addToMapJournal(srcMap1, closingItems);

        StreamStage<Integer> stage0 = srcStage.withTimestamps(i -> i, maxLag);
        StreamStage<Integer> stage1 = drawEventJournalValues(srcName1).withTimestamps(i -> i, maxLag);

        // When
        final int winSize = 4;
        StreamStage<String> aggregated =
            attachAggregatingStageFn.apply(stage0.window(tumbling(winSize)), stage1, formatFn);

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<String> expected = input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return start == FILTERED_OUT_WINDOW_START ? null
                            : formatFn.apply((long) start + winSize, tuple2(sum, sum));
                })
                .filter(Objects::nonNull)
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()), 10);
    }

    @Test
    public void aggregate3_withSeparateAggrOps() {
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        testAggregate3((stage0, stage1, stage2) -> stage0.aggregate3(aggrOp, stage1, aggrOp, stage2, aggrOp));
    }

    @Test
    public void aggregate3_withAggrOp3() {
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        testAggregate3((stage0, stage1, stage2) ->
                stage0.aggregate3(stage1, stage2, aggregateOperation3(aggrOp, aggrOp, aggrOp)));
    }

    private void testAggregate3(
            TriFunction<
                    StageWithWindow<Integer>,
                    StreamStage<Integer>,
                    StreamStage<Integer>,
                    StreamStage<TimestampedItem<Tuple3<Long, Long, Long>>>>
                attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        String srcName1 = journaledMapName();
        Map<String, Integer> srcMap1 = jet().getMap(srcName1);
        addToMapJournal(srcMap1, input);
        addToMapJournal(srcMap1, closingItems);

        String srcName2 = journaledMapName();
        Map<String, Integer> srcMap2 = jet().getMap(srcName2);
        addToMapJournal(srcMap2, input);
        addToMapJournal(srcMap2, closingItems);

        StreamStage<Integer> stage0 = srcStage.withTimestamps(i -> i, maxLag);
        StreamStage<Integer> stage1 = drawEventJournalValues(srcName1).withTimestamps(i -> i, maxLag);
        StreamStage<Integer> stage2 = drawEventJournalValues(srcName2).withTimestamps(i -> i, maxLag);

        // When
        final int winSize = 4;
        StreamStage<TimestampedItem<Tuple3<Long, Long, Long>>> aggregated =
                attachAggregatingStageFn.apply(stage0.window(tumbling(winSize)), stage1, stage2);

        //Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<TimestampedItem<Tuple3<Long, Long, Long>>> expected = input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return new TimestampedItem<>((long) start + winSize, tuple3(sum, sum, sum));
                })
                .collect(toList());
        Map<TimestampedItem<Tuple3<Long, Long, Long>>, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()), 10);
    }

    @Test
    public void aggregate3_withSeparateAggrOps_withOutputFn() {
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        testAggregate3_withOutputFn((stage0, stage1, stage2, formatFn) ->
                stage0.aggregate3(aggrOp, stage1, aggrOp, stage2, aggrOp,
                        (start, end, sum0, sum1, sum2) -> start == FILTERED_OUT_WINDOW_START ? null
                                : formatFn.apply(end, tuple3(sum0, sum1, sum2))));
    }

    @Test
    public void aggregate3_withAggrOp3_withOutputFn() {
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        testAggregate3_withOutputFn((stage0, stage1, stage2, formatFn) ->
                stage0.aggregate3(stage1, stage2, aggregateOperation3(aggrOp, aggrOp, aggrOp),
                        (start, end, sums) -> start == FILTERED_OUT_WINDOW_START ? null
                                : formatFn.apply(end, tuple3(sums.f0(), sums.f1(), sums.f2()))));
    }

    private void testAggregate3_withOutputFn(
            QuadFunction<
                    StageWithWindow<Integer>,
                    StreamStage<Integer>,
                    StreamStage<Integer>,
                    DistributedBiFunction<Long, Tuple3<Long, Long, Long>, String>,
                    StreamStage<String>
                > attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Long, Tuple3<Long, Long, Long>, String> formatFn = (timestamp, sums) ->
                String.format("(%03d: %03d, %03d, %03d)", timestamp, sums.f0(), sums.f1(), sums.f2());

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        String srcName1 = journaledMapName();
        Map<String, Integer> srcMap1 = jet().getMap(srcName1);
        addToMapJournal(srcMap1, input);
        addToMapJournal(srcMap1, closingItems);

        String srcName2 = journaledMapName();
        Map<String, Integer> srcMap2 = jet().getMap(srcName2);
        addToMapJournal(srcMap2, input);
        addToMapJournal(srcMap2, closingItems);

        StreamStage<Integer> stage0 = srcStage.withTimestamps(i -> i, maxLag);
        StreamStage<Integer> stage1 = drawEventJournalValues(srcName1).withTimestamps(i -> i, maxLag);
        StreamStage<Integer> stage2 = drawEventJournalValues(srcName2).withTimestamps(i -> i, maxLag);

        // When
        final int winSize = 4;
        StreamStage<String> aggregated =
                attachAggregatingStageFn.apply(stage0.window(tumbling(winSize)), stage1, stage2, formatFn);

        //Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<String> expected = input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return start == FILTERED_OUT_WINDOW_START ? null
                            : formatFn.apply((long) start + winSize, tuple3(sum, sum, sum));
                })
                .filter(Objects::nonNull)
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()), 10);
    }

    private class AggregateBuilderFixture {
        List<Integer> input = sequence(itemCount);

        String srcName1 = journaledMapName();
        Map<String, Integer> srcMap1 = jet().getMap(srcName1);

        StreamStage<Integer> srcStage0 = srcStage.withTimestamps(i -> i, maxLag);
        StreamStage<Integer> srcStage1 = drawEventJournalValues(srcName1).withTimestamps(i -> i, maxLag);

        AggregateBuilderFixture() {
            addToSrcMapJournal(input);
            addToSrcMapJournal(closingItems);
            addToMapJournal(srcMap1, input);
            addToMapJournal(srcMap1, closingItems);
        }
    }

    @Test
    public void aggregateBuilder_withSeparateAggrOps() {
        // Given
        AggregateBuilderFixture fx = new AggregateBuilderFixture();

        // When
        final int winSize = 4;
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        WindowAggregateBuilder<Long> b = fx.srcStage0.window(tumbling(winSize)).aggregateBuilder(aggrOp);
        Tag<Long> tag0 = b.tag0();
        Tag<Long> tag1 = b.add(fx.srcStage1, aggrOp);
        DistributedBiFunction<Long, ItemsByTag, String> formatFn =
                (timestamp, sums) -> String.format("(%03d: %03d, %03d)", timestamp, sums.get(tag0), sums.get(tag1));

        StreamStage<String> aggregated = b.build((start, end, sums) ->
                start == FILTERED_OUT_WINDOW_START ? null : formatFn.apply(end, sums));

        // Then
        validateAggrBuilder(aggregated, fx, winSize, tag0, tag1, formatFn);
    }

    @Test
    public void aggregateBuilder_withComplexAggrOp() {
        // Given
        AggregateBuilderFixture fx = new AggregateBuilderFixture();

        // When
        final int winSize = 4;
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        WindowAggregateBuilder1<Integer> b = fx.srcStage0.window(tumbling(winSize)).aggregateBuilder();
        Tag<Integer> tag0_in = b.tag0();
        Tag<Integer> tag1_in = b.add(fx.srcStage1);

        CoAggregateOperationBuilder b2 = coAggregateOperationBuilder();
        Tag<Long> tag0 = b2.add(tag0_in, aggrOp);
        Tag<Long> tag1 = b2.add(tag1_in, aggrOp);
        DistributedBiFunction<Long, ItemsByTag, String> formatFn =
                (timestamp, sums) -> String.format("(%03d: %03d, %03d)", timestamp, sums.get(tag0), sums.get(tag1));

        StreamStage<String> aggregated = b.build(
                b2.build(),
                (start, end, sums) -> start == FILTERED_OUT_WINDOW_START ? null : formatFn.apply(end, sums));

        // Then
        validateAggrBuilder(aggregated, fx, winSize, tag0, tag1, formatFn);
    }

    @SuppressWarnings("SameParameterValue")
    private void validateAggrBuilder(
            StreamStage<String> aggregated, AggregateBuilderFixture fx,
            int winSize,
            Tag<Long> tag0, Tag<Long> tag1,
            DistributedBiFunction<Long, ItemsByTag, String> formatFn
    ) {
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<String> expected = fx.input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return start == FILTERED_OUT_WINDOW_START ? null
                            : formatFn.apply((long) start + winSize, itemsByTag(tag0, sum, tag1, sum));
                })
                .filter(Objects::nonNull)
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()), 10);
    }
}
