/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.BatchAggregateTest.QuadFunction;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.pipeline.WindowDefinition.session;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.junit.Assert.assertEquals;

public class WindowGroupAggregateTest extends PipelineStreamTestSupport {

    @Test
    public void windowDefinition() {
        SlidingWindowDef tumbling = tumbling(2);
        StageWithKeyAndWindow<Integer, Integer> stage =
                srcStage.addKey(wholeItem()).window(tumbling);
        assertEquals(tumbling, stage.windowDefinition());
    }

    private class WindowTestFixture {
        List<Integer> input = sequence(itemCount);
        DistributedTriFunction<Long, String, String, String> formatFn =
                (timestamp, key, result) -> String.format("(%03d, %s, %s)", timestamp, key, result);

        String srcName1 = journaledMapName();
        String srcName2 = journaledMapName();

        Map<String, Integer> srcMap1 = jet().getMap(srcName1);
        Map<String, Integer> srcMap2 = jet().getMap(srcName2);

        StreamStage<Entry<String, Integer>> stage0() {
            return addKeys(srcStage);
        }

        StreamStage<Entry<String, Integer>> stage1() {
            return addKeys(drawEventJournalValues(srcName1));
        }

        StreamStage<Entry<String, Integer>> stage2() {
            return addKeys(drawEventJournalValues(srcName2));
        }

        private StreamStage<Entry<String, Integer>> addKeys(StreamStage<Integer> stage) {
            return stage.addTimestamps(i -> i, maxLag)
                        .flatMap(i -> Traverser.over(entry("a", i), entry("b", i)));
        }
    }

    @Test
    public void tumblingWindow() {
        // Given
        WindowTestFixture fx = new WindowTestFixture();
        addToSrcMapJournal(fx.input);
        addToSrcMapJournal(closingItems);

        // When
        final int winSize = 4;
        DistributedTriFunction<Long, String, String, String> formatFn = fx.formatFn;
        StreamStage<String> aggregated = fx.stage0()
                .addKey(Entry::getKey)
                .window(tumbling(winSize))
                .aggregate(summingLong(Entry::getValue),
                        (start, end, key, sum) -> formatFn.apply(end, key, sum.toString()));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        Map<String, Integer> expectedBag = toBag(fx.input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .flatMap(start -> {
                    long winEnd = (long) start + winSize;
                    long sum = expectedWindowSum.apply(start);
                    return Stream.of(formatFn.apply(winEnd, "a", String.valueOf(sum)),
                            formatFn.apply(winEnd, "b", String.valueOf(sum)));
                })
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void slidingWindow() {
        // Given
        WindowTestFixture fx = new WindowTestFixture();
        addToSrcMapJournal(fx.input);
        addToSrcMapJournal(closingItems);

        // When
        final int winSize = 4;
        final int slideBy = 2;
        DistributedTriFunction<Long, String, String, String> formatFn = fx.formatFn;
        StreamStage<String> aggregated = fx.stage0()
                .window(sliding(winSize, slideBy))
                .addKey(Entry::getKey)
                .aggregate(summingLong(Entry::getValue), (start, end, key, sum) ->
                        formatFn.apply(end, key, sum.toString()));

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
                .boxed()
                .flatMap(start -> Stream.of(entry("a", start), entry("b", start)))
                .map(e -> {
                    int start = e.getValue();
                    return formatFn.apply((long) start + winSize, e.getKey(),
                            String.valueOf(expectedWindowSum.apply(0, start + winSize)));
                });
        Stream<String> restOfStream = fx.input
                .stream()
                .map(i -> i - i % slideBy)
                .distinct()
                .flatMap(start -> Stream.of(entry("a", start), entry("b", start)))
                .map(e -> {
                    int start = e.getValue();
                    return formatFn.apply((long) start + winSize, e.getKey(),
                            String.valueOf(expectedWindowSum.apply(start, min(start + winSize, itemCount))));
                });
        List<String> expected = concat(headOfStream, restOfStream).collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void slidingWindow_twoAggregations() {
        // Given
        for (int i = 0; i < 3; i++) {
            srcMap.put("key", i);
        }
        addToSrcMapJournal(closingItems);
        // When
        srcStage
                .addTimestamps(i -> i, 0)
                .flatMap(i -> Traverser.over(entry("a", "a" + i), entry("b", "b" + i)))
                .addKey(Entry::getKey)
                .window(sliding(2, 1))
                .aggregate(AggregateOperations.mapping(Entry::getValue, AggregateOperations.toList()))
                .map(TimestampedEntry::getValue)
                .window(tumbling(1))
                .aggregate(AggregateOperations.toSet())
                .drainTo(sink);

        // Then
        jet().newJob(p);
        assertTrueEventually(() -> assertEquals(
                asList(
                        new TimestampedItem<>(1, new HashSet<>(asList(singletonList("a0"), singletonList("b0")))),
                        new TimestampedItem<>(2, new HashSet<>(asList(asList("a0", "a1"), asList("b0", "b1")))),
                        new TimestampedItem<>(3, new HashSet<>(asList(asList("a1", "a2"), asList("b1", "b2")))),
                        new TimestampedItem<>(4, new HashSet<>(asList(singletonList("a2"), singletonList("b2"))))
                ),
                asList(sinkList.toArray())
        ), 5);
    }

    @Test
    public void sessionWindow() {
        // Given
        WindowTestFixture fx = new WindowTestFixture();
        final int sessionLength = 4;
        final int sessionTimeout = 2;
        List<Integer> input = fx.input
                .stream()
                .map(ts -> ts + (ts / sessionLength) * sessionTimeout)
                .collect(toList());
        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        // When
        DistributedTriFunction<Long, String, String, String> formatFn = fx.formatFn;
        StreamStage<String> aggregated = fx.stage0()
                .window(session(sessionTimeout))
                .addKey(Entry::getKey)
                .aggregate(summingLong(Entry::getValue), (start, end, key, sum) ->
                        formatFn.apply(start, key, sum.toString()));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Long, Long> expectedWindowSum = start -> sessionLength * (2 * start + sessionLength - 1) / 2L;
        List<String> expected = input
                .stream()
                .map(i -> (long) i - i % (sessionLength + sessionTimeout))
                .distinct()
                .flatMap(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return Stream.of(formatFn.apply(start, "a", String.valueOf(sum)),
                            formatFn.apply(start, "b", String.valueOf(sum)));
                })
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void aggregate2_withSeparateAggrOps() {
        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        testAggregate2((stage0, stage1) -> stage0.aggregate2(aggrOp, stage1, aggrOp));
    }

    @Test
    public void aggregate2_withAggrOp2() {
        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        testAggregate2((stage0, stage1) -> stage0.aggregate2(stage1, aggregateOperation2(aggrOp, aggrOp)));
    }

    private void testAggregate2(
            BiFunction<
                    StageWithKeyAndWindow<Entry<String, Integer>, String>,
                    StreamStageWithKey<Entry<String, Integer>, String>,
                    StreamStage<TimestampedEntry<String, Tuple2<Long, Long>>>>
                attachAggregatingStageFn
    ) {
        // Given
        WindowTestFixture fx = new WindowTestFixture();
        addToSrcMapJournal(fx.input);
        addToSrcMapJournal(closingItems);

        addToMapJournal(fx.srcMap1, fx.input);
        addToMapJournal(fx.srcMap1, closingItems);

        // When
        final int winSize = 4;
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 =
                fx.stage0()
                  .window(tumbling(winSize))
                  .addKey(Entry::getKey);
        StreamStageWithKey<Entry<String, Integer>, String> stage1 =
                fx.stage1()
                  .addKey(Entry::getKey);
        StreamStage<TimestampedEntry<String, Tuple2<Long, Long>>> aggregated =
                attachAggregatingStageFn.apply(stage0, stage1);

        //Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<TimestampedEntry<String, Tuple2<Long, Long>>> expected = fx.input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .flatMap(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return Stream.of(
                            new TimestampedEntry<>((long) start + winSize, "a", tuple2(sum, sum)),
                            new TimestampedEntry<>((long) start + winSize, "b", tuple2(sum, sum)));
                })
                .collect(toList());
        Map<TimestampedEntry<String, Tuple2<Long, Long>>, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void aggregate2_withSeparateAggrOps_withOutputFn() {
        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        testAggregate2_withOutputFn((stage0, stage1, formatFn) -> stage0.aggregate2(
                aggrOp, stage1, aggrOp,
                (start, end, key, sum0, sum1) -> formatFn.apply(end, key, sum0 + "," + sum1)));
    }

    @Test
    public void aggregate2_withAggrOp2_withOutputFn() {
        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        testAggregate2_withOutputFn((stage0, stage1, formatFn) -> stage0.aggregate2(
                stage1, aggregateOperation2(aggrOp, aggrOp),
                (start, end, key, sums) -> formatFn.apply(end, key, sums.f0() + "," + sums.f1())));
    }

    private void testAggregate2_withOutputFn(
            TriFunction<
                    StageWithKeyAndWindow<Entry<String, Integer>, String>,
                    StreamStageWithKey<Entry<String, Integer>, String>,
                    DistributedTriFunction<Long, String, String, String>,
                    StreamStage<String>
                > attachAggregatingStageFn
    ) {
        // Given
        WindowTestFixture fx = new WindowTestFixture();
        DistributedTriFunction<Long, String, String, String> formatFn = fx.formatFn;

        addToSrcMapJournal(fx.input);
        addToSrcMapJournal(closingItems);

        addToMapJournal(fx.srcMap1, fx.input);
        addToMapJournal(fx.srcMap1, closingItems);

        // When
        final int winSize = 4;

        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.stage0()
                                                                         .window(tumbling(winSize))
                                                                         .addKey(Entry::getKey);
        StreamStageWithKey<Entry<String, Integer>, String> stage1 = fx.stage1().addKey(Entry::getKey);

        StreamStage<String> aggregated = attachAggregatingStageFn.apply(stage0, stage1, formatFn);

        //Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<String> expected = fx.input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .flatMap(start -> {
                    long sum = expectedWindowSum.apply(start);
                    long end = (long) start + winSize;
                    return Stream.of(
                            formatFn.apply(end, "a", sum + "," + sum),
                            formatFn.apply(end, "b", sum + "," + sum));
                })
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void aggregate3_withSeparateAggrOps() {
        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        testAggregate3((stage0, stage1, stage2) -> stage0.aggregate3(aggrOp, stage1, aggrOp, stage2, aggrOp));
    }

    @Test
    public void aggregate3_withAggrOp2() {
        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        testAggregate3((stage0, stage1, stage2) -> stage0.aggregate3(
                stage1, stage2, aggregateOperation3(aggrOp, aggrOp, aggrOp)));
    }

    private void testAggregate3(
            TriFunction<
                    StageWithKeyAndWindow<Entry<String, Integer>, String>,
                    StreamStageWithKey<Entry<String, Integer>, String>,
                    StreamStageWithKey<Entry<String, Integer>, String>,
                    StreamStage<TimestampedEntry<String, Tuple3<Long, Long, Long>>>>
                attachAggregatingStageFn
    ) {
        // Given
        WindowTestFixture fx = new WindowTestFixture();
        addToSrcMapJournal(fx.input);
        addToSrcMapJournal(closingItems);

        addToMapJournal(fx.srcMap1, fx.input);
        addToMapJournal(fx.srcMap1, closingItems);

        addToMapJournal(fx.srcMap2, fx.input);
        addToMapJournal(fx.srcMap2, closingItems);

        // When
        final int winSize = 4;
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 =
                fx.stage0()
                  .window(tumbling(winSize))
                  .addKey(Entry::getKey);
        StreamStageWithKey<Entry<String, Integer>, String> stage1 =
                fx.stage1()
                  .addKey(Entry::getKey);
        StreamStageWithKey<Entry<String, Integer>, String> stage2 =
                fx.stage2()
                  .addKey(Entry::getKey);
        StreamStage<TimestampedEntry<String, Tuple3<Long, Long, Long>>> aggregated =
                attachAggregatingStageFn.apply(stage0, stage1, stage2);

        //Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<TimestampedEntry<String, Tuple3<Long, Long, Long>>> expected = fx.input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .flatMap(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return Stream.of(
                            new TimestampedEntry<>((long) start + winSize, "a", tuple3(sum, sum, sum)),
                            new TimestampedEntry<>((long) start + winSize, "b", tuple3(sum, sum, sum)));
                })
                .collect(toList());
        Map<TimestampedEntry<String, Tuple3<Long, Long, Long>>, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void aggregate3_withSeparateAggrOps_withOutputFn() {
        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        testAggregate3_withOutputFn((stage0, stage1, stage2, formatFn) -> stage0.aggregate3(
                aggrOp, stage1, aggrOp, stage2, aggrOp,
                (start, end, key, sum0, sum1, sum2) -> formatFn.apply(end, key, sum0 + "," + sum1 + ',' + sum2)));
    }

    @Test
    public void aggregate3_withAggrOp2_withOutputFn() {
        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        testAggregate3_withOutputFn((stage0, stage1, stage2, formatFn) -> stage0.aggregate3(
                stage1, stage2, aggregateOperation3(aggrOp, aggrOp, aggrOp),
                (start, end, key, sums) -> formatFn.apply(end, key, sums.f0() + "," + sums.f1() + ',' + sums.f2())));
    }

    private void testAggregate3_withOutputFn(
            QuadFunction<
                    StageWithKeyAndWindow<Entry<String, Integer>, String>,
                    StreamStageWithKey<Entry<String, Integer>, String>,
                    StreamStageWithKey<Entry<String, Integer>, String>,
                    DistributedTriFunction<Long, String, String, String>,
                    StreamStage<String>
                > attachAggregatingStageFn
    ) {
        // Given
        WindowTestFixture fx = new WindowTestFixture();
        DistributedTriFunction<Long, String, String, String> formatFn = fx.formatFn;

        addToSrcMapJournal(fx.input);
        addToSrcMapJournal(closingItems);

        addToMapJournal(fx.srcMap1, fx.input);
        addToMapJournal(fx.srcMap1, closingItems);

        addToMapJournal(fx.srcMap2, fx.input);
        addToMapJournal(fx.srcMap2, closingItems);

        // When
        final int winSize = 4;
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx
                .stage0()
                .window(tumbling(winSize))
                .addKey(Entry::getKey);
        StreamStageWithKey<Entry<String, Integer>, String> stage1 = fx
                .stage1()
                .addKey(Entry::getKey);
        StreamStageWithKey<Entry<String, Integer>, String> stage2 = fx
                .stage2()
                .addKey(Entry::getKey);
        StreamStage<String> aggregated = attachAggregatingStageFn.apply(stage0, stage1, stage2, formatFn);

        //Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<String> expected = fx.input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .flatMap(start -> {
                    long sum = expectedWindowSum.apply(start);
                    long end = (long) start + winSize;
                    return Stream.of(
                            formatFn.apply(end, "a", sum + "," + sum + ',' + sum),
                            formatFn.apply(end, "b", sum + "," + sum + ',' + sum));
                })
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    private class AggregateBuilderFixture {
        List<Integer> input = sequence(itemCount);

        String srcName1 = journaledMapName();
        Map<String, Integer> srcMap1 = jet().getMap(srcName1);

        StreamStage<Entry<String, Integer>> stage0 = addKeys(srcStage);
        StreamStage<Entry<String, Integer>> stage1 = addKeys(drawEventJournalValues(srcName1));
        {
            addToSrcMapJournal(input);
            addToSrcMapJournal(closingItems);
            addToMapJournal(srcMap1, input);
            addToMapJournal(srcMap1, closingItems);
        }

        private StreamStage<Entry<String, Integer>> addKeys(StreamStage<Integer> stage) {
            return stage.addTimestamps(i -> i, maxLag)
                        .flatMap(i -> Traverser.over(entry("a", i), entry("b", i)));
        }
    }

    @Test
    public void aggregateBuilder_withSeparateAggrOps() {
        // Given
        AggregateBuilderFixture fx = new AggregateBuilderFixture();

        // When
        final int winSize = 4;
        StreamStageWithKey<Entry<String, Integer>, String> stage0 = fx.stage0.addKey(Entry::getKey);
        StreamStageWithKey<Entry<String, Integer>, String> stage1 = fx.stage1.addKey(Entry::getKey);

        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        WindowGroupAggregateBuilder<String, Long> b = stage0.window(tumbling(winSize)).aggregateBuilder(aggrOp);
        Tag<Long> tag0 = b.tag0();
        Tag<Long> tag1 = b.add(stage1, aggrOp);
        DistributedTriFunction<Long, String, ItemsByTag, String> formatFn = (timestamp, key, sums) ->
                String.format("(%03d: %03d, %03d)", timestamp, sums.get(tag0), sums.get(tag1));

        StreamStage<String> aggregated = b.build((start, end, key, sums) -> formatFn.apply(end, key, sums));

        // Then
        validateAggrBuilder(aggregated, fx, winSize, tag0, tag1, formatFn);
    }

    @Test
    public void aggregateBuilder_withComplexAggrOp() {
        // Given
        AggregateBuilderFixture fx = new AggregateBuilderFixture();

        // When
        final int winSize = 4;
        StreamStageWithKey<Entry<String, Integer>, String> stage0 = fx.stage0.addKey(Entry::getKey);
        StreamStageWithKey<Entry<String, Integer>, String> stage1 = fx.stage1.addKey(Entry::getKey);

        WindowGroupAggregateBuilder1<Entry<String, Integer>, String> b = stage0
                .window(tumbling(winSize))
                .aggregateBuilder();
        Tag<Entry<String, Integer>> tag0_in = b.tag0();
        Tag<Entry<String, Integer>> tag1_in = b.add(stage1);

        AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> aggrOp = summingLong(Entry::getValue);
        CoAggregateOperationBuilder b2 = coAggregateOperationBuilder();
        Tag<Long> tag0 = b2.add(tag0_in, aggrOp);
        Tag<Long> tag1 = b2.add(tag1_in, aggrOp);
        DistributedTriFunction<Long, String, ItemsByTag, String> formatFn = (timestamp, key, sums) ->
                String.format("(%03d: %03d, %03d)", timestamp, sums.get(tag0), sums.get(tag1));

        StreamStage<String> aggregated = b.build(
                b2.build(),
                (start, end, key, sums) -> formatFn.apply(end, key, sums));

        // Then
        validateAggrBuilder(aggregated, fx, winSize, tag0, tag1, formatFn);
    }

    @SuppressWarnings("SameParameterValue")
    private void validateAggrBuilder(
            StreamStage<String> aggregated, AggregateBuilderFixture fx,
            int winSize,
            Tag<Long> tag0, Tag<Long> tag1,
            DistributedTriFunction<Long, String, ItemsByTag, String> formatFn
    ) {
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> winSize * (2L * start + winSize - 1) / 2;
        List<String> expected = fx.input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .flatMap(start -> {
                    long sum = expectedWindowSum.apply(start);
                    long end = (long) start + winSize;
                    return Stream.of(
                            formatFn.apply(end, "a", itemsByTag(tag0, sum, tag1, sum)),
                            formatFn.apply(end, "b", itemsByTag(tag0, sum, tag1, sum)));
                })
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }
}
