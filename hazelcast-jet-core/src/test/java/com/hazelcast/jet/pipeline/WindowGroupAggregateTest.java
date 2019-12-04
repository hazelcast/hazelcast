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
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.mapping;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.pipeline.WindowDefinition.session;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class WindowGroupAggregateTest extends PipelineStreamTestSupport {

    private static final Function<KeyedWindowResult<String, Long>, Tuple2<Long, String>> TS_ENTRY_DISTINCT_FN =
            kwr -> tuple2(kwr.end(), kwr.key());

    private static final Function<KeyedWindowResult<String, Long>, String> KWR_WIN_START_FORMAT_FN =
            kwr -> String.format("(%04d %s: %04d)", kwr.start(), kwr.key(), kwr.result());

    private static final Function<KeyedWindowResult<String, Long>, String> KWR_WIN_END_FORMAT_FN =
            kwr -> String.format("(%04d %s: %04d)", kwr.end(), kwr.key(), kwr.result());

    private static final Function<KeyedWindowResult<String, Tuple2<Long, Long>>, String> TS_ENTRY_FORMAT_FN_2 =
            kwr -> String.format("(%04d %s: %04d, %04d)",
                    kwr.end(), kwr.key(), kwr.result().f0(), kwr.result().f1());

    private static final Function<KeyedWindowResult<String, Tuple3<Long, Long, Long>>, String> TS_ENTRY_FORMAT_FN_3 =
            kwr -> String.format("(%04d %s: %04d, %04d, %04d)",
                    kwr.end(), kwr.key(), kwr.result().f0(), kwr.result().f1(), kwr.result().f2());

    private static final Function<Entry<Long, Long>, String> MOCK_FORMAT_FN =
            e -> String.format("(%04d a: %04d)\n(%04d b: %04d)", e.getKey(), e.getValue(), e.getKey(), e.getValue());

    private static final Function<Entry<Long, Long>, String> MOCK_FORMAT_FN_2 =
            e -> String.format("(%04d a: %04d, %04d)\n(%04d b: %04d, %04d)",
                    e.getKey(), e.getValue(), e.getValue(),
                    e.getKey(), e.getValue(), e.getValue());

    private static final Function<Entry<Long, Long>, String> MOCK_FORMAT_FN_3 =
            e -> String.format("(%04d a: %04d, %04d, %04d)\n(%04d b: %04d, %04d, %04d)",
                    e.getKey(), e.getValue(), e.getValue(), e.getValue(),
                    e.getKey(), e.getValue(), e.getValue(), e.getValue());

    private static final AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> SUMMING =
            summingLong(Entry::getValue);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_noTimestamps_then_error() {
        StageWithWindow<SimpleEvent> stage = p
                .readFrom(TestSources.itemStream(0))
                // When
                .withoutTimestamps()
                .window(tumbling(1));

        // Then
        exception.expectMessage("is missing a timestamp definition");
        stage.aggregate(counting());
    }

    @Test
    public void windowDefinition() {
        SlidingWindowDefinition tumbling = tumbling(2);
        StageWithKeyAndWindow<Integer, Integer> stage =
                streamStageFromList(emptyList()).groupingKey(wholeItem()).window(tumbling);
        assertEquals(tumbling, stage.windowDefinition());
    }

    private class WindowTestFixture {
        final boolean emittingEarlyResults;

        final SlidingWindowDefinition tumblingWinDef = tumbling(4);

        List<Integer> input = sequence(itemCount);

        final String expectedString2 = new SlidingWindowSimulator(tumblingWinDef)
                .acceptStream(input.stream())
                .stringResults(MOCK_FORMAT_FN_2);

        final String expectedString3 = new SlidingWindowSimulator(tumblingWinDef)
                .acceptStream(input.stream())
                .stringResults(MOCK_FORMAT_FN_3);

        WindowTestFixture(boolean emittingEarlyResults) {
            this.emittingEarlyResults = emittingEarlyResults;
        }

        StreamStageWithKey<Entry<String, Integer>, String> newSourceStage() {
            StreamStage<Integer> sourceStage = emittingEarlyResults
                    ? streamStageFromList(input, EARLY_RESULTS_PERIOD)
                    : streamStageFromList(input);
            return sourceStage.flatMap(i -> traverseItems(entry("a", i), entry("b", i)))
                              .groupingKey(Entry::getKey);
        }
    }

    @Test
    public void distinct() {
        // Given
        itemCount = (int) roundUp(itemCount, 2);
        int winSize = itemCount / 2;
        List<Integer> timestamps = sequence(itemCount);
        StageWithKeyAndWindow<Integer, Integer> windowed = streamStageFromList(timestamps)
                .groupingKey(i -> i / 2)
                .window(tumbling(winSize));

        // When
        StreamStage<KeyedWindowResult<Integer, Integer>> distinct = windowed.distinct();

        // Then
        distinct.writeTo(sink);
        execute();
        assertEquals(
                IntStream.range(0, itemCount)
                         .mapToObj(i -> String.format("(%04d, %04d)", roundUp(i + 1, winSize), i / 2))
                         .distinct()
                         .sorted()
                         .collect(joining("\n")),
                streamToString(
                        this.<Integer>sinkStreamOfWinResult(),
                        wr -> String.format("(%04d, %04d)", wr.end(), wr.result() / 2))
        );
    }

    @Test
    public void tumblingWindow() {
        // Given
        final int winSize = 4;
        WindowTestFixture fx = new WindowTestFixture(false);

        // When
        SlidingWindowDefinition wDef = tumbling(winSize);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING)
                .writeTo(sink);
        execute();
        assertEquals(
                new SlidingWindowSimulator(wDef)
                        .acceptStream(fx.input.stream())
                        .stringResults(MOCK_FORMAT_FN),
                streamToString(sinkStreamOfKeyedWinResult(), KWR_WIN_END_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        );
    }

    @Test
    public void tumblingWindow_withEarlyResults() {
        // Given
        final int winSize = 4;
        WindowTestFixture fx = new WindowTestFixture(true);

        // When
        SlidingWindowDefinition wDef = tumbling(winSize).setEarlyResultsPeriod(EARLY_RESULTS_PERIOD);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING)
                .writeTo(sink);
        jet().newJob(p);
        String expectedString = new SlidingWindowSimulator(wDef)
                .acceptStream(fx.input.stream())
                .stringResults(MOCK_FORMAT_FN);
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkStreamOfKeyedWinResult(), KWR_WIN_END_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void slidingWindow() {
        // Given
        final int winSize = 4;
        final int slideBy = 2;
        WindowTestFixture fx = new WindowTestFixture(false);

        // When
        SlidingWindowDefinition wDef = sliding(winSize, slideBy);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING)
                .writeTo(sink);
        execute();
        assertEquals(
                new SlidingWindowSimulator(wDef)
                        .acceptStream(fx.input.stream())
                        .stringResults(MOCK_FORMAT_FN),
                streamToString(sinkStreamOfKeyedWinResult(), KWR_WIN_END_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        );
    }

    @Test
    public void slidingWindow_withEarlyResults() {
        // Given
        final int winSize = 4;
        final int slideBy = 2;
        WindowTestFixture fx = new WindowTestFixture(true);

        // When
        SlidingWindowDefinition wDef = sliding(winSize, slideBy).setEarlyResultsPeriod(EARLY_RESULTS_PERIOD);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING)
                .writeTo(sink);
        jet().newJob(p);
        String expectedString = new SlidingWindowSimulator(wDef)
                .acceptStream(fx.input.stream())
                .stringResults(MOCK_FORMAT_FN);
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkStreamOfKeyedWinResult(), KWR_WIN_END_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void slidingWindow_cascadingAggregations() {
        // Given
        List<Integer> input = asList(0, 1, 2);
        StreamStage<Entry<String, String>> srcStage =
                streamStageFromList(input)
                        .flatMap(i -> traverseItems(entry("a", "a" + i), entry("b", "b" + i)));

        // When
        srcStage.groupingKey(Entry::getKey)
                .window(sliding(2, 1))
                .aggregate(mapping(Entry::getValue, AggregateOperations.toList()))
                .map(KeyedWindowResult::result)
                .window(tumbling(1))
                .aggregate(AggregateOperations.toList())
                .map(wr -> {
                    List<List<String>> listOfList = wr.result();
                    listOfList.sort(comparing(l -> l.get(0)));
                    return formatTsItem(wr.end(), listOfList.get(0), listOfList.get(1));
                })
                .writeTo(sink);

        // Then
        execute();
        assertEquals(
                String.join("\n",
                        formatTsItem(1, singletonList("a0"), singletonList("b0")),
                        formatTsItem(2, asList("a0", "a1"), asList("b0", "b1")),
                        formatTsItem(3, asList("a1", "a2"), asList("b1", "b2")),
                        formatTsItem(4, singletonList("a2"), singletonList("b2"))
                ),
                streamToString(sinkList.stream().map(String.class::cast), identity()));
    }

    private static String formatTsItem(long timestamp, List<String> l1, List<String> l2) {
        return String.format("%04d: [%s, %s]", timestamp, l1, l2);
    }

    @Test
    public void sessionWindow() {
        // Given
        final int sessionLength = 4;
        final int sessionTimeout = 2;
        WindowTestFixture fx = new WindowTestFixture(false);
        fx.input = sequence(itemCount / sessionLength * sessionLength)
                .stream()
                .map(ts -> ts + (ts / sessionLength) * sessionTimeout)
                .collect(toList());

        // When
        SessionWindowDefinition wDef = session(sessionTimeout);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING)
                .writeTo(sink);
        execute();

        assertEquals(
                new SessionWindowSimulator(wDef, sessionLength + sessionTimeout)
                        .acceptStream(fx.input.stream())
                        .stringResults(MOCK_FORMAT_FN),
                streamToString(sinkStreamOfKeyedWinResult(), KWR_WIN_START_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        );
    }

    @Test
    public void sessionWindow_withEarlyResults() {
        // Given
        final int sessionLength = 4;
        final int sessionTimeout = 2;
        WindowTestFixture fx = new WindowTestFixture(true);
        fx.input = fx.input
                .stream()
                .map(ts -> ts + (ts / sessionLength) * sessionTimeout)
                .collect(toList());

        // When
        SessionWindowDefinition wDef = session(sessionTimeout).setEarlyResultsPeriod(EARLY_RESULTS_PERIOD);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING)
                // suppress incomplete windows to get predictable results
                .filter(wr -> wr.end() - wr.start() == sessionLength + sessionTimeout - 1)
                .writeTo(sink);
        jet().newJob(p);
        String expectedString = new SessionWindowSimulator(wDef, sessionLength + sessionTimeout)
                .acceptStream(fx.input.stream())
                .stringResults(MOCK_FORMAT_FN);
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkStreamOfKeyedWinResult(), KWR_WIN_START_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregate2_withSeparateAggrOps() {
        // Given
        final int winSize = 4;
        WindowTestFixture fx = new WindowTestFixture(false);
        SlidingWindowDefinition wDef = tumbling(winSize);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(wDef);

        // When
        StreamStage<KeyedWindowResult<String, Tuple2<Long, Long>>> aggregated =
                stage0.aggregate2(SUMMING, fx.newSourceStage(), SUMMING);

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString2,
                streamToString(sinkStreamOfKeyedWinResult(), TS_ENTRY_FORMAT_FN_2));
    }

    @Test
    public void aggregate2_withAggrOp2() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<KeyedWindowResult<String, Tuple2<Long, Long>>> aggregated =
                stage0.aggregate2(fx.newSourceStage(), aggregateOperation2(SUMMING, SUMMING));

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString2,
                streamToString(sinkStreamOfKeyedWinResult(), TS_ENTRY_FORMAT_FN_2));
    }

    @Test
    public void aggregate3_withSeparateAggrOps() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<KeyedWindowResult<String, Tuple3<Long, Long, Long>>> aggregated =
                stage0.aggregate3(SUMMING, fx.newSourceStage(), SUMMING, fx.newSourceStage(), SUMMING);

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString3,
                streamToString(sinkStreamOfKeyedWinResult(), TS_ENTRY_FORMAT_FN_3)
        );
    }

    @Test
    public void aggregate3_withAggrOp3() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<KeyedWindowResult<String, Tuple3<Long, Long, Long>>> aggregated =
                stage0.aggregate3(fx.newSourceStage(), fx.newSourceStage(),
                        aggregateOperation3(SUMMING, SUMMING, SUMMING));

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString3,
                streamToString(sinkStreamOfKeyedWinResult(), TS_ENTRY_FORMAT_FN_3));
    }

    @Test
    public void aggregateBuilder_withSeparateAggrOps() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StreamStageWithKey<Entry<String, Integer>, String> stage0 = fx.newSourceStage();
        StreamStageWithKey<Entry<String, Integer>, String> stage1 = fx.newSourceStage();

        // When
        WindowGroupAggregateBuilder<String, Long> b = stage0.window(fx.tumblingWinDef).aggregateBuilder(SUMMING);
        Tag<Long> tag0 = b.tag0();
        Tag<Long> tag1 = b.add(stage1, SUMMING);
        StreamStage<KeyedWindowResult<String, ItemsByTag>> aggregated = b.build();

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString2,
                streamToString(this.<ItemsByTag>sinkStreamOfKeyedWinResult(), wr -> String.format(
                        "(%04d %s: %04d, %04d)", wr.end(), wr.key(), wr.result().get(tag0), wr.result().get(tag1))));
    }

    @Test
    public void aggregateBuilder_withComplexAggrOp() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);

        // When
        StreamStageWithKey<Entry<String, Integer>, String> stage0 = fx.newSourceStage();
        StreamStageWithKey<Entry<String, Integer>, String> stage1 = fx.newSourceStage();

        WindowGroupAggregateBuilder1<Entry<String, Integer>, String> b = stage0
                .window(fx.tumblingWinDef)
                .aggregateBuilder();
        Tag<Entry<String, Integer>> tag0_in = b.tag0();
        Tag<Entry<String, Integer>> tag1_in = b.add(stage1);

        CoAggregateOperationBuilder b2 = coAggregateOperationBuilder();
        Tag<Long> tag0 = b2.add(tag0_in, SUMMING);
        Tag<Long> tag1 = b2.add(tag1_in, SUMMING);

        StreamStage<KeyedWindowResult<String, ItemsByTag>> aggregated = b.build(b2.build());

        // Then
        aggregated.writeTo(sink);
        execute();
        assertEquals(fx.expectedString2,
                streamToString(this.<ItemsByTag>sinkStreamOfKeyedWinResult(), wr -> String.format(
                        "(%04d %s: %04d, %04d)", wr.end(), wr.key(), wr.result().get(tag0), wr.result().get(tag1))));
    }
}
