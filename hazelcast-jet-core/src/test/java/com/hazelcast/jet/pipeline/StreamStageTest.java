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

import com.hazelcast.core.IMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.JetEvent;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class StreamStageTest extends PipelineStreamTestSupport {

    private static BiFunction<String, Integer, String> ENRICHING_FORMAT_FN =
            (prefix, i) -> String.format("%s-%04d", prefix, i);

    @Test
    public void setName() {
        // Given
        String stageName = randomName();

        // When
        StreamStage<Integer> stage = sourceStageFromList(emptyList());
        stage.setName(stageName);

        // Then
        assertEquals(stageName, stage.name());
    }

    @Test
    public void setLocalParallelism() {
        // Given
        int localParallelism = 10;

        // When
        StreamStage<Integer> stage = sourceStageFromList(emptyList());
        stage.setLocalParallelism(localParallelism);

        // Then
        assertEquals(localParallelism, transformOf(stage).localParallelism());
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> mapFn = item -> String.format("%04d-x", item);

        // When
        StreamStage<String> mapped = sourceStageFromList(input).map(mapFn);

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(input.stream().map(mapFn), identity());
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkList.stream(), Object::toString)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void filter() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedPredicate<Integer> filterFn = i -> i % 2 == 1;
        Function<Integer, String> formatFn = i -> String.format("%04d", i);

        // When
        StreamStage<Integer> filtered = sourceStageFromList(input).filter(filterFn);

        // Then
        filtered.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(input.stream().filter(filterFn), formatFn);
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkStreamOfInt(), formatFn)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void flatMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Stream<String>> flatMapFn =
                i -> Stream.of("A", "B").map(s -> String.format("%04d-%s", i, s));

        // When
        StreamStage<String> flatMapped = sourceStageFromList(input)
                .flatMap(o -> traverseStream(flatMapFn.apply(o)));

        // Then
        flatMapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(input.stream().flatMap(flatMapFn), identity());
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkList.stream(), Object::toString)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void mapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<String, Integer, String> formatFn = (suffix, i) -> String.format("%04d%s", i, suffix);
        String suffix = "-context";

        // When
        StreamStage<String> mapped = sourceStageFromList(input).mapUsingContext(
                ContextFactory.withCreateFn(x -> suffix),
                formatFn);

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);

        String expectedString = streamToString(input.stream().map(i -> formatFn.apply(suffix, i)), identity());
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkList.stream(), Object::toString)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void mapUsingContext_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<String, Integer, String> formatFn = (suffix, i) -> String.format("%04d%s", i, suffix);
        String suffix = "-keyed-context";

        // When
        StreamStage<String> mapped = sourceStageFromList(input)
                .groupingKey(i -> i)
                .mapUsingContext(ContextFactory.withCreateFn(i -> suffix), (suff, k, i) -> formatFn.apply(suff, i));

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(input.stream().map(i -> formatFn.apply(suffix, i)), identity());
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkList.stream(), Object::toString)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void filterUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        int acceptedRemainder = 1;
        Function<Integer, String> formatFn = i -> String.format("%04d", i);

        // When
        StreamStage<Integer> mapped = sourceStageFromList(input)
                .filterUsingContext(ContextFactory.withCreateFn(i -> acceptedRemainder), (rem, i) -> i % 2 == rem);

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);

        String expectedString = streamToString(
                input.stream().filter(i -> i % 2 == acceptedRemainder),
                formatFn);
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkStreamOfInt(), formatFn)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void filterUsingContext_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        int acceptedRemainder = 1;

        // When
        StreamStage<Integer> mapped = sourceStageFromList(input)
                .groupingKey(i -> i)
                .filterUsingContext(
                        ContextFactory.withCreateFn(i -> acceptedRemainder),
                        (rem, k, i) -> i % 2 == rem);

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(
                input.stream().filter(r -> r % 2 == acceptedRemainder),
                formatFn);
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkStreamOfInt(), formatFn)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void flatMapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Stream<String>> flatMapFn =
                i -> Stream.of("A", "B").map(s -> String.format("%04d-%s", i, s));

        // When
        StreamStage<String> flatMapped = sourceStageFromList(input)
                .flatMapUsingContext(
                        ContextFactory.withCreateFn(x -> flatMapFn),
                        (fn, i) -> traverseStream(fn.apply(i))
                );

        // Then
        flatMapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(input.stream().flatMap(flatMapFn), identity());
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkList.stream(), Object::toString)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void flatMapUsingContext_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Stream<String>> flatMapFn =
                i -> Stream.of("A", "B").map(s -> String.format("%04d-%s", i, s));

        // When
        StreamStage<String> flatMapped = sourceStageFromList(input)
                .groupingKey(i -> i)
                .flatMapUsingContext(
                        ContextFactory.withCreateFn(x -> flatMapFn),
                        (fn, k, i) -> traverseStream(fn.apply(i))
                );

        // Then
        flatMapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(input.stream().flatMap(flatMapFn), identity());
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkList.stream(), Object::toString)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void mapUsingReplicatedMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        String valuePrefix = "value-";
        ReplicatedMap<Integer, String> map = member.getReplicatedMap(randomMapName());
        // ReplicatedMap: {0 -> "value-0000, 1 -> "value-0001", ...}
        for (int i : input) {
            map.put(i, String.format("%s%04d", valuePrefix, i));
        }

        // When
        StreamStage<Entry<Integer, String>> mapped = sourceStageFromList(input)
                .mapUsingReplicatedMap(map, (m, i) -> entry(i, m.get(i)));

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(
                input.stream().map(i -> String.format("(%04d, %s%04d)", i, valuePrefix, i)),
                identity());
        // sinkList: entry(0, "value-0000"), entry(1, "value-0001"), ...
        assertTrueEventually(
                () -> assertEquals(expectedString,
                        streamToString(
                                this.<Integer, String>sinkStreamOfEntry(),
                                e -> String.format("(%04d, %s)", e.getKey(), e.getValue()))),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void mapUsingIMapAsync() {
        // Given
        List<Integer> input = sequence(itemCount);
        String valuePrefix = "value-";
        IMap<Integer, String> map = member.getMap(randomMapName());
        // IMap: {0 -> "value-0000, 1 -> "value-0001", ...}
        for (int i : input) {
            map.put(i, String.format("%s%04d", valuePrefix, i));
        }

        // When
        StreamStage<Entry<Integer, String>> mapped = sourceStageFromList(input)
                .mapUsingIMapAsync(map, (m, i) -> Util.toCompletableFuture(m.getAsync(i))
                                                      .thenApply(v -> entry(i, v)));

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(
                input.stream().map(i -> String.format("(%04d, %s%04d)", i, valuePrefix, i)),
                identity());
        // sinkList: entry(0, "value-0000"), entry(1, "value-0001"), ...
        assertTrueEventually(
                () -> assertEquals(expectedString,
                        streamToString(
                                this.<Integer, String>sinkStreamOfEntry(),
                                e -> String.format("(%04d, %s)", e.getKey(), e.getValue()))),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void mapUsingIMapAsync_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        String valuePrefix = "value-";
        IMap<Integer, String> map = member.getMap(randomMapName());
        // IMap: {0 -> "value-0000, 1 -> "value-0001", ...}
        for (int i : input) {
            map.put(i, String.format("%s%04d", valuePrefix, i));
        }

        // When
        StreamStage<Entry<Integer, String>> mapped = sourceStageFromList(input)
                .groupingKey(i -> i)
                .mapUsingIMapAsync(map, Util::entry);

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(
                input.stream().map(i -> String.format("(%04d, %s%04d)", i, valuePrefix, i)),
                identity());
        // sinkList: entry(0, "value-0000"), entry(1, "value-0001"), ...
        assertTrueEventually(
                () -> assertEquals(expectedString,
                        streamToString(
                                this.<Integer, String>sinkStreamOfEntry(),
                                e -> String.format("(%04d, %s)", e.getKey(), e.getValue()))),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void rollingAggregate() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        StreamStage<Long> rolled = sourceStageFromList(input)
                .rollingAggregate(counting());

        // Then
        rolled.drainTo(sink);
        jet().newJob(p);
        Function<Object, String> formatFn = i -> String.format("%04d", (Long) i);
        String expectedString = streamToString(LongStream.rangeClosed(1, itemCount).boxed(), formatFn);
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkList.stream(), formatFn)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void rollingAggregate_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        StreamStage<Entry<Integer, Long>> mapped = sourceStageFromList(input)
                .groupingKey(i -> i % 2)
                .rollingAggregate(counting());

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        Function<Entry<Integer, Long>, String> formatFn = e -> String.format("(%d, %04d)", e.getKey(), e.getValue());
        String expectedString = streamToString(
                IntStream.range(2, itemCount + 2).mapToObj(i -> entry(i % 2, (long) i / 2)),
                formatFn
        );
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkStreamOfEntry(), formatFn)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void rollingAggregate_keyed_withOutputFn() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Integer, Long, String> formatFn =
                (key, count) -> String.format("(%d, %04d)", key, count);

        // When
        StreamStage<String> mapped = sourceStageFromList(input)
                .groupingKey(i -> i % 2)
                .rollingAggregate(counting(), formatFn);

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(
                IntStream.range(2, itemCount + 2).mapToObj(i -> entry(i % 2, (long) i / 2)),
                e -> formatFn.apply(e.getKey(), e.getValue())
        );
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkList.stream().map(String.class::cast), identity())
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void rollingAggregate_when_outputFnReturnsNull_then_filteredOut() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        StreamStage<String> mapped = sourceStageFromList(input)
                .groupingKey(i -> i % 2)
                .rollingAggregate(counting(), (x, y) -> null);

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        assertTrueFiveSeconds(() -> assertEquals(0, sinkList.size()));
    }

    @Test
    public void when_rollingAggregateWithTimestamps_then_timestampsPropagated() {
        // Given
        List<Integer> input = sequence(itemCount);
        AggregateOperation1<Integer, LongAccumulator, Integer> identity = AggregateOperation
                .withCreate(LongAccumulator::new)
                .<Integer>andAccumulate((acc, i) -> acc.set((long) i))
                .andExportFinish(acc -> (int) acc.get());

        // When
        StreamStage<Integer> rolling = sourceStageFromList(input).rollingAggregate(identity);

        // Then
        rolling.window(tumbling(1))
               .aggregate(identity)
               .drainTo(sink);
        jet().newJob(p);
        String expectedString = LongStream.range(0, itemCount)
                                          .mapToObj(i -> String.format("(%04d %04d)", i + 1, i))
                                          .collect(joining("\n"));
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(
                        this.<Long>sinkStreamOfTsItem(),
                        tsItem -> String.format("(%04d %04d)", tsItem.timestamp(), tsItem.item()))
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void merge() {
        // Given
        List<Integer> input = sequence(itemCount);
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        StreamStage<Integer> srcStage0 = sourceStageFromList(input);
        StreamStage<Integer> srcStage1 = sourceStageFromList(input);

        // When
        StreamStage<Integer> merged = srcStage0.merge(srcStage1);

        // Then
        merged.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(input.stream().flatMap(i -> Stream.of(i, i)), formatFn);
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkStreamOfInt(), formatFn)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void hashJoin() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefixA = "A";
        // entry(0, "A-0000"), entry(1, "A-0001"), ...
        BatchStage<Entry<Integer, String>> enrichingStage = enrichingStage(input, prefixA);

        // When
        @SuppressWarnings("Convert2MethodRef")
        // there's a method ref bug in JDK
        StreamStage<Tuple2<Integer, String>> hashJoined = sourceStageFromList(input).hashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                (i, valueA) -> tuple2(i, valueA)
        );

        // Then
        hashJoined.drainTo(sink);
        jet().newJob(p);
        BiFunction<Integer, String, String> formatFn = (i, value) -> String.format("(%04d, %s)", i, value);
        String expectedString = streamToString(
                input.stream().map(i -> formatFn.apply(i, ENRICHING_FORMAT_FN.apply(prefixA, i))),
                identity());
        // sinkList: tuple2(0, "A-0000"), tuple2(1, "A-0001"), ...
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkList.stream().<Tuple2<Integer, String>>map(Tuple2.class::cast),
                        t2 -> formatFn.apply(t2.f0(), t2.f1()))
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void hashJoin2() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefixA = "A";
        String prefixB = "B";
        // entry(0, "A-0000"), entry(1, "A-0001"), ...
        BatchStage<Entry<Integer, String>> enrichingStage1 = enrichingStage(input, prefixA);
        // entry(0, "B-0000"), entry(1, "B-0001"), ...
        BatchStage<Entry<Integer, String>> enrichingStage2 = enrichingStage(input, prefixB);

        // When
        @SuppressWarnings("Convert2MethodRef")
        // there's a method ref bug in JDK
        StreamStage<Tuple3<Integer, String, String>> hashJoined = sourceStageFromList(input).hashJoin2(
                enrichingStage1, joinMapEntries(wholeItem()),
                enrichingStage2, joinMapEntries(wholeItem()),
                (i, valueA, valueB) -> tuple3(i, valueA, valueB)
        );

        // Then
        hashJoined.drainTo(sink);
        jet().newJob(p);

        TriFunction<Integer, String, String, String> formatFn =
                (i, valueA, valueB) -> String.format("(%04d, %s, %s)", i, valueA, valueB);
        String expectedString = streamToString(
                input.stream().map(i -> formatFn.apply(i,
                        ENRICHING_FORMAT_FN.apply(prefixA, i),
                        ENRICHING_FORMAT_FN.apply(prefixB, i)
                )),
                identity());
        // sinkList: tuple3(0, "A-0000", "B-0000"), tuple3(1, "A-0001", "B-0001"), ...
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkList.stream().<Tuple3<Integer, String, String>>map(Tuple3.class::cast),
                        t3 -> formatFn.apply(t3.f0(), t3.f1(), t3.f2()))
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void hashJoinBuilder() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefixA = "A";
        String prefixB = "B";
        // entry(0, "A-0000"), entry(1, "A-0001"), ...
        BatchStage<Entry<Integer, String>> enrichingStage1 = enrichingStage(input, prefixA);
        // entry(0, "B-0000"), entry(1, "B-0001"), ...
        BatchStage<Entry<Integer, String>> enrichingStage2 = enrichingStage(input, prefixB);

        // When
        StreamHashJoinBuilder<Integer> builder = sourceStageFromList(input).hashJoinBuilder();
        Tag<String> tagA = builder.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = builder.add(enrichingStage2, joinMapEntries(wholeItem()));
        @SuppressWarnings("Convert2MethodRef")
        // there's a method ref bug in JDK
        GeneralStage<Tuple2<Integer, ItemsByTag>> joined = builder.build((a, b) -> tuple2(a, b));

        // Then
        joined.drainTo(sink);
        jet().newJob(p);

        TriFunction<Integer, String, String, String> formatFn =
                (i, valueA, valueB) -> String.format("(%04d, %s, %s)", i, valueA, valueB);
        String expectedString = streamToString(
                input.stream().map(i -> formatFn.apply(i,
                        ENRICHING_FORMAT_FN.apply(prefixA, i),
                        ENRICHING_FORMAT_FN.apply(prefixB, i)
                )),
                identity());
        // sinkList: tuple2(0, ibt(tagA: "A-0000", tagB: "B-0000")), tuple2(1, ibt(tagA: "A-0001", tagB: "B-0001"))
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkList.stream().<Tuple2<Integer, ItemsByTag>>map(Tuple2.class::cast),
                        t2 -> formatFn.apply(t2.f0(), t2.f1().get(tagA), t2.f1().get(tagB)))
        ), ASSERT_TIMEOUT_SECONDS);
    }

    private BatchStage<Entry<Integer, String>> enrichingStage(List<Integer> input, String prefix) {
        return p.drawFrom(SourceBuilder.batch("data", x -> null)
                .<Entry<Integer, String>>fillBufferFn((x, buf) -> {
                    input.forEach(i -> buf.add(entry(i, ENRICHING_FORMAT_FN.apply(prefix, i))));
                    buf.close();
                }).build());
    }

    @Test
    public void customTransform() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> mapFn = item -> String.format("%04d-x", item);

        // When
        StreamStage<String> custom = sourceStageFromList(input).customTransform("map",
                Processors.mapP(o -> {
                    @SuppressWarnings("unchecked")
                    JetEvent<Integer> jetEvent = (JetEvent<Integer>) o;
                    return jetEvent(mapFn.apply(jetEvent.payload()), jetEvent.timestamp());
                }));

        // Then
        custom.drainTo(sink);
        jet().newJob(p);
        String expectedString = streamToString(input.stream().map(mapFn), identity());
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkList.stream(), Object::toString)),
                ASSERT_TIMEOUT_SECONDS);
    }


    @Test
    public void customTransform_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> extractKeyFn = i -> i % 2;

        // When
        StreamStage<Object> custom = sourceStageFromList(input)
                .groupingKey(extractKeyFn)
                .customTransform("map", Processors.mapUsingContextP(
                        ContextFactory.withCreateFn(jet -> new HashSet<>()),
                        (Set<Integer> seen, JetEvent<Integer> jetEvent) -> {
                            Integer key = extractKeyFn.apply(jetEvent.payload());
                            return seen.add(key) ? jetEvent(key, jetEvent.timestamp()) : null;
                        }));

        // Then
        custom.drainTo(sink);
        jet().newJob(p);

        // Each processor emitted distinct keys it observed. If groupingKey isn't
        // correctly partitioning, multiple processors will observe the same keys.
        assertTrueEventually(() -> assertEquals(
                "0\n1",
                streamToString(sinkList.stream().map(Object::toString), identity())
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void peek_when_addedTimestamp_then_unwrapsJetEvent() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        StreamStage<Integer> peeked = sourceStageFromList(input).peek();

        // Then
        peeked.drainTo(sink);
        jet().newJob(p);
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        String expectedString = streamToString(input.stream(), formatFn);
        assertTrueEventually(
                () -> assertEquals(expectedString, streamToString(sinkStreamOfInt(), formatFn)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void peekWithToStringFunctionIsTransparent() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedPredicate<Integer> filterFn = i -> i % 2 == 1;

        // When
        sourceStageFromList(input)
         .filter(filterFn)
         .peek(Object::toString)
         .drainTo(sink);

        // Then
        jet().newJob(p);

        Map<Integer, Integer> expected = toBag(input.stream().filter(filterFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()), ASSERT_TIMEOUT_SECONDS);
    }
}
