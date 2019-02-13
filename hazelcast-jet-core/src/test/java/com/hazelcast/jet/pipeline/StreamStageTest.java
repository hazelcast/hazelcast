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
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class StreamStageTest extends PipelineStreamTestSupport {

    @Test
    public void setName() {
        //Given
        String stageName = randomName();

        //When
        StreamStage<Integer> stage = srcStage.withoutTimestamps();
        stage.setName(stageName);

        //Then
        assertEquals(stageName, stage.name());
    }

    @Test
    public void setLocalParallelism() {
        //Given
        int localParallelism = 10;

        //When
        StreamStage<Integer> stage = srcStage.withoutTimestamps();
        stage.setLocalParallelism(localParallelism);

        //Then
        assertEquals(localParallelism, transformOf(stage).localParallelism());
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        DistributedFunction<Integer, String> mapFn = item -> item + "-x";

        // When
        StreamStage<String> mapped = srcStage.withoutTimestamps().map(mapFn);

        // Then
        mapped.drainTo(sink);
        executeAsync();
        Map<String, Integer> expected = toBag(input.stream().map(mapFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void filter() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        DistributedPredicate<Integer> filterFn = i -> i % 2 == 1;

        // When
        StreamStage<Integer> filtered = srcStage.withoutTimestamps().filter(filterFn);

        // Then
        filtered.drainTo(sink);
        executeAsync();
        Map<Integer, Integer> expected = toBag(input.stream().filter(filterFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void flatMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        DistributedFunction<Integer, Stream<String>> flatMapFn = o -> Stream.of(o + "A", o + "B");

        // When
        StreamStage<String> flatMapped = srcStage.withoutTimestamps().flatMap(o -> traverseStream(flatMapFn.apply(o)));

        // Then
        flatMapped.drainTo(sink);
        executeAsync();
        Map<String, Integer> expected = toBag(input.stream().flatMap(flatMapFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void mapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        // When
        StreamStage<String> mapped = srcStage
                .withIngestionTimestamps()
                .mapUsingContext(ContextFactory.withCreateFn(i -> "-context"), (suffix, r) -> r + suffix);

        // Then
        mapped.drainTo(sink);
        executeAsync();

        List<String> expected = input.stream().map(r -> r + "-context").collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void mapUsingContext_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        // When
        StreamStage<String> mapped = srcStage
                .withIngestionTimestamps()
                .groupingKey(i -> i)
                .mapUsingContext(ContextFactory.withCreateFn(i -> "-keyed-context"), (suffix, k, r) -> r + suffix);

        // Then
        mapped.drainTo(sink);

        executeAsync();

        List<String> expected = input.stream().map(r -> r + "-keyed-context").collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void filterUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        // When
        StreamStage<Integer> mapped = srcStage
                .withIngestionTimestamps()
                .filterUsingContext(ContextFactory.withCreateFn(i -> 1), (ctx, r) -> r % 2 == ctx);

        // Then
        mapped.drainTo(sink);
        executeAsync();

        List<Integer> expected = input.stream()
                .filter(r -> r % 2 == 1)
                .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void filterUsingContext_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        // When
        StreamStage<Integer> mapped = srcStage
                .withIngestionTimestamps()
                .groupingKey(i -> i)
                .filterUsingContext(ContextFactory.withCreateFn(i -> 1), (ctx, k, r) -> r % 2 == ctx);

        // Then
        mapped.drainTo(sink);
        executeAsync();
        List<Integer> expected = input.stream()
                .filter(r -> r % 2 == 1)
                .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void flatMapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        DistributedFunction<Integer, Stream<String>> flatMapFn = o -> Stream.of(o + "A", o + "B");

        // When
        StreamStage<String> flatMapped = srcStage
                .withIngestionTimestamps()
                .flatMapUsingContext(
                        ContextFactory.withCreateFn(procCtx -> flatMapFn),
                        (ctx, o) -> traverseStream(ctx.apply(o))
                );

        // Then
        flatMapped.drainTo(sink);
        executeAsync();

        Map<String, Integer> expected = toBag(input.stream().flatMap(flatMapFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void flatMapUsingContext_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        DistributedFunction<Integer, Stream<String>> flatMapFn = o -> Stream.of(o + "A", o + "B");

        // When
        StreamStage<String> flatMapped = srcStage
                .withIngestionTimestamps()
                .groupingKey(i -> i)
                .flatMapUsingContext(
                        ContextFactory.withCreateFn(procCtx -> flatMapFn),
                        (ctx, k, o) -> traverseStream(ctx.apply(o))
                );

        // Then
        flatMapped.drainTo(sink);
        executeAsync();

        Map<String, Integer> expected = toBag(input.stream().flatMap(flatMapFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void mapUsingReplicatedMap() {
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        ReplicatedMap<Integer, String> map = member.getReplicatedMap(randomMapName());
        for (int i : input) {
            map.put(i, String.valueOf(i));
        }

        srcStage.withIngestionTimestamps()
                .mapUsingReplicatedMap(map, (m, r) -> entry(r, m.get(r)))
                .drainTo(sink);

        executeAsync();

        List<Entry<Integer, String>> expected = input.stream()
                .map(i -> entry(i, String.valueOf(i)))
                .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void mapUsingIMapAsync() {
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        IMap<Integer, String> map = member.getMap(randomMapName());
        for (int i : input) {
            map.put(i, String.valueOf(i));
        }

        srcStage.withIngestionTimestamps()
                .mapUsingIMapAsync(map, (m, r) -> Util.toCompletableFuture(m.getAsync(r))
                                                      .thenApply(a -> entry(r, a)))
                .drainTo(sink);

        executeAsync();

        List<Entry<Integer, String>> expected = input.stream()
                .map(i -> entry(i, String.valueOf(i)))
                .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void mapUsingIMapAsync_keyed() {
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        IMap<Integer, String> map = member.getMap(randomMapName());
        for (int integer : input) {
            map.put(integer, String.valueOf(integer));
        }

        srcStage.withIngestionTimestamps()
                .groupingKey(r -> r)
                .mapUsingIMapAsync(map, (k, v) -> Util.entry(k, v))
                .drainTo(sink);

        executeAsync();

        List<Entry<Integer, String>> expected = input.stream()
                .map(i -> entry(i, String.valueOf(i)))
                .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }


    @Test
    public void rollingAggregate_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        StreamStage<Entry<Integer, Long>> mapped = srcStage
                .withoutTimestamps()
                .groupingKey(i -> i % 2)
                .rollingAggregate(counting());

        // Then
        mapped.drainTo(sink);
        executeAsync();

        assertEquals(0, itemCount % 2);
        Map<Entry<Integer, Long>, Integer> expected = toBag(LongStream.range(1, itemCount / 2 + 1)
                .boxed()
                .flatMap(i -> Stream.of(entry(0, i), entry(1, i)))
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void rollingAggregate_global() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        StreamStage<Long> mapped = srcStage
                .withoutTimestamps()
                .rollingAggregate(counting());

        // Then
        mapped.drainTo(sink);
        executeAsync();

        Map<Long, Integer> expected = toBag(LongStream.range(1, itemCount + 1).boxed().collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void when_rollingAggregateWithTimestamps_then_timestampsPropagated() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);
        AggregateOperation1<Integer, MutableReference<Integer>, Integer> identity = AggregateOperation
                .withCreate(MutableReference<Integer>::new)
                .andAccumulate(MutableReference<Integer>::set)
                .andExportFinish(MutableReference::get);

        // When
        StreamStage<Integer> rolling = srcStage.withTimestamps(i -> i, 100)
                                               .rollingAggregate(identity);

        // Then
        rolling.window(tumbling(1))
               .aggregate(identity)
               .drainTo(sink);
        executeAsync();
        Map<TimestampedItem<Integer>, Integer> expected = toBag(
                LongStream.range(0, itemCount)
                          .mapToObj(i -> new TimestampedItem<>(i + 1, (int) i))
                          .collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void merge() {
        // Given
        String src2Name = journaledMapName();
        StreamStage<Integer> srcStage2 = drawEventJournalValues(src2Name).withoutTimestamps();
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        putToMap(jet().getMap(src2Name), input);

        // When
        StreamStage<Integer> merged = srcStage.withoutTimestamps().merge(srcStage2);

        // Then
        merged.drainTo(sink);
        executeAsync();

        input.addAll(input);
        Map<Integer, Integer> expected = toBag(input);
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void hashJoin() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        String enrichingName = randomMapName();
        IMap<Integer, String> enriching = jet().getMap(enrichingName);
        input.forEach(i -> enriching.put(i, i + "A"));
        BatchStage<Entry<Integer, String>> enrichingStage = p.drawFrom(Sources.map(enrichingName));

        // When
        StreamStage<Tuple2<Integer, String>> hashJoined = srcStage.withoutTimestamps().hashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                Tuple2::tuple2
        );

        // Then
        hashJoined.drainTo(sink);
        executeAsync();

        Map<Tuple2<Integer, String>, Integer> expected = toBag(
                input.stream().map(i -> tuple2(i, i + "A")).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void hashJoin2() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        String enriching1Name = randomMapName();
        String enriching2Name = randomMapName();
        BatchStage<Entry<Integer, String>> enrichingStage1 = p.drawFrom(Sources.map(enriching1Name));
        BatchStage<Entry<Integer, String>> enrichingStage2 = p.drawFrom(Sources.map(enriching2Name));
        IMap<Integer, String> enriching1 = jet().getMap(enriching1Name);
        IMap<Integer, String> enriching2 = jet().getMap(enriching2Name);
        input.forEach(i -> enriching1.put(i, i + "A"));
        input.forEach(i -> enriching2.put(i, i + "B"));

        // When
        StreamStage<Tuple3<Integer, String, String>> hashJoined = srcStage.withoutTimestamps().hashJoin2(
                enrichingStage1, joinMapEntries(wholeItem()),
                enrichingStage2, joinMapEntries(wholeItem()),
                Tuple3::tuple3
        );

        // Then
        hashJoined.drainTo(sink);
        executeAsync();

        Map<Tuple3<Integer, String, String>, Integer> expected = toBag(input
                .stream().map(i -> tuple3(i, i + "A", i + "B")).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void hashJoinBuilder() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        String enriching1Name = randomMapName();
        String enriching2Name = randomMapName();
        BatchStage<Entry<Integer, String>> enrichingStage1 = p.drawFrom(Sources.map(enriching1Name));
        BatchStage<Entry<Integer, String>> enrichingStage2 = p.drawFrom(Sources.map(enriching2Name));
        IMap<Integer, String> enriching1 = jet().getMap(enriching1Name);
        IMap<Integer, String> enriching2 = jet().getMap(enriching2Name);
        input.forEach(i -> {
            enriching1.put(i, i + "A");
            enriching2.put(i, i + "B");
        });

        // When
        StreamHashJoinBuilder<Integer> b = srcStage.withoutTimestamps().hashJoinBuilder();
        Tag<String> tagA = b.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = b.add(enrichingStage2, joinMapEntries(wholeItem()));
        GeneralStage<Tuple2<Integer, ItemsByTag>> joined = b.build((t1, t2) -> tuple2(t1, t2));

        // Then
        joined.drainTo(sink);
        executeAsync();

        Map<Tuple2<Integer, ItemsByTag>, Integer> expected = toBag(input
                .stream()
                .map(i -> tuple2(i, itemsByTag(tagA, i + "A", tagB, i + "B")))
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void customTransform() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        DistributedFunction<Integer, String> mapFn = o -> o + "-x";

        // When
        StreamStage<String> custom = srcStage.withoutTimestamps().customTransform("map", Processors.mapP(mapFn));

        // Then
        custom.drainTo(sink);
        executeAsync();

        Map<String, Integer> expected = toBag(input
                .stream().map(mapFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }


    @Test
    public void customTransform_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        DistributedFunction<Integer, Integer> extractKeyFn = i -> i % 2;

        // When
        StreamStage<Object> custom = srcStage
                .withoutTimestamps()
                .groupingKey(extractKeyFn)
                .customTransform("map", Processors.mapUsingContextP(
                        ContextFactory.withCreateFn(jet -> new HashSet<>()),
                        (Set<Integer> ctx, Integer item) -> {
                            Integer key = extractKeyFn.apply(item);
                            return ctx.add(key) ? key : null;
                        }));

        // Then
        custom.drainTo(sink);
        executeAsync();
        // Each processor emitted distinct keys it observed. If groupingKey isn't correctly partitioning,
        // multiple processors will observe the same keys and the counts won't match.
        assertTrueEventually(() -> assertEquals(toBag(asList(0, 1)), sinkToBag()));
    }

    @Test
    public void peek_when_addedTimestamp_then_unwrapsJetEvent() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        // When
        StreamStage<Integer> peeked = srcStage.withIngestionTimestamps().peek();

        // Then
        peeked.drainTo(sink);
        jet().newJob(p);
        assertTrueEventually(() -> assertEquals(toBag(input), sinkToBag()), 10);
    }

    @Test
    public void peekWithToStringFunctionIsTransparent() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        DistributedPredicate<Integer> filterFn = i -> i % 2 == 1;

        // When
        srcStage
         .withoutTimestamps()
         .filter(filterFn)
         .peek(Object::toString)
         .drainTo(sink);

        // Then
        executeAsync();

        Map<Integer, Integer> expected = toBag(input.stream().filter(filterFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }
}
