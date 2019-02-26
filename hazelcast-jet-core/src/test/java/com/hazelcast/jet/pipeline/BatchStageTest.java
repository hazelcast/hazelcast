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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.TriFunction;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BatchStageTest extends PipelineTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void when_emptyPipelineToDag_then_exceptionInIterator() {
        Pipeline.create().toDag().iterator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_missingSink_then_exceptionInDagIterator() {
        p.toDag().iterator();
    }

    @Test
    public void when_minimalPipeline_then_validDag() {
        batchStageFromList(emptyList()).drainTo(sink);
        assertTrue(p.toDag().iterator().hasNext());
    }

    @Test
    public void setName() {
        // Given
        String stageName = randomName();
        BatchStage<Integer> stage = batchStageFromList(emptyList());

        // When
        stage.setName(stageName);

        // Then
        assertEquals(stageName, stage.name());
    }

    @Test
    public void setLocalParallelism() {
        // Given
        int localParallelism = 10;
        BatchStage<Integer> stage = batchStageFromList(emptyList());

        // When
        stage.setLocalParallelism(localParallelism);

        // Then
        assertEquals(localParallelism, transformOf(stage).localParallelism());
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        BatchStage<String> mapped = batchStageFromList(input).map(formatFn);

        // Then
        mapped.drainTo(sink);
        execute();
        assertEquals(streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void filter() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedPredicate<Integer> filterFn = i -> i % 2 == 1;

        // When
        BatchStage<Integer> filtered = batchStageFromList(input).filter(filterFn);

        // Then
        filtered.drainTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d-string", i);
        assertEquals(streamToString(input.stream().filter(filterFn), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void flatMap() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, String>> flatMapped = batchStageFromList(input)
                .flatMap(i -> traverseItems(entry(i, "A"), entry(i, "B")));

        // Then
        flatMapped.drainTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn = e -> String.format("%04d-%s", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().flatMap(i -> Stream.of(entry(i, "A"), entry(i, "B"))), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void mapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<String, Integer, String> formatFn = (s, i) -> String.format("%04d-%s", i, s);
        String suffix = "-context";

        // When
        BatchStage<String> mapped = batchStageFromList(input).mapUsingContext(
                ContextFactory.withCreateFn(i -> suffix),
                formatFn
        );

        // Then
        mapped.drainTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream(), i -> formatFn.apply(suffix, i)),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void mapUsingContext_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Integer, String, String> formatFn = (i, s) -> String.format("%04d-%s", i, s);
        String suffix = "-keyed-context";

        // When
        BatchStage<String> mapped = batchStageFromList(input)
                .groupingKey(i -> i)
                .mapUsingContext(
                        ContextFactory.withCreateFn(i -> suffix),
                        (context, k, i) -> formatFn.apply(i, context)
                );

        // Then
        mapped.drainTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream(), i -> formatFn.apply(i, suffix)),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void filterUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> mapped = batchStageFromList(input).filterUsingContext(
                ContextFactory.withCreateFn(i -> 1),
                (ctx, i) -> i % 2 == ctx);

        // Then
        mapped.drainTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d-string", i);
        assertEquals(
                streamToString(input.stream().filter(i -> i % 2 == 1), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void filterUsingContext_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> mapped = batchStageFromList(input)
                .groupingKey(i -> i)
                .filterUsingContext(
                        ContextFactory.withCreateFn(i -> 1),
                        (ctx, k, r) -> r % 2 == ctx);

        // Then
        mapped.drainTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d-string", i);
        assertEquals(
                streamToString(input.stream().filter(i -> i % 2 == 1), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void flatMapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, String>> flatMapped = batchStageFromList(input).flatMapUsingContext(
                ContextFactory.withCreateFn(procCtx -> asList("A", "B")),
                (ctx, i) -> traverseItems(entry(i, ctx.get(0)), entry(i, ctx.get(1))));

        // Then
        flatMapped.drainTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn = e -> String.format("%04d-%s", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().flatMap(i -> Stream.of(entry(i, "A"), entry(i, "B"))), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void flatMapUsingContext_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, String>> flatMapped = batchStageFromList(input)
                .groupingKey(i -> i)
                .flatMapUsingContext(
                        ContextFactory.withCreateFn(procCtx -> asList("A", "B")),
                        (ctx, k, i) -> traverseItems(entry(i, ctx.get(0)), entry(i, ctx.get(1))));

        // Then
        flatMapped.drainTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn = e -> String.format("%04d-%s", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().flatMap(i -> Stream.of(entry(i, "A"), entry(i, "B"))), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void mapUsingReplicatedMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        String replicatedMapName = randomMapName();
        ReplicatedMap<Integer, String> replicatedMap = member.getReplicatedMap(replicatedMapName);
        for (int i : input) {
            replicatedMap.put(i, prefix + i);
        }
        for (JetInstance jet : allJetInstances()) {
            assertSizeEventually(itemCount, jet.getReplicatedMap(replicatedMapName));
        }

        // When
        BatchStage<Entry<Integer, String>> stage = batchStageFromList(input)
                .mapUsingReplicatedMap(replicatedMap, DistributedFunction.identity(), Util::entry);

        // Then
        stage.drainTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> entry(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void mapUsingIMapAsync() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        IMap<Integer, String> map = member.getMap(randomMapName());
        for (int i : input) {
            map.put(i, prefix + i);
        }

        // When
        BatchStage<Entry<Integer, String>> stage = batchStageFromList(input)
                .mapUsingIMap(map, DistributedFunction.identity(), Util::entry);

        // Then
        stage.drainTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> entry(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void mapUsingIMapAsync_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        IMap<Integer, String> map = member.getMap(randomMapName());
        for (int i : input) {
            map.put(i, prefix + i);
        }

        // When
        BatchStage<Entry<Integer, String>> stage = batchStageFromList(input)
                .groupingKey(i -> i)
                .mapUsingIMap(map, Util::entry);

        // Then
        stage.drainTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> entry(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void rollingAggregate_global() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Long> mapped = batchStageFromList(input).rollingAggregate(counting());

        // Then
        mapped.drainTo(sink);
        execute();
        Function<Long, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(LongStream.range(1, itemCount + 1).boxed(), formatFn),
                streamToString(sinkStreamOf(Long.class), formatFn));
    }

    @Test
    public void rollingAggregate_keyed() {
        // Given
        itemCount = (int) roundUp(itemCount, 2);
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, Long>> mapped = batchStageFromList(input)
                .groupingKey(i -> i % 2)
                .rollingAggregate(counting());

        // Then
        mapped.drainTo(sink);
        execute();
        assertEquals(0, itemCount % 2);
        Stream<Entry<Integer, Long>> expectedStream =
                LongStream.range(1, itemCount / 2 + 1)
                          .boxed()
                          .flatMap(i -> Stream.of(entry(0, i), entry(1, i)));
        Function<Entry<Integer, Long>, String> formatFn =
                e -> String.format("(%04d, %04d)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(expectedStream, formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void merge() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> stage0 = batchStageFromList(input);
        BatchStage<Integer> stage1 = batchStageFromList(input);

        // When
        BatchStage<Integer> merged = stage0.merge(stage1);

        // Then
        merged.drainTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(input.stream().flatMap(i -> Stream.of(i, i)), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void distinct() {
        // Given
        List<Integer> input = IntStream.range(0, 2 * itemCount)
                                       .map(i -> i % itemCount)
                                       .boxed().collect(toList());
        Collections.shuffle(input);

        // When
        BatchStage<Integer> distinct = batchStageFromList(input).distinct();

        // Then
        distinct.drainTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(IntStream.range(0, itemCount).boxed(), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void distinct_keyed() {
        // Given
        DistributedFunction<Integer, Integer> keyFn = i -> i / 2;
        List<Integer> input = IntStream.range(0, 2 * itemCount).boxed().collect(toList());
        Collections.shuffle(input);

        // When
        BatchStage<Integer> distinct = batchStageFromList(input)
                .groupingKey(keyFn)
                .distinct();

        // Then
        distinct.drainTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(input.stream().map(keyFn).distinct(), formatFn),
                streamToString(sinkStreamOf(Integer.class).map(keyFn), formatFn));
    }

    @Test
    public void hashJoin() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        BatchStage<Entry<Integer, String>> enrichingStage =
                batchStageFromList(input).map(i -> entry(i, prefix + i));

        // When
        BatchStage<Entry<Integer, String>> joined = batchStageFromList(input).hashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                (i, enriching) -> entry(i, enriching));

        // Then
        joined.drainTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> tuple2(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void hashJoin2() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix1 = "A-";
        String prefix2 = "B-";
        BatchStage<Entry<Integer, String>> enrichingStage1 =
                batchStageFromList(input).map(i -> entry(i, prefix1 + i));
        BatchStage<Entry<Integer, String>> enrichingStage2 =
                batchStageFromList(input).map(i -> entry(i, prefix2 + i));

        // When
        BatchStage<Tuple3<Integer, String, String>> joined = batchStageFromList(input).hashJoin2(
                enrichingStage1, joinMapEntries(wholeItem()),
                enrichingStage2, joinMapEntries(wholeItem()),
                (t1, t2, t3) -> tuple3(t1, t2, t3)
        );

        // Then
        joined.drainTo(sink);
        execute();
        Function<Tuple3<Integer, String, String>, String> formatFn =
                t3 -> String.format("(%04d, %s, %s)", t3.f0(), t3.f1(), t3.f2());
        assertEquals(
                streamToString(input.stream().map(i -> tuple3(i, prefix1 + i, prefix2 + i)), formatFn),
                streamToString(sinkList.stream().map(o -> (Tuple3<Integer, String, String>) o), formatFn)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void hashJoinBuilder() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix1 = "A-";
        String prefix2 = "B-";
        BatchStage<Entry<Integer, String>> enrichingStage1 =
                batchStageFromList(input).map(i -> entry(i, prefix1 + i));
        BatchStage<Entry<Integer, String>> enrichingStage2 =
                batchStageFromList(input).map(i -> entry(i, prefix2 + i));

        // When
        HashJoinBuilder<Integer> b = batchStageFromList(input).hashJoinBuilder();
        Tag<String> tagA = b.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = b.add(enrichingStage2, joinMapEntries(wholeItem()));
        GeneralStage<Tuple2<Integer, ItemsByTag>> joined =
                b.build((t1, t2) -> tuple2(t1, t2));

        // Then
        joined.drainTo(sink);
        execute();
        TriFunction<Integer, String, String, String> formatFn =
                (i, v1, v2) -> String.format("(%04d, %s, %s)", i, v1, v2);
        assertEquals(
                streamToString(input.stream(),
                        i -> formatFn.apply(i, prefix1 + i, prefix2 + i)),
                streamToString(sinkList.stream().map(o -> (Tuple2<Integer, ItemsByTag>) o),
                        t2 -> formatFn.apply(t2.f0(), t2.f1().get(tagA), t2.f1().get(tagB)))
        );
    }

    @Test
    public void peekIsTransparent() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> peeked = batchStageFromList(input).peek();

        // Then
        peeked.drainTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void peekWithToStringFunctionIsTransparent() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> peeked = batchStageFromList(input).peek(Object::toString);

        // Then
        peeked.drainTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void customTransform() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> formatFn = i -> String.format("%04d", i);

        // When
        BatchStage<String> custom = batchStageFromList(input)
                .customTransform("map", Processors.mapP(formatFn));

        // Then
        custom.drainTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void customTransform_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> extractKeyFn = i -> i % 2;

        // When
        BatchStage<Object> custom = batchStageFromList(input)
                .groupingKey(extractKeyFn)
                .customTransform("map", Processors.mapUsingContextP(
                        ContextFactory.withCreateFn(jet -> new HashSet<>()),
                        (Set<Integer> ctx, Integer item) -> {
                            Integer key = extractKeyFn.apply(item);
                            return ctx.add(key) ? key : null;
                        }));

        // Then
        custom.drainTo(sink);
        execute();
        // Each processor emitted distinct keys it observed. If groupingKey isn't correctly partitioning,
        // multiple processors will observe the same keys and the counts won't match.
        assertEquals("0\n1", streamToString(sinkStreamOf(Integer.class), Object::toString));
    }
}
