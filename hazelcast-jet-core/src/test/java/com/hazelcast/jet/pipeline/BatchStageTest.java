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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BatchStageTest extends PipelineTestSupport {

    @Before
    public void before() {
        srcStage = pipeline.drawFrom(mapValuesSource(srcName));
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_emptyPipelineToDag_then_exceptionInIterator() {
        Pipeline.create().toDag().iterator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_missingSink_then_exceptionInDagIterator() {
        pipeline.toDag().iterator();
    }

    @Test
    public void when_minimalPipeline_then_validDag() {
        srcStage.drainTo(sink);
        assertTrue(pipeline.toDag().iterator().hasNext());
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        BatchStage<String> mapped = srcStage.map(Object::toString);
        mapped.drainTo(sink);
        execute();

        // Then
        List<String> expected = input.stream()
                                     .map(String::valueOf)
                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void filter() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        BatchStage<Integer> filtered = srcStage.filter(i -> i % 2 == 1);
        filtered.drainTo(sink);
        execute();

        // Then
        List<Integer> expected = input.stream()
                                      .filter(i -> i % 2 == 1)
                                      .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void flatMap() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        BatchStage<String> flatMapped = srcStage.flatMap(o -> traverseIterable(asList(o + "A", o + "B")));
        flatMapped.drainTo(sink);
        execute();

        // Then
        List<String> expected = input.stream()
                                     .flatMap(o -> Stream.of(o + "A", o + "B"))
                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void groupBy() {
        //Given
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToSrcMap(input);

        // When
        BatchStage<Entry<Integer, Long>> grouped = srcStage.groupingKey(wholeItem()).aggregate(counting());
        grouped.drainTo(sink);
        execute();

        // Then
        List<Entry<Integer, Long>> expected = IntStream.range(1, 100)
                                                       .mapToObj(i -> entry(i, (long) i))
                                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void hashJoinTwo() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
        String enrichingName = HazelcastTestSupport.randomName();
        IMap<Integer, String> enriching = jet().getMap(enrichingName);
        input.forEach(i -> enriching.put(i, i + "A"));
        BatchStage<Entry<Integer, String>> enrichingStage = pipeline.drawFrom(Sources.map(enrichingName));

        // When
        BatchStage<Tuple2<Integer, String>> joined = srcStage.hashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                (t1, t2) -> tuple2(t1, t2));
        joined.drainTo(sink);
        execute();

        // Then
        List<Tuple2<Integer, String>> expected = input.stream()
                                                      .map(i -> tuple2(i, i + "A"))
                                                      .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void hashJoinThree() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
        String enriching1Name = HazelcastTestSupport.randomName();
        String enriching2Name = HazelcastTestSupport.randomName();
        BatchStage<Entry<Integer, String>> enrichingStage1 = pipeline.drawFrom(Sources.map(enriching1Name));
        BatchStage<Entry<Integer, String>> enrichingStage2 = pipeline.drawFrom(Sources.map(enriching2Name));
        IMap<Integer, String> enriching1 = jet().getMap(enriching1Name);
        IMap<Integer, String> enriching2 = jet().getMap(enriching2Name);
        input.forEach(i -> enriching1.put(i, i + "A"));
        input.forEach(i -> enriching2.put(i, i + "B"));

        // When
        BatchStage<Tuple3<Integer, String, String>> joined = srcStage.hashJoin2(
                enrichingStage1, joinMapEntries(wholeItem()),
                enrichingStage2, joinMapEntries(wholeItem()),
                (t1, t2, t3) -> tuple3(t1, t2, t3)
        );
        joined.drainTo(sink);
        execute();

        // Then
        List<Tuple3<Integer, String, String>> expected = input.stream()
                                                              .map(i -> tuple3(i, i + "A", i + "B"))
                                                              .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void hashJoinBuilder() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
        String enriching1Name = HazelcastTestSupport.randomName();
        String enriching2Name = HazelcastTestSupport.randomName();
        BatchStage<Entry<Integer, String>> enrichingStage1 = pipeline.drawFrom(Sources.map(enriching1Name));
        BatchStage<Entry<Integer, String>> enrichingStage2 = pipeline.drawFrom(Sources.map(enriching2Name));
        IMap<Integer, String> enriching1 = jet().getMap(enriching1Name);
        IMap<Integer, String> enriching2 = jet().getMap(enriching2Name);
        input.forEach(i -> enriching1.put(i, i + "A"));
        input.forEach(i -> enriching2.put(i, i + "B"));

        // When
        HashJoinBuilder<Integer> b = srcStage.hashJoinBuilder();
        Tag<String> tagA = b.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = b.add(enrichingStage2, joinMapEntries(wholeItem()));
        GeneralStage<Tuple2<Integer, ItemsByTag>> joined = b.build((t1, t2) -> tuple2(t1, t2));
        joined.drainTo(sink);
        execute();

        // Then
        List<Tuple2<Integer, ItemsByTag>> expected = input
                .stream()
                .map(i -> tuple2(i, itemsByTag(tagA, i + "A", tagB, i + "B")))
                .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void coGroupTwo() {
        //Given
        String src1Name = HazelcastTestSupport.randomName();
        BatchStage<Integer> srcStage1 = pipeline.drawFrom(mapValuesSource(src1Name));
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToSrcMap(input);
        putToMap(jet().getMap(src1Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage1 = srcStage1.groupingKey(wholeItem());
        BatchStage<Entry<Integer, Long>> coGrouped = stage0.aggregate2(stage1,
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.addAllowingOverflow(1))
                        .andAccumulate1((count, item) -> count.addAllowingOverflow(10))
                        .andCombine(LongAccumulator::addAllowingOverflow)
                        .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);
        execute();

        // Then
        List<Entry<Integer, Long>> expected = IntStream.range(1, 100)
                                                       .mapToObj(i -> entry(i, 11L * i))
                                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void coGroupThree() {
        //Given
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToSrcMap(input);
        String src1Name = HazelcastTestSupport.randomName();
        String src2Name = HazelcastTestSupport.randomName();
        BatchStage<Integer> src1 = pipeline.drawFrom(mapValuesSource(src1Name));
        BatchStage<Integer> src2 = pipeline.drawFrom(mapValuesSource(src2Name));
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage1 = src1.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage2 = src2.groupingKey(wholeItem());
        BatchStage<Entry<Integer, Long>> coGrouped = stage0.aggregate3(stage1, stage2,
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.addAllowingOverflow(1))
                        .andAccumulate1((count, item) -> count.addAllowingOverflow(10))
                        .andAccumulate2((count, item) -> count.addAllowingOverflow(100))
                        .andCombine(LongAccumulator::addAllowingOverflow)
                        .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);
        execute();

        // Then
        List<Entry<Integer, Long>> expected = IntStream.range(1, 100)
                                                       .mapToObj(i -> entry(i, 111L * i))
                                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void coGroupBuilder() {
        //Given
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToSrcMap(input);
        String src1Name = HazelcastTestSupport.randomName();
        String src2Name = HazelcastTestSupport.randomName();
        BatchStage<Integer> src1 = pipeline.drawFrom(mapValuesSource(src1Name));
        BatchStage<Integer> src2 = pipeline.drawFrom(mapValuesSource(src2Name));
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage1 = src1.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage2 = src2.groupingKey(wholeItem());
        GroupAggregateBuilder<Integer, Integer> b = stage0.aggregateBuilder();
        Tag<Integer> tag0 = b.tag0();
        Tag<Integer> tag1 = b.add(stage1);
        Tag<Integer> tag2 = b.add(stage2);
        BatchStage<Entry<Integer, Long>> coGrouped = b.build(AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(tag0, (count, item) -> count.addAllowingOverflow(1))
                .andAccumulate(tag1, (count, item) -> count.addAllowingOverflow(10))
                .andAccumulate(tag2, (count, item) -> count.addAllowingOverflow(100))
                .andCombine(LongAccumulator::addAllowingOverflow)
                .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);
        execute();

        // Then
        List<Entry<Integer, Long>> expected = IntStream.range(1, 100)
                                                       .mapToObj(i -> entry(i, 111L * i))
                                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void peekIsTransparent() {
        // Given
        List<Integer> input = sequence(50);
        putToSrcMap(input);

        // When
        srcStage.peek().drainTo(sink);
        execute();

        // Then
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void customTransform() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        BatchStage<Object> custom = srcStage.customTransform("map", Processors.mapP(Object::toString));
        custom.drainTo(sink);
        execute();

        // Then
        List<String> expected = input.stream()
                                     .map(String::valueOf)
                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }
}
