/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedFunction;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.JoinClause.joinMapEntries;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.query.TruePredicate.truePredicate;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ComputeStageTest extends TestInClusterSupport {

    private static final char ITEM_COUNT = 1000;
    private Pipeline pipeline;
    private ComputeStage<Integer> srcStage;
    private Sink<Object> sink;

    private IMap<String, Integer> srcMap;
    private IList<Object> sinkList;

    @Before
    public void before() {
        pipeline = Pipeline.create();

        String srcName = randomName();
        srcStage = pipeline.drawFrom(readMapValues(srcName));
        srcMap = jet().getMap(srcName);

        String sinkName = randomName();
        sink = Sinks.writeList(sinkName);
        sinkList = jet().getList(sinkName);
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
        srcStage.drainTo(Sinks.writeList("out"));
        assertTrue(pipeline.toDag().iterator().hasNext());
    }

    @Test
    public void map() {
        // Given
        ComputeStage<String> mapped = srcStage.map(Object::toString);
        mapped.drainTo(sink);

        // When
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
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
        ComputeStage<Integer> filtered = srcStage.filter(i -> i % 2 == 1);
        filtered.drainTo(sink);

        // When
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
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
        ComputeStage<String> flatMapped = srcStage.flatMap(o -> traverseIterable(asList(o + "A", o + "B")));
        flatMapped.drainTo(sink);

        // When
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
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
        ComputeStage<Entry<Integer, Long>> grouped = srcStage.groupBy(wholeItem(), counting());
        grouped.drainTo(sink);

        // When
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToSrcMap(input);
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
        String enrichingName = randomName();
        ComputeStage<Entry<Integer, String>> enrichingStage = pipeline.drawFrom(Sources.readMap(enrichingName));

        ComputeStage<Tuple2<Integer, String>> joined = srcStage.hashJoin(enrichingStage, joinMapEntries(wholeItem()));
        joined.drainTo(sink);

        // When
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
        IMap<Integer, String> enriching = jet().getMap(enrichingName);
        input.forEach(i -> enriching.put(i, i + "A"));
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
        String enriching1Name = randomName();
        String enriching2Name = randomName();
        ComputeStage<Entry<Integer, String>> enrichingStage1 = pipeline.drawFrom(Sources.readMap(enriching1Name));
        ComputeStage<Entry<Integer, String>> enrichingStage2 = pipeline.drawFrom(Sources.readMap(enriching2Name));
        ComputeStage<Tuple3<Integer, String, String>> joined = srcStage.hashJoin(
                enrichingStage1, joinMapEntries(wholeItem()),
                enrichingStage2, joinMapEntries(wholeItem())
        );
        joined.drainTo(sink);

        // When
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
        IMap<Integer, String> enriching1 = jet().getMap(enriching1Name);
        IMap<Integer, String> enriching2 = jet().getMap(enriching2Name);
        input.forEach(i -> enriching1.put(i, i + "A"));
        input.forEach(i -> enriching2.put(i, i + "B"));
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
        String enriching1Name = randomName();
        String enriching2Name = randomName();
        ComputeStage<Entry<Integer, String>> enrichingStage1 = pipeline.drawFrom(Sources.readMap(enriching1Name));
        ComputeStage<Entry<Integer, String>> enrichingStage2 = pipeline.drawFrom(Sources.readMap(enriching2Name));
        HashJoinBuilder<Integer> b = srcStage.hashJoinBuilder();
        Tag<String> tagA = b.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = b.add(enrichingStage2, joinMapEntries(wholeItem()));
        ComputeStage<Tuple2<Integer, ItemsByTag>> joined = b.build();
        joined.drainTo(sink);

        // When
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
        IMap<Integer, String> enriching1 = jet().getMap(enriching1Name);
        IMap<Integer, String> enriching2 = jet().getMap(enriching2Name);
        input.forEach(i -> enriching1.put(i, i + "A"));
        input.forEach(i -> enriching2.put(i, i + "B"));
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
        String src1Name = randomName();
        ComputeStage<Integer> src1 = pipeline.drawFrom(readMapValues(src1Name));

        ComputeStage<Entry<Integer, Long>> coGrouped = srcStage.coGroup(wholeItem(), src1, wholeItem(),
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.add(1))
                        .andAccumulate1((count, item) -> count.add(10))
                        .andCombine(LongAccumulator::add)
                        .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);

        // When
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
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
        String src1Name = randomName();
        String src2Name = randomName();
        ComputeStage<Integer> src1 = pipeline.drawFrom(readMapValues(src1Name));
        ComputeStage<Integer> src2 = pipeline.drawFrom(readMapValues(src2Name));

        ComputeStage<Entry<Integer, Long>> coGrouped = srcStage.coGroup(wholeItem(),
                src1, wholeItem(),
                src2, wholeItem(),
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.add(1))
                        .andAccumulate1((count, item) -> count.add(10))
                        .andAccumulate2((count, item) -> count.add(100))
                        .andCombine(LongAccumulator::add)
                        .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);

        // When
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
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
        String src1Name = randomName();
        String src2Name = randomName();
        ComputeStage<Integer> src1 = pipeline.drawFrom(readMapValues(src1Name));
        ComputeStage<Integer> src2 = pipeline.drawFrom(readMapValues(src2Name));
        CoGroupBuilder<Integer, Integer> b = srcStage.coGroupBuilder(wholeItem());
        Tag<Integer> tag0 = b.tag0();
        Tag<Integer> tag1 = b.add(src1, wholeItem());
        Tag<Integer> tag2 = b.add(src2, wholeItem());
        ComputeStage<Tuple2<Integer, Long>> coGrouped = b.build(AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(tag0, (count, item) -> count.add(1))
                .andAccumulate(tag1, (count, item) -> count.add(10))
                .andAccumulate(tag2, (count, item) -> count.add(100))
                .andCombine(LongAccumulator::add)
                .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);

        // When
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        execute();

        // Then
        List<Entry<Integer, Long>> expected = IntStream.range(1, 100)
                                                       .mapToObj(i -> entry(i, 111L * i))
                                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void customTransform() {
        // Given
        ComputeStage<Object> custom = srcStage.customTransform("map", Processors.mapP(Object::toString));
        custom.drainTo(sink);

        // When
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);
        execute();

        // Then
        List<String> expected = input.stream()
                                     .map(String::valueOf)
                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    private void putToSrcMap(List<Integer> data) {
        putToMap(srcMap, data);
    }

    private static void putToMap(Map<String, Integer> dest, List<Integer> data) {
        int[] key = {0};
        data.forEach(i -> dest.put(String.valueOf(key[0]++), i));
    }

    private JetInstance jet() {
        return testMode.getJet();
    }

    private void execute() {
        jet().newJob(pipeline).join();
    }

    private Map<Object, Integer> sinkToBag() {
        return toBag(this.sinkList);
    }

    private static Source<Integer> readMapValues(String srcName) {
        return Sources.readMap(srcName, truePredicate(),
                (DistributedFunction<Entry<Integer, Integer>, Integer>) Entry::getValue);
    }

    private static <T> Map<T, Integer> toBag(Collection<T> coll) {
        Map<T, Integer> bag = new HashMap<>();
        for (T t : coll) {
            bag.merge(t, 1, (count, x) -> count + 1);
        }
        return bag;
    }

    private static List<Integer> sequence(int itemCount) {
        return IntStream.range(0, itemCount).boxed().collect(toList());
    }
}
