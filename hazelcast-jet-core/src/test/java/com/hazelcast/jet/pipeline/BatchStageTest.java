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
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.pipeline.ContextFactories.iMapContext;
import static com.hazelcast.jet.pipeline.ContextFactories.replicatedMapContext;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BatchStageTest extends PipelineTestSupport {

    private BatchStage<Integer> srcStage;

    @Before
    public void before() {
        srcStage = p.drawFrom(mapValuesSource(srcName));
    }

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
        srcStage.drainTo(sink);
        assertTrue(p.toDag().iterator().hasNext());
    }

    @Test
    public void setName() {
        //Given
        String stageName = randomName();

        //When
        srcStage.setName(stageName);

        //Then
        assertEquals(stageName, srcStage.name());
    }

    @Test
    public void setLocalParallelism() {
        //Given
        int localParallelism = 10;

        //When
        srcStage.setLocalParallelism(localParallelism);

        //Then
        assertEquals(localParallelism, transformOf(srcStage).localParallelism());
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchStage<String> mapped = srcStage.map(Object::toString);

        // Then
        mapped.drainTo(sink);
        execute();
        List<String> expected = input.stream().map(String::valueOf).collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void mapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> mapFn = i -> i + "-x";
        putToBatchSrcMap(input);
        String transformMapName = randomMapName();
        ReplicatedMap<Integer, String> transformMap = jet().getHazelcastInstance().getReplicatedMap(transformMapName);
        input.forEach(i -> transformMap.put(i, mapFn.apply(i)));

        // When
        BatchStage<String> mapped = srcStage.mapUsingContext(
                ContextFactories.<Integer, String>replicatedMapContext(transformMapName),
                ReplicatedMap::get);

        // Then
        mapped.drainTo(sink);
        execute();
        List<String> expected = input.stream().map(mapFn).collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void filter() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        DistributedPredicate<Integer> filterFn = i -> i % 2 == 1;

        // When
        BatchStage<Integer> filtered = srcStage.filter(filterFn);

        // Then
        filtered.drainTo(sink);
        execute();
        List<Integer> expected = input.stream()
                                      .filter(filterFn)
                                      .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void filterUsingReplicatedMapContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        String filteringMapName = randomMapName();
        ReplicatedMap<Integer, Integer> filteringMap = jet().getHazelcastInstance().getReplicatedMap(filteringMapName);
        filteringMap.put(1, 1);
        filteringMap.put(3, 3);

        // When
        BatchStage<Integer> mapped = srcStage.filterUsingContext(
                replicatedMapContext(filteringMapName),
                ReplicatedMap::containsKey);

        // Then
        mapped.drainTo(sink);
        execute();
        List<Integer> expected = input.stream()
                                      .filter(filteringMap::containsKey)
                                      .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void filterUsingIMapContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        String filteringMapName = randomMapName();
        Map<Integer, Integer> filteringMap = jet().getMap(filteringMapName);
        filteringMap.put(1, 1);
        filteringMap.put(3, 3);

        // When
        BatchStage<Integer> mapped = srcStage.filterUsingContext(
                iMapContext(filteringMapName),
                IMap::containsKey);

        // Then
        mapped.drainTo(sink);
        execute();
        List<Integer> expected = input.stream().filter(filteringMap::containsKey).collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void flatMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchStage<String> flatMapped = srcStage.flatMap(o -> traverseIterable(asList(o + "A", o + "B")));

        // Then
        flatMapped.drainTo(sink);
        execute();
        List<String> expected = input.stream().flatMap(o -> Stream.of(o + "A", o + "B")).collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void flatMapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchStage<String> flatMapped = srcStage.flatMapUsingContext(
                ContextFactory.withCreateFn(procCtx -> asList("A", "B")),
                (ctx, o) -> traverseIterable(asList(o + ctx.get(0), o + ctx.get(1))));

        // Then
        flatMapped.drainTo(sink);
        execute();
        List<String> expected = input.stream().flatMap(o -> Stream.of(o + "A", o + "B")).collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void merge() {
        // Given
        String src1Name = HazelcastTestSupport.randomName();
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name));
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);

        // When
        BatchStage<Integer> merged = srcStage.merge(srcStage1);

        // Then
        merged.drainTo(sink);
        execute();
        input.addAll(input);
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void distinct() {
        // Given
        List<Integer> input = IntStream.range(0, 2 * itemCount)
                                       .map(i -> i % itemCount)
                                       .boxed().collect(toList());
        Collections.shuffle(input);
        putToBatchSrcMap(input);

        // When
        BatchStage<Integer> distinct = srcStage.distinct();

        // Then
        distinct.drainTo(sink);
        execute();
        assertEquals(
                toBag(IntStream.range(0, itemCount).boxed().collect(toList())),
                sinkToBag());
    }

    @Test
    public void distinctByKey() {
        // Given
        DistributedFunction<Integer, Integer> keyFn = i -> i / 2;
        List<Integer> input = IntStream.range(0, 2 * itemCount).boxed().collect(toList());
        Collections.shuffle(input);
        putToBatchSrcMap(input);

        // When
        BatchStage<Integer> distinct = srcStage.groupingKey(keyFn).distinct();

        // Then
        distinct.drainTo(sink);
        execute();
        Map<Integer, Integer> sinkBag = sinkToBag();
        assertEquals(
                toBag(input.stream().map(keyFn).distinct().collect(toList())),
                toBag(sinkBag.keySet().stream().map(keyFn).collect(toList())));
    }

    @Test
    public void hashJoinTwo() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        String enrichingName = HazelcastTestSupport.randomName();
        IMap<Integer, String> enriching = jet().getMap(enrichingName);
        input.forEach(i -> enriching.put(i, i + "A"));
        BatchStage<Entry<Integer, String>> enrichingStage = p.drawFrom(Sources.map(enrichingName));

        // When
        BatchStage<Tuple2<Integer, String>> joined = srcStage.hashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                (t1, t2) -> tuple2(t1, t2));

        // Then
        joined.drainTo(sink);
        execute();
        List<Tuple2<Integer, String>> expected = input.stream()
                                                      .map(i -> tuple2(i, i + "A"))
                                                      .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void hashJoinThree() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        String enriching1Name = HazelcastTestSupport.randomName();
        String enriching2Name = HazelcastTestSupport.randomName();
        BatchStage<Entry<Integer, String>> enrichingStage1 = p.drawFrom(Sources.map(enriching1Name));
        BatchStage<Entry<Integer, String>> enrichingStage2 = p.drawFrom(Sources.map(enriching2Name));
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

        // Then
        joined.drainTo(sink);
        execute();
        List<Tuple3<Integer, String, String>> expected = input.stream()
                                                              .map(i -> tuple3(i, i + "A", i + "B"))
                                                              .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void hashJoinBuilder() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        String enriching1Name = HazelcastTestSupport.randomName();
        String enriching2Name = HazelcastTestSupport.randomName();
        BatchStage<Entry<Integer, String>> enrichingStage1 = p.drawFrom(Sources.map(enriching1Name));
        BatchStage<Entry<Integer, String>> enrichingStage2 = p.drawFrom(Sources.map(enriching2Name));
        IMap<Integer, String> enriching1 = jet().getMap(enriching1Name);
        IMap<Integer, String> enriching2 = jet().getMap(enriching2Name);
        input.forEach(i -> enriching1.put(i, i + "A"));
        input.forEach(i -> enriching2.put(i, i + "B"));

        // When
        HashJoinBuilder<Integer> b = srcStage.hashJoinBuilder();
        Tag<String> tagA = b.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = b.add(enrichingStage2, joinMapEntries(wholeItem()));
        GeneralStage<Tuple2<Integer, ItemsByTag>> joined = b.build((t1, t2) -> tuple2(t1, t2));

        // Then
        joined.drainTo(sink);
        execute();
        List<Tuple2<Integer, ItemsByTag>> expected = input
                .stream()
                .map(i -> tuple2(i, itemsByTag(tagA, i + "A", tagB, i + "B")))
                .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void peekIsTransparent() {
        // Given
        List<Integer> input = sequence(50);
        putToBatchSrcMap(input);

        // When
        BatchStage<Integer> peeked = srcStage.peek();

        // Then
        peeked.drainTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void peekWithToStringFunctionIsTransparent() {
        // Given
        List<Integer> input = sequence(50);
        putToBatchSrcMap(input);

        // When
        BatchStage<Integer> peeked = srcStage.peek(Object::toString);

        // Then
        peeked.drainTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void customTransform() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchStage<Object> custom = srcStage.customTransform("map", Processors.mapP(Object::toString));

        // Then
        custom.drainTo(sink);
        execute();
        List<String> expected = input.stream()
                                     .map(String::valueOf)
                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }
}
