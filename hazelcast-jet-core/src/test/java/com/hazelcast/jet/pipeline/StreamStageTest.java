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
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.pipeline.ContextFactories.replicatedMapContext;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class StreamStageTest extends PipelineStreamTestSupport {

    @Test
    public void setName() {
        //Given
        String stageName = randomName();

        //When
        mapJournalSrcStage.setName(stageName);

        //Then
        assertEquals(stageName, mapJournalSrcStage.name());
    }

    @Test
    public void setLocalParallelism() {
        //Given
        int localParallelism = 10;

        //When
        mapJournalSrcStage.setLocalParallelism(localParallelism);

        //Then
        assertEquals(localParallelism, transformOf(mapJournalSrcStage).localParallelism());
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        DistributedFunction<Integer, String> mapFn = item -> item + "-x";

        // When
        StreamStage<String> mapped = mapJournalSrcStage.map(mapFn);

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
        Map<String, Integer> expected = toBag(input.stream().map(mapFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void mapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        Function<Integer, String> mapFn = i -> i + "-x";
        String enrichingMapName = randomMapName();
        ReplicatedMap<Integer, String> enrichingMap = jet().getHazelcastInstance().getReplicatedMap(enrichingMapName);
        input.forEach(i -> enrichingMap.put(i, mapFn.apply(i)));

        // When
        StreamStage<String> mapped = mapJournalSrcStage
                .mapUsingContext(
                        ContextFactories.<Long, String>replicatedMapContext(enrichingMapName),
                        ReplicatedMap::get);

        // Then
        mapped.drainTo(sink);
        jet().newJob(p);
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
        StreamStage<Integer> filtered = mapJournalSrcStage.filter(filterFn);

        // Then
        filtered.drainTo(sink);
        jet().newJob(p);
        Map<Integer, Integer> expected = toBag(input.stream().filter(filterFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void filterUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        Predicate<Integer> filterFn = i -> i % 2 == 1;

        String filteringMapName = randomMapName();
        ReplicatedMap<Integer, Integer> filteringMap = jet().getHazelcastInstance().getReplicatedMap(filteringMapName);
        input.stream().filter(filterFn).forEach(item -> filteringMap.put(item, item));

        // When
        StreamStage<Integer> filtered = mapJournalSrcStage.filterUsingContext(
                replicatedMapContext(filteringMapName),
                ReplicatedMap::containsKey);

        // Then
        filtered.drainTo(sink);
        jet().newJob(p);
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
        StreamStage<String> flatMapped = mapJournalSrcStage.flatMap(o -> traverseStream(flatMapFn.apply(o)));

        // Then
        flatMapped.drainTo(sink);
        jet().newJob(p);
        Map<String, Integer> expected = toBag(input.stream().flatMap(flatMapFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void flatMapUsingContext() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        DistributedFunction<Integer, Stream<String>> flatMapFn = o -> Stream.of(o + "A", o + "B");

        // When
        StreamStage<String> flatMapped = mapJournalSrcStage.flatMapUsingContext(
                ContextFactory.withCreateFn(procCtx -> flatMapFn),
                (ctx, o) -> traverseStream(ctx.apply(o)));

        // Then
        flatMapped.drainTo(sink);
        jet().newJob(p);
        Map<String, Integer> expected = toBag(input.stream().flatMap(flatMapFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void merge() {
        // Given
        String src2Name = journaledMapName();
        StreamStage<Integer> srcStage2 = drawEventJournalValues(src2Name);
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);
        putToMap(jet().getMap(src2Name), input);

        // When
        StreamStage<Integer> merged = mapJournalSrcStage.merge(srcStage2);

        // Then
        merged.drainTo(sink);
        jet().newJob(p);
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
        StreamStage<Tuple2<Integer, String>> hashJoined = mapJournalSrcStage.hashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                Tuple2::tuple2
        );

        // Then
        hashJoined.drainTo(sink);
        jet().newJob(p);
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
        StreamStage<Tuple3<Integer, String, String>> hashJoined = mapJournalSrcStage.hashJoin2(
                enrichingStage1, joinMapEntries(wholeItem()),
                enrichingStage2, joinMapEntries(wholeItem()),
                Tuple3::tuple3
        );

        // Then
        hashJoined.drainTo(sink);
        jet().newJob(p);
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
        StreamHashJoinBuilder<Integer> b = mapJournalSrcStage.hashJoinBuilder();
        Tag<String> tagA = b.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = b.add(enrichingStage2, joinMapEntries(wholeItem()));
        GeneralStage<Tuple2<Integer, ItemsByTag>> joined = b.build((t1, t2) -> tuple2(t1, t2));

        // Then
        joined.drainTo(sink);
        jet().newJob(p);
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
        DistributedFunction<Integer, String> mapFn = o -> Integer.toString(o) + "-x";

        // When
        StreamStage<String> custom = mapJournalSrcStage.customTransform("map", Processors.mapP(mapFn));

        // Then
        custom.drainTo(sink);
        jet().newJob(p);
        Map<String, Integer> expected = toBag(input
                .stream().map(mapFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    public void peek_when_addedTimestamp_then_unwrapsJetEvent() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcMapJournal(input);

        // When
        StreamStage<Integer> peeked = mapJournalSrcStage.addTimestamps().peek();

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
        mapJournalSrcStage
         .filter(filterFn)
         .peek(Object::toString)
         .drainTo(sink);
        jet().newJob(p);

        // Then
        Map<Integer, Integer> expected = toBag(input.stream().filter(filterFn).collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }
}
