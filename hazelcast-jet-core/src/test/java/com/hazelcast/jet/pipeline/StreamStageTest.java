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
import org.junit.Test;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.pipeline.ContextFactories.replicatedMapContext;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class StreamStageTest extends PipelineTestSupport {

    @Test
    public void setName() {
        //Given
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        String stageName = randomName();

        //When
        StreamStage<Entry<Long, String>> streamStage = p
                .drawFrom(Sources.<Long, String>mapJournal(mapName, START_FROM_OLDEST))
                .setName(stageName);

        //Then
        assertEquals(stageName, streamStage.name());
    }

    @Test
    public void setLocalParallelism() {
        //Given
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        int localParallelism = 10;

        //When
        StreamStage<Entry<Long, String>> streamStage = p
                .drawFrom(Sources.<Long, String>mapJournal(mapName, START_FROM_OLDEST))
                .setLocalParallelism(localParallelism);

        //Then
        assertEquals(localParallelism, transformOf(streamStage).localParallelism());
    }

    @Test
    public void peekWithToStringFunctionIsTransparent() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<Long, String> map = jet().getMap(mapName);
        map.put(0L, "foo");
        map.put(1L, "bar");
        map.put(2L, "baz");

        // When
        p.drawFrom(Sources.<Long, String>mapJournal(mapName, START_FROM_OLDEST))
         .filter(e -> e.getValue().startsWith("f"))
         .map(entryValue())
         .peek(Object::toString)
         .drainTo(sink);
        jet().newJob(p);

        // Then
        List<String> expected = map.values().stream()
                                   .filter(e -> e.startsWith("f"))
                                   .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void map() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<Long, String> map = jet().getMap(mapName);
        map.put(0L, "foo");
        map.put(1L, "bar");
        map.put(2L, "baz");

        // When
        p.drawFrom(Sources.<Long, String>mapJournal(mapName, START_FROM_OLDEST))
         .map(e -> e.getValue() + "-x")
         .drainTo(sink);
        jet().newJob(p);

        // Then
        List<String> expected = map.values().stream()
                                   .map(e -> e + "-x")
                                   .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void mapUsingContext() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<Long, String> map = jet().getMap(mapName);
        map.put(0L, "foo");
        map.put(1L, "bar");
        map.put(2L, "baz");
        String transformMapName = randomMapName();
        ReplicatedMap<Long, String> transformMap = jet().getHazelcastInstance().getReplicatedMap(transformMapName);
        List<String> expected = map.keySet().stream()
                                   .peek(i -> transformMap.put(i, String.valueOf(i)))
                                   .map(String::valueOf)
                                   .collect(toList());

        // When
        p.drawFrom(Sources.<Long, String>mapJournal(mapName, START_FROM_OLDEST))
         .map(entryKey())
         .mapUsingContext(
                 ContextFactories.<Long, String>replicatedMapContext(transformMapName),
                 ReplicatedMap::get)
         .drainTo(sink);
        jet().newJob(p);

        // Then
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }


    @Test
    public void filter() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<Long, String> map = jet().getMap(mapName);
        map.put(0L, "foo");
        map.put(1L, "bar");
        map.put(2L, "baz");

        // When
        p.drawFrom(Sources.<Long, String>mapJournal(mapName, START_FROM_OLDEST))
         .filter(e -> e.getValue().startsWith("f"))
         .map(entryValue())
         .drainTo(sink);
        jet().newJob(p);

        // Then
        List<String> expected = map.values().stream()
                                   .filter(e -> e.startsWith("f"))
                                   .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void filterWithContext() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<Long, String> map = jet().getMap(mapName);
        map.put(0L, "foo");
        map.put(1L, "bar");
        map.put(2L, "baz");
        String filteringMapName = randomMapName();
        ReplicatedMap<Long, Long> filteringMap = jet().getHazelcastInstance().getReplicatedMap(filteringMapName);
        filteringMap.put(1L, 1L);
        filteringMap.put(2L, 2L);

        // When
        p.drawFrom(Sources.<Long, String>mapJournal(mapName, START_FROM_OLDEST))
         .map(entryKey())
         .filterUsingContext(
                 replicatedMapContext(filteringMapName),
                 ReplicatedMap::containsKey)
         .drainTo(sink);
        jet().newJob(p);

        // Then
        List<Long> expected = map.keySet().stream()
                                 .filter(filteringMap::containsKey)
                                 .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void flatMap() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<Long, String> map = jet().getMap(mapName);
        map.put(0L, "foo");

        // When
        p.drawFrom(Sources.<Long, String>mapJournal(mapName, START_FROM_OLDEST))
         .map(entryValue())
         .flatMap(o -> traverseIterable(asList(o + "A", o + "B")))
         .drainTo(sink);
        jet().newJob(p);

        // Then
        List<String> expected = map.values().stream()
                                   .flatMap(o -> Stream.of(o + "A", o + "B"))
                                   .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void flatMapUsingContext() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<Long, String> map = jet().getMap(mapName);
        map.put(0L, "foo");

        // When
        p.drawFrom(Sources.<Long, String>mapJournal(mapName, START_FROM_OLDEST))
         .map(entryValue())
         .flatMapUsingContext(
                 ContextFactory.withCreateFn(procCtx -> asList("A", "B")),
                 (ctx, o) -> traverseIterable(asList(o + ctx.get(0), o + ctx.get(1))))
         .drainTo(sink);
        jet().newJob(p);

        // Then
        List<String> expected = map.values().stream()
                                   .flatMap(o -> Stream.of(o + "A", o + "B"))
                                   .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }


    @Test
    public void hashJoinTwo() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<String, Integer> map = jet().getMap(mapName);
        putToMap(map, input);

        String enrichingName = randomMapName();
        IMap<Integer, String> enriching = jet().getMap(enrichingName);
        input.forEach(i -> enriching.put(i, i + "A"));
        BatchStage<Entry<Integer, String>> enrichingStage = p.drawFrom(Sources.map(enrichingName));

        // When
        p.drawFrom(Sources.<Integer, String, Integer>mapJournal(mapName, mapPutEvents(), mapEventNewValue(),
                START_FROM_OLDEST))
         .hashJoin(enrichingStage,
                 joinMapEntries(wholeItem()),
                 Tuple2::tuple2
         )
         .drainTo(sink);
        jet().newJob(p);

        // Then
        List<Tuple2<Integer, String>> expected = input.stream()
                                                      .map(i -> tuple2(i, i + "A"))
                                                      .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void hashJoinThree() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<String, Integer> map = jet().getMap(mapName);
        putToMap(map, input);

        String enriching1Name = randomMapName();
        String enriching2Name = randomMapName();
        BatchStage<Entry<Integer, String>> enrichingStage1 = p.drawFrom(Sources.map(enriching1Name));
        BatchStage<Entry<Integer, String>> enrichingStage2 = p.drawFrom(Sources.map(enriching2Name));
        IMap<Integer, String> enriching1 = jet().getMap(enriching1Name);
        IMap<Integer, String> enriching2 = jet().getMap(enriching2Name);
        input.forEach(i -> enriching1.put(i, i + "A"));
        input.forEach(i -> enriching2.put(i, i + "B"));

        // When
        p
                .drawFrom(Sources.<Integer, String, Integer>mapJournal(mapName, mapPutEvents(), mapEventNewValue(),
                        START_FROM_OLDEST))
                .hashJoin2(
                        enrichingStage1, joinMapEntries(wholeItem()),
                        enrichingStage2, joinMapEntries(wholeItem()),
                        Tuple3::tuple3
                ).drainTo(sink);
        jet().newJob(p);

        // Then
        List<Tuple3<Integer, String, String>> expected = input.stream()
                                                              .map(i -> tuple3(i, i + "A", i + "B"))
                                                              .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }


    @Test
    public void hashJoinBuilder() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<String, Integer> map = jet().getMap(mapName);
        putToMap(map, input);

        String enriching1Name = randomMapName();
        String enriching2Name = randomMapName();
        BatchStage<Entry<Integer, String>> enrichingStage1 = p.drawFrom(Sources.map(enriching1Name));
        BatchStage<Entry<Integer, String>> enrichingStage2 = p.drawFrom(Sources.map(enriching2Name));
        IMap<Integer, String> enriching1 = jet().getMap(enriching1Name);
        IMap<Integer, String> enriching2 = jet().getMap(enriching2Name);
        input.forEach(i -> enriching1.put(i, i + "A"));
        input.forEach(i -> enriching2.put(i, i + "B"));

        // When
        StreamHashJoinBuilder<Integer> b = p
                .drawFrom(Sources.<Integer, String, Integer>mapJournal(mapName, mapPutEvents(), mapEventNewValue(),
                        START_FROM_OLDEST))
                .hashJoinBuilder();

        Tag<String> tagA = b.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = b.add(enrichingStage2, joinMapEntries(wholeItem()));
        GeneralStage<Tuple2<Integer, ItemsByTag>> joined = b.build((t1, t2) -> tuple2(t1, t2));
        joined.drainTo(sink);
        jet().newJob(p);

        // Then
        List<Tuple2<Integer, ItemsByTag>> expected = input
                .stream()
                .map(i -> tuple2(i, itemsByTag(tagA, i + "A", tagB, i + "B")))
                .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void customTransform() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<String, Integer> map = jet().getMap(mapName);
        putToMap(map, input);

        // When
        StreamStage<String> custom = p
                .drawFrom(Sources.<Integer, String, Integer>mapJournal(mapName, mapPutEvents(), mapEventNewValue(),
                        START_FROM_OLDEST))
                .customTransform("map", Processors.<Integer, String>mapP(o -> Integer.toString(o)));
        custom.drainTo(sink);
        jet().newJob(p);

        // Then
        List<String> expected = input.stream()
                                     .map(String::valueOf)
                                     .collect(toList());
        assertTrueEventually(() -> assertEquals(toBag(expected), sinkToBag()));
    }

    @Test
    public void peek_when_addedTimestamp_then_unwrapsJetEvent() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        String mapName = JOURNALED_MAP_PREFIX + randomMapName();
        IMap<String, Integer> map = jet().getMap(mapName);
        putToMap(map, input);

        // When
        StreamStage<Integer> custom = p
                .drawFrom(Sources.<Integer, String, Integer>mapJournal(mapName, mapPutEvents(), mapEventNewValue(),
                        START_FROM_OLDEST))
                .addTimestamps()
                .peek((Integer i) -> true, (Integer i) -> String.valueOf(i));
        custom.drainTo(sink);
        jet().newJob(p);

        // Then
        assertTrueEventually(() -> assertEquals(toBag(input), sinkToBag()), 10);
    }
}
