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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.map.journal.EventJournalMapEvent;
import org.junit.Test;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class Sources_withEventJournalTest extends PipelineTestSupport {

    @Test
    public void mapJournal() {
        // Given a pre-populated source map...
        List<Integer> input = sequence(ITEM_COUNT);
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<String, Integer> srcMap = jet().getMap(mapName);
        int[] key = {0};
        input.forEach(i -> srcMap.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        pipeline.drawFrom(Sources.<String, Integer>mapJournal(mapName, START_FROM_OLDEST,
                wmGenParams(Entry::getValue, withFixedLag(0), suppressDuplicates(), 10_000)))
                .map(entryValue())
                .drainTo(sink);
        jet().newJob(pipeline);

        // Then eventually we get all the map values in the sink.
        assertSizeEventually(ITEM_COUNT, sinkList);

        // When we update all the map items...
        key[0] = 0;
        input.forEach(i -> srcMap.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(2 * ITEM_COUNT, sinkList);

        // When we delete all map items...
        input.forEach(i -> srcMap.remove(String.valueOf(key[0]++)));

        // Then we won't get any more events in the sink.
        assertTrueAllTheTime(() -> assertEquals(2 * ITEM_COUNT, sinkList.size()), 2);

        // The values we got are exactly all the original values
        // and all the updated values.
        List<Integer> expected = Stream.concat(input.stream().map(i -> Integer.MIN_VALUE + i), input.stream())
                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void mapJournal_withPredicateAndProjection() {
        // Given a pre-populated source map...
        List<Integer> input = sequence(ITEM_COUNT);
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<String, Integer> srcMap = jet().getMap(mapName);
        int[] key = {0};
        input.forEach(i -> srcMap.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        DistributedPredicate<EventJournalMapEvent<String, Integer>> p = e -> e.getNewValue() % 2 == 0;
        pipeline.drawFrom(Sources.mapJournal(mapName, p, EventJournalMapEvent::getNewValue, START_FROM_OLDEST,
                wmGenParams(v -> v, withFixedLag(0), suppressDuplicates(), 10_000)))
                .drainTo(sink);
        jet().newJob(pipeline);

        // Then eventually we get all the map values in the sink.
        assertSizeEventually(ITEM_COUNT / 2, sinkList);

        // When we update all the map items...
        key[0] = 0;
        input.forEach(i -> srcMap.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(ITEM_COUNT, sinkList);

        // The values we got are exactly all the original values
        // and all the updated values.
        List<Integer> expected = Stream
                .concat(input.stream().map(i -> Integer.MIN_VALUE + i),
                        input.stream())
                .filter(i -> i % 2 == 0)
                .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }
}
