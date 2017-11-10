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

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.map.journal.EventJournalMapEvent;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

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
        pipeline.drawFrom(Sources.mapJournal(mapName, false))
                .map(EventJournalMapEvent::getNewValue)
                .drainTo(sink);
        jet().newJob(pipeline);

        // Then eventually we get all the map values in the sink.
        assertSizeEventually(ITEM_COUNT, sinkList);

        // When we update all the map items...
        key[0] = 0;
        input.forEach(i -> srcMap.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(2 * ITEM_COUNT, sinkList);

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
        pipeline.drawFrom(Sources.mapJournal(mapName, p, EventJournalMapEvent::getNewValue, false))
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

    private abstract static class JournalEvent {

        final String key;
        final Integer oldValue;
        final Integer newValue;

        JournalEvent(String key, Integer oldValue, Integer newValue) {
            this.key = key;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        Integer newValue() {
            return newValue;
        }
    }

    private static class MapJournalEvent extends JournalEvent {
        final EntryEventType type;

        MapJournalEvent(String key, Integer oldValue, Integer newValue, EntryEventType type) {
            super(key, oldValue, newValue);
            this.type = type;
        }
    }

    private static class CacheJournalEvent extends JournalEvent {
        final CacheEventType type;

        CacheJournalEvent(String key, Integer oldValue, Integer newValue, CacheEventType type) {
            super(key, oldValue, newValue);
            this.type = type;
        }
    }
}
