/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.map.EventJournalMapEvent;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;

public class MapJournalLostEventTest extends AbstractJournalLostEventTest {

    private String sourceMap;

    @Before
    public void before() {
        sourceMap = journaledMapName();
    }

    @Test
    public void defaultProjection1_eventReceived() {
        performTest(
                member,
                Sources.mapJournalEntries(sourceMap, START_FROM_OLDEST),
                JournalSourceEntry::isAfterLostEvents,
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void defaultProjection2_eventReceived() {
        performTest(
                member,
                Sources.mapJournalEntries(hz().getMap(sourceMap), START_FROM_OLDEST),
                JournalSourceEntry::isAfterLostEvents,
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void customProjection_allEvents_eventReceived() {
        performTest(
                member,
                Sources.mapJournal(sourceMap, START_FROM_OLDEST, EventJournalMapEvent::isAfterLostEvents, PredicateEx.alwaysTrue()),
                FunctionEx.identity(),
                EventFilterType.ALL
        );
    }

    @Test
    public void customProjection_putEvents_eventReceived() {
        performTest(
                member,
                Sources.mapJournal(sourceMap, START_FROM_OLDEST, EventJournalMapEvent::isAfterLostEvents, mapPutEvents()),
                FunctionEx.identity(),
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void customProjection_removeEvents_eventReceived() {
        performTest(
                member,
                Sources.mapJournal(sourceMap, START_FROM_OLDEST, EventJournalMapEvent::isAfterLostEvents, e -> e.getType() == REMOVED),
                FunctionEx.identity(),
                EventFilterType.ONLY_REMOVE
        );
    }


    @Test
    public void customProjection2_eventReceived() {
        performTest(
                member,
                Sources.mapJournal(hz().getMap(sourceMap), START_FROM_OLDEST, EventJournalMapEvent::isAfterLostEvents, mapPutEvents()),
                FunctionEx.identity(),
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void remoteSourceDefaultProjection_receivedEvent() {
        performTest(
                remoteInstance,
                Sources.remoteMapJournalEntries(sourceMap, remoteHzClientConfig, START_FROM_OLDEST),
                JournalSourceEntry::isAfterLostEvents,
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void remoteSourceCustomProjection_receivedEvent() {
        performTest(
                remoteInstance,
                Sources.remoteMapJournal(sourceMap, remoteHzClientConfig, START_FROM_OLDEST, EventJournalMapEvent::isAfterLostEvents, mapPutEvents()),
                FunctionEx.identity(),
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void performRareEventTest() {
        var streamSource = Sources.mapJournal(
                sourceMap,
                START_FROM_OLDEST,
                EventJournalMapEvent::isAfterLostEvents,
                e -> e.getType() == REMOVED
        );
        performRareEventTest(streamSource);
    }

    @Override
    protected void put(HazelcastInstance hz, Integer key, Integer value) {
        hz.getMap(sourceMap).put(key, value);
    }

    @Override
    protected void remove(HazelcastInstance hz, Integer key) {
        hz.getMap(sourceMap).remove(key);
    }
}
