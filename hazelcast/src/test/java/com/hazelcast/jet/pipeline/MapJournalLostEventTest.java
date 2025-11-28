/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.EventJournalMapEvent;
import org.junit.Before;
import org.junit.Test;

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
                Sources.mapJournalEntries(sourceMap, START_FROM_OLDEST),
                JournalSourceEntry::isAfterLostEvents
        );
    }

    @Test
    public void defaultProjection2_eventReceived() {
        performTest(
                Sources.mapJournalEntries(hz().getMap(sourceMap), START_FROM_OLDEST),
                JournalSourceEntry::isAfterLostEvents
        );
    }

    @Test
    public void customProjection1_eventReceived() {
        performTest(
                Sources.mapJournal(sourceMap, START_FROM_OLDEST, EventJournalMapEvent::isAfterLostEvents, mapPutEvents()),
                FunctionEx.identity()
        );
    }

    @Test
    public void customProjection2_eventReceived() {
        performTest(
                Sources.mapJournal(hz().getMap(sourceMap), START_FROM_OLDEST, EventJournalMapEvent::isAfterLostEvents, mapPutEvents()),
                FunctionEx.identity()
        );
    }

    @Test
    public void remoteSourceDefaultProjection_receivedEvent() {
        performTest(
                remoteInstance,
                Sources.remoteMapJournalEntries(sourceMap, remoteHzClientConfig, START_FROM_OLDEST),
                JournalSourceEntry::isAfterLostEvents
        );
    }

    @Test
    public void remoteSourceCustomProjection_receivedEvent() {
        performTest(
                remoteInstance,
                Sources.remoteMapJournal(sourceMap, remoteHzClientConfig, START_FROM_OLDEST, EventJournalMapEvent::isAfterLostEvents, mapPutEvents()),
                FunctionEx.identity()
        );
    }

    protected void put(HazelcastInstance hz, Integer key, Integer value) {
        hz.getMap(sourceMap).put(key, value);
    }
}
