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

package com.hazelcast.map.impl.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.journal.AbstractEventJournalBasicTest;
import com.hazelcast.journal.EventJournalTestContext;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapEventJournalBasicTest<K, V> extends AbstractEventJournalBasicTest<EventJournalMapEvent> {

    private static final String NON_EXPIRING_MAP = "mappy";
    private static final String EXPIRING_MAP = "expiring";

    @Override
    protected Config getConfig() {
        final MapConfig nonExpiringMapConfig = new MapConfig(NON_EXPIRING_MAP)
                .setInMemoryFormat(getInMemoryFormat());

        final MapConfig expiringMapConfig = new MapConfig(EXPIRING_MAP).setTimeToLiveSeconds(1)
                                                                       .setInMemoryFormat(getInMemoryFormat());
        return super.getConfig()
                    .addMapConfig(nonExpiringMapConfig)
                    .addMapConfig(expiringMapConfig);
    }

    protected InMemoryFormat getInMemoryFormat() {
        return MapConfig.DEFAULT_IN_MEMORY_FORMAT;
    }

    @Override
    protected EventJournalTestContext<K, V, EventJournalMapEvent<K, V>> createContext() {
        return new EventJournalTestContext<K, V, EventJournalMapEvent<K, V>>(
                new EventJournalMapDataStructureAdapter<K, V>(getRandomInstance().<K, V>getMap(NON_EXPIRING_MAP)),
                new EventJournalMapDataStructureAdapter<K, V>(getRandomInstance().<K, V>getMap(EXPIRING_MAP)),
                new EventJournalMapEventAdapter<K, V>()
        );
    }
}
