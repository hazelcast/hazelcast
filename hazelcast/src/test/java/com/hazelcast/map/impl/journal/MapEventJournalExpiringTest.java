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
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.MapStore;
import com.hazelcast.journal.AbstractEventJournalExpiringTest;
import com.hazelcast.journal.EventJournalTestContext;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.MapUtil;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelTest.class})
public class MapEventJournalExpiringTest<K, V> extends AbstractEventJournalExpiringTest<EventJournalMapEvent> {

    private static final String MAP_NAME = "mappy";

    @Override
    protected Config getConfig() {
        final MapConfig nonExpiringMapConfig = new MapConfig(MAP_NAME)
                .setInMemoryFormat(getInMemoryFormat());

        return super.getConfig()
                    .addMapConfig(nonExpiringMapConfig);
    }

    protected InMemoryFormat getInMemoryFormat() {
        return MapConfig.DEFAULT_IN_MEMORY_FORMAT;
    }

    @Override
    protected EventJournalTestContext<K, V, EventJournalMapEvent<K, V>> createContext() {
        return new EventJournalTestContext<K, V, EventJournalMapEvent<K, V>>(
                new EventJournalMapDataStructureAdapter<K, V>(getRandomInstance().<K, V>getMap(MAP_NAME)),
                null,
                new EventJournalMapEventAdapter<K, V>()
        );
    }
}
