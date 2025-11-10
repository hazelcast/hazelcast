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

package com.hazelcast.map.impl.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.journal.AbstractEventJournalExpiringTest;
import com.hazelcast.journal.EventJournalTestContext;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class MapEventJournalExpiringTest<K, V> extends AbstractEventJournalExpiringTest<EventJournalMapEvent> {

    private static final String MAP_NAME = "mappy";

    @Override
    protected Config getConfig() {
        Config defConfig = super.getConfig();
        defConfig.getMapConfig(MAP_NAME)
                .setBackupCount(1)
                .setInMemoryFormat(getInMemoryFormat())
                .setEventJournalConfig(getEventJournalConfig());
        return defConfig;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return MapConfig.DEFAULT_IN_MEMORY_FORMAT;
    }

    @Override
    protected EventJournalTestContext<K, V, EventJournalMapEvent<K, V>> createContext() {
        return new EventJournalTestContext<>(
                new EventJournalMapDataStructureAdapter<>(getRandomInstance().getMap(MAP_NAME)),
                null,
                new EventJournalMapEventAdapter<>()
        );
    }

    protected String getName() {
        return MAP_NAME;
    }

    protected  RingbufferContainer<?, ?> getRingBufferContainer(String name, int partitionId, HazelcastInstance instance) {
        var serviceName = RingbufferService.SERVICE_NAME;
        final RingbufferService service = getNodeEngine(instance).getService(serviceName);
        final ObjectNamespace ns = MapService.getObjectNamespace(name);

        RingbufferContainer<?, ?> ringbuffer = service.getContainerOrNull(partitionId, ns);
        if (ringbuffer == null) {
            throw new IllegalStateException("No ringbuffer container for partition " + partitionId);
        }
        return ringbuffer;
    }
}
