/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.ConstructorFunction;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.map.impl.MapListenerAdaptors.getConstructors;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("ConstantConditions")
public class MapListenerAdapterTest {

    @Test
    public void ensure_map_listener_adapter_implements_listeners_for_all_entry_event_types_except_invalidation() {
        MapListenerAdapter mapListenerAdapterInstance = new MapListenerAdapter();
        Map<EntryEventType, ConstructorFunction<MapListener, ListenerAdapter>> constructors = getConstructors();
        for (Map.Entry<EntryEventType, ConstructorFunction<MapListener, ListenerAdapter>> entry : constructors.entrySet()) {
            EntryEventType entryEventType = entry.getKey();
            if (entryEventType == EntryEventType.INVALIDATION) {
                // this event is used to listen near-cache invalidations.
                continue;
            }

            assertNotNull("MapListenerAdapter misses an interface "
                            + "to implement for entry-event-type=" + entryEventType,
                    entry.getValue().createNew(mapListenerAdapterInstance));
        }
    }
}
