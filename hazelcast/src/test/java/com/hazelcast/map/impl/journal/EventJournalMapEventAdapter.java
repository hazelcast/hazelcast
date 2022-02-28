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

package com.hazelcast.map.impl.journal;

import com.hazelcast.journal.EventJournalEventAdapter;
import com.hazelcast.map.EventJournalMapEvent;

public class EventJournalMapEventAdapter<K, V> implements EventJournalEventAdapter<K, V, EventJournalMapEvent<K, V>> {
    @Override
    public K getKey(EventJournalMapEvent<K, V> e) {
        return e.getKey();
    }

    @Override
    public V getNewValue(EventJournalMapEvent<K, V> e) {
        return e.getNewValue();
    }

    @Override
    public V getOldValue(EventJournalMapEvent<K, V> e) {
        return e.getOldValue();
    }

    @Override
    public EventType getType(EventJournalMapEvent<K, V> e) {
        switch (e.getType()) {
            case ADDED:
                return EventType.ADDED;
            case REMOVED:
                return EventType.REMOVED;
            case UPDATED:
                return EventType.UPDATED;
            case EVICTED:
                return EventType.EVICTED;
            case LOADED:
                return EventType.LOADED;
            default:
                throw new IllegalArgumentException("Unknown event journal event type " + e.getType());
        }
    }
}
