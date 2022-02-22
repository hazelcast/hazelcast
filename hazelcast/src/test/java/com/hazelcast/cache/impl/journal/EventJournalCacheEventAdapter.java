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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.journal.EventJournalEventAdapter;

public class EventJournalCacheEventAdapter<K, V> implements EventJournalEventAdapter<K, V, EventJournalCacheEvent<K, V>> {

    @Override
    public K getKey(EventJournalCacheEvent<K, V> e) {
        return e.getKey();
    }

    @Override
    public V getNewValue(EventJournalCacheEvent<K, V> e) {
        return e.getNewValue();
    }

    @Override
    public V getOldValue(EventJournalCacheEvent<K, V> e) {
        return e.getOldValue();
    }

    @Override
    public EventType getType(EventJournalCacheEvent<K, V> e) {
        switch (e.getType()) {
            case CREATED:
                return EventType.ADDED;
            case REMOVED:
                return EventType.REMOVED;
            case UPDATED:
                return EventType.UPDATED;
            case EVICTED:
                return EventType.EVICTED;
            default:
                throw new IllegalArgumentException("Unknown event journal event type " + e.getType());
        }
    }
}
