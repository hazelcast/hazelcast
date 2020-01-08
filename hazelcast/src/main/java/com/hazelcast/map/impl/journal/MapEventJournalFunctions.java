/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.util.function.Function;
import com.hazelcast.util.function.Predicate;

import java.io.Serializable;
import java.util.Map.Entry;

/**
 * Utility functions used from Jet. They are here so that they can be also used
 * when Jet accesses remote imdg (non-jet) cluster.
 */
public final class MapEventJournalFunctions {

    private MapEventJournalFunctions() { }

    public static <K, V> Predicate<EventJournalMapEvent<K, V>> mapPutEvents() {
        return new MapPutEventsPredicate<K, V>();
    }

    public static <K, V> Function<EventJournalMapEvent<K, V>, Entry<K, V>> mapEventToEntry() {
        return new MapEventToEntryProjection<K, V>();
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Function<EventJournalMapEvent<K, V>, V> mapEventNewValue() {
        return new MapEventNewValueProjection();
    }

    @SerializableByConvention
    private static class MapPutEventsPredicate<K, V> implements Predicate<EventJournalMapEvent<K, V>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean test(EventJournalMapEvent<K, V> e) {
            return e.getType() == EntryEventType.ADDED || e.getType() == EntryEventType.UPDATED;
        }
    }

    @SerializableByConvention
    private static class MapEventToEntryProjection<K, V>
            implements Function<EventJournalMapEvent<K, V>, Entry<K, V>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Entry<K, V> apply(EventJournalMapEvent<K, V> e) {
            DeserializingEventJournalMapEvent<K, V> casted = (DeserializingEventJournalMapEvent<K, V>) e;
            return new DeserializingEntry<K, V>(casted.getDataKey(), casted.getDataNewValue());
        }
    }

    @SerializableByConvention
    private static class MapEventNewValueProjection implements Function, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Object apply(Object event) {
            DeserializingEventJournalMapEvent casted = (DeserializingEventJournalMapEvent) event;
            return casted.getDataNewValue();
        }
    }
}
