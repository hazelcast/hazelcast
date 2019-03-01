/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.util.function.Function;
import com.hazelcast.util.function.Predicate;

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

/**
 * Utility functions used from Jet. They are here so that they can be also used
 * when Jet accesses remote imdg (non-jet) cluster.
 */
public final class MapEventJournalFunctions {

    private MapEventJournalFunctions() { }

    public static <K, V> Predicate<EventJournalMapEvent<K, V>> mapPutEvents() {
        return new SerializablePredicate<EventJournalMapEvent<K, V>>() {
            @Override
            public boolean test(EventJournalMapEvent<K, V> e) {
                return e.getType() == EntryEventType.ADDED || e.getType() == EntryEventType.UPDATED;
            }
        };
    }

    public static <K, V> Function<EventJournalMapEvent<K, V>, Entry<K, V>> mapEventToEntry() {
        return new SerializableFunction<EventJournalMapEvent<K, V>, Entry<K, V>>() {
            @Override
            public Entry<K, V> apply(EventJournalMapEvent<K, V> e) {
                return entry(e.getKey(), e.getNewValue());
            }
        };
    }

    public static <K, V> Function<EventJournalMapEvent<K, V>, V> mapEventNewValue() {
        return new SerializableFunction<EventJournalMapEvent<K, V>, V>() {
            @Override
            public V apply(EventJournalMapEvent<K, V> event) {
                return event.getNewValue();
            }
        };
    }

    private static <K, V> Entry<K, V> entry(K k, V v) {
        return new SimpleImmutableEntry<>(k, v);
    }

    private abstract static class SerializablePredicate<T> implements Predicate<T>, Serializable { }
    private abstract static class SerializableFunction<T, R> implements Function<T, R>, Serializable { }
}
