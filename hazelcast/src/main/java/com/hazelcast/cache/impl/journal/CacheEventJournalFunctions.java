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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.util.function.Function;
import com.hazelcast.util.function.Predicate;

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

/**
 * Utility functions used from Jet. They are here so that they can be also used
 * when Jet accesses remote imdg (non-jet) cluster.
 */
public final class CacheEventJournalFunctions {

    private CacheEventJournalFunctions() { }

    public static <K, V> Predicate<EventJournalCacheEvent<K, V>> cachePutEvents() {
        return new SerializablePredicate<EventJournalCacheEvent<K, V>>() {
            @Override
            public boolean test(EventJournalCacheEvent<K, V> e) {
                return e.getType() == CacheEventType.CREATED || e.getType() == CacheEventType.UPDATED;
            }
        };
    }

    public static <K, V> Function<EventJournalCacheEvent<K, V>, Entry<K, V>> cacheEventToEntry() {
        return new SerializableFunction<EventJournalCacheEvent<K, V>, Entry<K, V>>() {
            @Override
            public Entry<K, V> apply(EventJournalCacheEvent<K, V> e) {
                return entry(e.getKey(), e.getNewValue());
            }
        };
    }

    public static <K, V> Function<EventJournalCacheEvent<K, V>, V> cacheEventNewValue() {
        return new SerializableFunction<EventJournalCacheEvent<K, V>, V>() {
            @Override
            public V apply(EventJournalCacheEvent<K, V> event) {
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
