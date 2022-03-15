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

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.SerializableByConvention;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Utility functions used from Jet. They are here so that they can be also used
 * when Jet accesses remote imdg (non-jet) cluster.
 */
public final class CacheEventJournalFunctions {

    private CacheEventJournalFunctions() { }

    public static <K, V> Predicate<EventJournalCacheEvent<K, V>> cachePutEvents() {
        return new CachePutEventsPredicate<K, V>();
    }

    public static <K, V> Function<EventJournalCacheEvent<K, V>, Entry<K, V>> cacheEventToEntry() {
        return new CacheEventToEntryProjection<K, V>();
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Function<EventJournalCacheEvent<K, V>, V> cacheEventNewValue() {
        return new CacheEventNewValueProjection();
    }

    @SerializableByConvention
    private static class CachePutEventsPredicate<K, V> implements Predicate<EventJournalCacheEvent<K, V>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean test(EventJournalCacheEvent<K, V> e) {
            return e.getType() == CacheEventType.CREATED || e.getType() == CacheEventType.UPDATED;
        }
    }

    @SerializableByConvention
    private static class CacheEventToEntryProjection<K, V>
            implements Function<EventJournalCacheEvent<K, V>, Entry<K, V>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Entry<K, V> apply(EventJournalCacheEvent<K, V> e) {
            DeserializingEventJournalCacheEvent<K, V> casted = (DeserializingEventJournalCacheEvent<K, V>) e;
            return new DeserializingEntry<K, V>(casted.getDataKey(), casted.getDataNewValue());
        }
    }

    @SerializableByConvention
    private static class CacheEventNewValueProjection implements Function, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Object apply(Object event) {
            DeserializingEventJournalCacheEvent casted = (DeserializingEventJournalCacheEvent) event;
            return casted.getDataNewValue();
        }
    }
}
