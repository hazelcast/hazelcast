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

package com.hazelcast.jet;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.journal.EventJournalMapEvent;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Map.Entry;

/**
 * Miscellaneous utility methods useful in DAG building logic.
 */
public final class Util {
    private static final char[] ID_TEMPLATE = "0000-0000-0000-0000".toCharArray();

    private Util() {
    }

    /**
     * Returns a {@code Map.Entry} with the given key and value.
     */
    public static <K, V> Entry<K, V> entry(K k, V v) {
        return new SimpleImmutableEntry<>(k, v);
    }

    /**
     * Returns a predicate for {@link Sources#mapJournal} and
     * {@link Sources#remoteMapJournal} that passes only
     * {@link EntryEventType#ADDED ADDED} and {@link EntryEventType#UPDATED
     * UPDATED} events.
     */
    public static <K, V> DistributedPredicate<EventJournalMapEvent<K, V>> mapPutEvents() {
        return e -> e.getType() == EntryEventType.ADDED || e.getType() == EntryEventType.UPDATED;
    }

    /**
     * Returns a predicate for {@link Sources#cacheJournal} and
     * {@link Sources#remoteCacheJournal} that passes only
     * {@link CacheEventType#CREATED CREATED} and {@link CacheEventType#UPDATED
     * UPDATED} events.
     */
    public static <K, V> DistributedPredicate<EventJournalCacheEvent<K, V>> cachePutEvents() {
        return e -> e.getType() == CacheEventType.CREATED || e.getType() == CacheEventType.UPDATED;
    }

    /**
     * Returns a projection that converts the {@link EventJournalMapEvent} to a
     * {@link java.util.Map.Entry} using the event's new value as a value.
     *
     * @see Sources#mapJournal
     * @see Sources#remoteMapJournal
     */
    public static <K, V> DistributedFunction<EventJournalMapEvent<K, V>, Entry<K, V>> mapEventToEntry() {
        return e -> entry(e.getKey(), e.getNewValue());
    }

    /**
     * Returns a projection that extracts the new value from a {@link
     * EventJournalMapEvent}.
     *
     * @see Sources#mapJournal
     * @see Sources#remoteMapJournal
     */
    public static <K, V> DistributedFunction<EventJournalMapEvent<K, V>, V> mapEventNewValue() {
        return EventJournalMapEvent::getNewValue;
    }

    /**
     * Returns a projection that converts the {@link EventJournalCacheEvent} to a
     * {@link java.util.Map.Entry} using the event's new value as a value.
     *
     * @see Sources#cacheJournal
     * @see Sources#remoteCacheJournal
     */
    public static <K, V> DistributedFunction<EventJournalCacheEvent<K, V>, Entry<K, V>> cacheEventToEntry() {
        return e -> entry(e.getKey(), e.getNewValue());
    }

    /**
     * Converts a {@code long} job or execution ID to a string representation.
     * Currently it is an unsigned 16-digit hex number.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static String idToString(long id) {
        char[] buf = Arrays.copyOf(ID_TEMPLATE, ID_TEMPLATE.length);
        String hexStr = Long.toHexString(id);
        for (int i = hexStr.length() - 1, j = 18; i >= 0; i--, j--) {
            buf[j] = hexStr.charAt(i);
            if (j == 15 || j == 10 || j == 5) {
                j--;
            }
        }
        return new String(buf);
    }
}
