/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.record.Expirable;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * ConcurrentHashMap to extend iterator capability
 *
 * @param <K>
 * @param <V>
 */
public class CacheConcurrentHashMap<K, V>
        extends ConcurrentReferenceHashMap<K, V> {

    private static final float LOAD_FACTOR = 0.91f;

    public CacheConcurrentHashMap(int initialCapacity) {
        //concurrency level 1 is important for fetch-method to function properly.
        // Moreover partitions are single threaded and higher concurrency has not much gain
        this(initialCapacity, LOAD_FACTOR, 1, ReferenceType.STRONG, ReferenceType.STRONG, null);
    }

    public CacheConcurrentHashMap(int initialCapacity, float loadFactor, int concurrencyLevel, ReferenceType keyType,
                                  ReferenceType valueType, EnumSet<Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
    }

    public CacheKeyIteratorResult fetchNext(int nextTableIndex, int size) {
        List<Data> keys = new ArrayList<Data>();
        int tableIndex = fetch(nextTableIndex, size, keys);

        return new CacheKeyIteratorResult(keys, tableIndex);
    }

    int fetch(int tableIndex, int size, List<Data> keys) {
        final long now = Clock.currentTimeMillis();
        //        List<K> keys = new ArrayList<K>();
        int nextTableIndex;
        final Segment<K, V> segment = segments[0];
        HashEntry<K, V>[] currentTable = segment.table;
        if (tableIndex >= 0 && tableIndex < segment.table.length) {
            nextTableIndex = tableIndex;
        } else {
            nextTableIndex = currentTable.length - 1;
        }
        int counter = 0;
        while (nextTableIndex >= 0 && counter < size) {
            HashEntry<K, V> nextEntry = currentTable[nextTableIndex--];
            while (nextEntry != null) {
                if (nextEntry.key() != null) {
                    final V value = nextEntry.value();
                    final boolean isExpired = (value instanceof Expirable) && ((Expirable) value).isExpiredAt(now);
                    if (!isExpired) {
                        keys.add((Data) nextEntry.key());
                        counter++;
                    }
                }
                nextEntry = nextEntry.next;
            }
        }
        return nextTableIndex;
    }

}
