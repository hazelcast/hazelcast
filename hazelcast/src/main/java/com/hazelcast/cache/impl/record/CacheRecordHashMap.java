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

package com.hazelcast.cache.impl.record;

import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.FetchableConcurrentHashMap;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class CacheRecordHashMap<K, V>
        extends FetchableConcurrentHashMap<K, V>
        implements CacheRecordMap<K, V> {

    public CacheRecordHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public CacheRecordHashMap(int initialCapacity,
                              float loadFactor,
                              int concurrencyLevel,
                              ConcurrentReferenceHashMap.ReferenceType keyType,
                              ConcurrentReferenceHashMap.ReferenceType valueType,
                              EnumSet<ConcurrentReferenceHashMap.Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
    }

    @Override
    public CacheKeyIteratorResult fetchNext(int nextTableIndex, int size) {
        List<Data> keys = new ArrayList<Data>();
        int tableIndex = fetch(nextTableIndex, size, keys);
        return new CacheKeyIteratorResult(keys, tableIndex);
    }

}
