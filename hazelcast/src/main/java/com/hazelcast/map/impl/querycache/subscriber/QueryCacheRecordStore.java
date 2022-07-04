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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Common contract for implementations which store {@link QueryCacheRecord}.
 */
public interface QueryCacheRecordStore {

    QueryCacheRecord add(Object queryCacheKey, Data valueData);

    QueryCacheRecord addWithoutEvictionCheck(Object queryCacheKey, Data valueData);

    /**
     * Adds entries from the given {@code entryIterator}. For each entry
     * that is successfully added, the given {@code postProcessor} is invoked
     * with the added entry and the old {@link QueryCacheRecord} that was
     * replaced as arguments. Depending on the query cache's eviction
     * configuration, it is possible that not all entries from the
     * iterator will be added to the record store.
     */
    void addBatch(Iterator<Map.Entry<Data, Data>> entryIterator,
                  BiConsumer<Map.Entry<Data, Data>, QueryCacheRecord> postProcessor);

    QueryCacheRecord get(Object queryCacheKey);

    QueryCacheRecord remove(Object queryCacheKey);

    boolean containsKey(Object queryCacheKey);

    boolean containsValue(Object value);

    Set keySet();

    Set<Map.Entry<Object, QueryCacheRecord>> entrySet();

    int clear();

    boolean isEmpty();

    int size();

    Object toQueryCacheKey(Object key);
}
