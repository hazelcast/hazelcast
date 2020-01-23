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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.internal.serialization.Data;

import java.util.Map;
import java.util.Set;

/**
 * Common contract for implementations which store {@link QueryCacheRecord}.
 */
public interface QueryCacheRecordStore {

    QueryCacheRecord add(Data keyData, Data valueData);

    QueryCacheRecord addWithoutEvictionCheck(Data keyData, Data valueData);

    QueryCacheRecord get(Data keyData);

    QueryCacheRecord remove(Data keyData);

    boolean containsKey(Data keyData);

    boolean containsValue(Object value);

    Set<Data> keySet();

    Set<Map.Entry<Data, QueryCacheRecord>> entrySet();

    int clear();

    boolean isEmpty();

    int size();
}
