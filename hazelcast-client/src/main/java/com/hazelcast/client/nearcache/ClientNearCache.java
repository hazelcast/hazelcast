/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.monitor.NearCacheStats;

/**
 * ClientNearCache
 *
 * @param <K> key type
 */
public interface ClientNearCache<K, V> {

    /**
     * Eviction factor
     */
    double EVICTION_FACTOR = 0.2;

    /**
     * TTL Clean up interval
     */
    int TTL_CLEANUP_INTERVAL_MILLS = 5000;

    /**
     * NULL Object
     */
    Object NULL_OBJECT = new Object();

    V get(K key);

    void put(K key, V object);

    void remove(K key);

    void invalidate(K key);

    void clear();

    void destroy();

    boolean isInvalidateOnChange();

    InMemoryFormat getInMemoryFormat();

    void setId(String id);

    String getId();

    NearCacheStats getNearCacheStats();

}
