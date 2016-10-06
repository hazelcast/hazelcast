/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.nearcache;

import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.adapter.DataStructureAdapter;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.serialization.SerializationService;

import javax.cache.CacheManager;

/**
 * Context for unified Near Cache tests.
 */
public class NearCacheTestContext<K, V, NK, NV> {

    public final SerializationService serializationService;
    public final HazelcastInstance nearCacheInstance;
    public final HazelcastInstance dataInstance;
    public final DataStructureAdapter<K, V> nearCacheAdapter;
    public final DataStructureAdapter<K, V> dataAdapter;
    public final boolean hasLocalData;

    public final NearCache<NK, NV> nearCache;
    public final NearCacheStats stats;
    public final NearCacheManager nearCacheManager;
    public final CacheManager cacheManager;
    public final HazelcastServerCacheManager memberCacheManager;

    public NearCacheTestContext(SerializationService serializationService,
                                HazelcastInstance nearCacheInstance,
                                DataStructureAdapter<K, V> nearCacheAdapter,
                                boolean hasLocalData,
                                NearCache<NK, NV> nearCache,
                                NearCacheManager nearCacheManager) {
        this(serializationService, nearCacheInstance, nearCacheInstance, nearCacheAdapter, nearCacheAdapter, hasLocalData,
                nearCache, nearCacheManager, null, null);
    }

    public NearCacheTestContext(SerializationService serializationService,
                                HazelcastInstance nearCacheInstance,
                                HazelcastInstance dataInstance,
                                DataStructureAdapter<K, V> nearCacheAdapter,
                                DataStructureAdapter<K, V> dataAdapter,
                                boolean hasLocalData,
                                NearCache<NK, NV> nearCache,
                                NearCacheManager nearCacheManager) {
        this(serializationService, nearCacheInstance, dataInstance, nearCacheAdapter, dataAdapter, hasLocalData,
                nearCache, nearCacheManager, null, null);
    }

    public NearCacheTestContext(SerializationService serializationService,
                                HazelcastInstance nearCacheInstance,
                                HazelcastInstance dataInstance,
                                DataStructureAdapter<K, V> nearCacheAdapter,
                                DataStructureAdapter<K, V> dataAdapter,
                                boolean hasLocalData,
                                NearCache<NK, NV> nearCache,
                                NearCacheManager nearCacheManager,
                                CacheManager cacheManager,
                                HazelcastServerCacheManager memberCacheManager) {
        this.serializationService = serializationService;
        this.nearCacheInstance = nearCacheInstance;
        this.dataInstance = dataInstance;
        this.nearCacheAdapter = nearCacheAdapter;
        this.dataAdapter = dataAdapter;
        this.hasLocalData = hasLocalData;

        this.nearCache = nearCache;
        this.stats = nearCache.getNearCacheStats();
        this.nearCacheManager = nearCacheManager;
        this.cacheManager = cacheManager;
        this.memberCacheManager = memberCacheManager;
    }
}
