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

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.adapter.DataStructureLoader;
import com.hazelcast.internal.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nearcache.NearCacheStats;

import javax.cache.CacheManager;

/**
 * Context for unified Near Cache tests.
 */
@SuppressWarnings("WeakerAccess")
public class NearCacheTestContext<K, V, NK, NV> {

    /**
     * The {@link NearCacheConfig} of the configured Near Cache.
     */
    public final NearCacheConfig nearCacheConfig;
    /**
     * The {@link SerializationService} used by the configured Near Cache.
     */
    public final SerializationService serializationService;

    /**
     * The {@link HazelcastInstance} which has the configured Near Cache.
     * <p>
     * In a scenario with Hazelcast client and member, this will be the client.
     */
    public final HazelcastInstance nearCacheInstance;
    /**
     * The {@link HazelcastInstance} which holds backing data structure for the Near Cache.
     * <p>
     * In a scenario with Hazelcast client and member, this will be the member.
     */
    public final HazelcastInstance dataInstance;
    /**
     * The {@link DataStructureAdapter} which has the configured Near Cache.
     * <p>
     * In a scenario with Hazelcast client and member, this will be the near cached data structure on the client.
     */
    public final DataStructureAdapter<K, V> nearCacheAdapter;
    /**
     * The {@link DataStructureAdapter} which has the original data.
     * <p>
     * In a scenario with Hazelcast client and member, this will be the original data structure on the member.
     */
    public final DataStructureAdapter<K, V> dataAdapter;

    /**
     * The configured {@link NearCache}.
     */
    public final NearCache<NK, NV> nearCache;
    /**
     * The {@link NearCacheStats} of the configured Near Cache.
     */
    public final NearCacheStatsImpl stats;
    /**
     * The {@link NearCacheManager} which manages the configured Near Cache.
     */
    public final NearCacheManager nearCacheManager;
    /**
     * The {@link CacheManager} if the configured {@link DataStructureAdapter} is a JCache implementation.
     */
    public final CacheManager cacheManager;
    /**
     * The {@link HazelcastServerCacheManager} if the configured {@link DataStructureAdapter} is a JCache implementation.
     */
    public final HazelcastServerCacheManager memberCacheManager;

    /**
     * Specifies if the we are able to retrieve {@link DataStructureAdapter#getLocalMapStats()}.
     */
    public final boolean hasLocalData;
    /**
     * The {@link DataStructureLoader} which loads entries into the data structure.
     */
    public final DataStructureLoader loader;

    /**
     * The {@link RepairingTask} from the Near Cache instance.
     */
    public final RepairingTask repairingTask;

    @SuppressWarnings("checkstyle:parameternumber")
    NearCacheTestContext(NearCacheConfig nearCacheConfig,
                         SerializationService serializationService,
                         HazelcastInstance nearCacheInstance,
                         HazelcastInstance dataInstance,
                         DataStructureAdapter<K, V> nearCacheAdapter,
                         DataStructureAdapter<K, V> dataAdapter,
                         NearCache<NK, NV> nearCache,
                         NearCacheManager nearCacheManager,
                         CacheManager cacheManager,
                         HazelcastServerCacheManager memberCacheManager,
                         boolean hasLocalData,
                         DataStructureLoader loader,
                         RepairingTask repairingTask) {
        this.nearCacheConfig = nearCacheConfig;
        this.serializationService = serializationService;

        this.nearCacheInstance = nearCacheInstance;
        this.dataInstance = dataInstance;
        this.nearCacheAdapter = nearCacheAdapter;
        this.dataAdapter = dataAdapter;

        this.nearCache = nearCache;
        this.stats = (nearCache == null) ? null : (NearCacheStatsImpl) nearCache.getNearCacheStats();
        this.nearCacheManager = nearCacheManager;
        this.cacheManager = cacheManager;
        this.memberCacheManager = memberCacheManager;

        this.hasLocalData = hasLocalData;
        this.loader = loader;
        this.repairingTask = repairingTask;
    }
}
