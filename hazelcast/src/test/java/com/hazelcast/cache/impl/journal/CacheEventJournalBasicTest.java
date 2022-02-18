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

import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.journal.AbstractEventJournalBasicTest;
import com.hazelcast.journal.EventJournalTestContext;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.EvictionConfig.DEFAULT_MAX_SIZE_POLICY;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheEventJournalBasicTest<K, V> extends AbstractEventJournalBasicTest<EventJournalMapEvent> {

    private static final String NON_EVICTING_CACHE = "cache";
    private static final String EVICTING_CACHE = "evicting";

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        CacheSimpleConfig nonEvictingCache = config.getCacheConfig(NON_EVICTING_CACHE)
                                                   .setInMemoryFormat(getInMemoryFormat());
        MaxSizePolicy maxSizePolicy = getInMemoryFormat() == InMemoryFormat.NATIVE
                ? USED_NATIVE_MEMORY_SIZE
                : DEFAULT_MAX_SIZE_POLICY;
        nonEvictingCache.getEvictionConfig()
                        .setMaxSizePolicy(maxSizePolicy)
                        .setSize(Integer.MAX_VALUE);

        final CacheSimpleConfig evictingCache = config.getCacheConfig(EVICTING_CACHE)
                                                      .setInMemoryFormat(getInMemoryFormat());
        evictingCache.getEvictionConfig().setMaxSizePolicy(maxSizePolicy);

        return config;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return CacheSimpleConfig.DEFAULT_IN_MEMORY_FORMAT;
    }

    @Override
    protected EventJournalTestContext<K, V, EventJournalCacheEvent<K, V>> createContext() {
        final CacheManager cacheManager = createCacheManager();

        return new EventJournalTestContext<K, V, EventJournalCacheEvent<K, V>>(
                new EventJournalCacheDataStructureAdapter<K, V>((ICache<K, V>) cacheManager.getCache(NON_EVICTING_CACHE)),
                new EventJournalCacheDataStructureAdapter<K, V>((ICache<K, V>) cacheManager.getCache(EVICTING_CACHE)),
                new EventJournalCacheEventAdapter<K, V>()
        );
    }

    @Override
    @Ignore
    public void receiveExpirationEventsWhenPutWithTtl() {
        // not tested
    }

    @Override
    @Ignore
    public void receiveExpirationEventsWhenPutOnExpiringStructure() {
        // not tested
    }

    @Override
    @Ignore
    public void receiveLoadedEventsWhenLoad() {
        // not tested
    }

    @Override
    @Ignore
    public void receiveLoadedEventsWhenLoadAll() {
        // not tested
    }

    protected CacheManager createCacheManager() {
        CachingProvider cachingProvider = createServerCachingProvider(getRandomInstance());
        return cachingProvider.getCacheManager();
    }
}
