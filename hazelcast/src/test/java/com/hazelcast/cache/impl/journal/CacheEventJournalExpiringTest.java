/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.journal.AbstractEventJournalExpiringTest;
import com.hazelcast.journal.EventJournalTestContext;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;

import javax.cache.spi.CachingProvider;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;

@Category({SlowTest.class, ParallelJVMTest.class})
public class CacheEventJournalExpiringTest<K, V> extends AbstractEventJournalExpiringTest<EventJournalCacheEvent> {

    private static final String CACHE_NAME = "cachey";
    private AbstractHazelcastCacheManager cacheManager;

    @Override
    protected Config getConfig() {
        Config defConfig = super.getConfig();
        var cacheConfig = defConfig.getCacheConfig(CACHE_NAME);
        cacheConfig.setEventJournalConfig(getEventJournalConfig());
        return defConfig;
    }

    @Override
    protected EventJournalTestContext<K, V, EventJournalCacheEvent<K, V>> createContext() {
        CachingProvider cachingProvider = createServerCachingProvider(getRandomInstance());
        cacheManager = (AbstractHazelcastCacheManager) cachingProvider.getCacheManager();

        return new EventJournalTestContext<>(
                new EventJournalCacheDataStructureAdapter<>(cacheManager.getCache(CACHE_NAME)),
                null,
                new EventJournalCacheEventAdapter<>()
        );
    }

    @Override
    protected String getName() {
        return CACHE_NAME;
    }

    protected RingbufferContainer<?, ?> getRingBufferContainer(String name, int partitionId, HazelcastInstance instance) {
        var serviceName = RingbufferService.SERVICE_NAME;
        var fullName = cacheManager.getCacheNameWithPrefix(name);
        final RingbufferService service = getNodeEngine(instance).getService(serviceName);
        final ObjectNamespace ns = CacheService.getObjectNamespace(fullName);

        RingbufferContainer<?, ?> ringbuffer = service.getContainerOrNull(partitionId, ns);
        if (ringbuffer == null) {
            throw new IllegalStateException("No ringbuffer container for partition " + partitionId);
        }
        return ringbuffer;
    }
}
