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

package com.hazelcast.internal.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.serialization.SerializationService;
import org.junit.Before;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class NearCacheManagerTestSupport extends CommonNearCacheTestSupport {

    private static final int DEFAULT_NEAR_CACHE_COUNT = 5;

    protected abstract NearCacheManager createNearCacheManager();

    protected SerializationService ss;
    protected ExecutionService executionService;

    @Before
    public void setUp() {
        HazelcastInstance instance = createHazelcastInstance();
        ss = getSerializationService(instance);
        executionService = getNodeEngineImpl(instance).getExecutionService();
    }

    void doCreateAndGetNearCache() {
        NearCacheManager nearCacheManager = createNearCacheManager();

        assertNull(nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME));

        NearCache createdNearCache1 = createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME);
        assertNotNull(createdNearCache1);

        NearCache createdNearCache2 = createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME);
        assertNotNull(createdNearCache2);
        assertEquals(createdNearCache1, createdNearCache2);

        Collection<NearCache> nearCaches = nearCacheManager.listAllNearCaches();
        assertEquals(1, nearCaches.size());
        assertEquals(createdNearCache1, nearCaches.iterator().next());
    }

    void doListNearCaches() {
        NearCacheManager nearCacheManager = createNearCacheManager();

        Set<String> nearCacheNames = new HashSet<String>();

        Collection<NearCache> nearCaches1 = nearCacheManager.listAllNearCaches();
        assertEquals(0, nearCaches1.size());

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            String nearCacheName = DEFAULT_NEAR_CACHE_NAME + "-" + i;
            createNearCache(nearCacheManager, nearCacheName);
            nearCacheNames.add(nearCacheName);
        }

        Collection<NearCache> nearCaches2 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches2.size());

        for (NearCache nearCache : nearCaches2) {
            assertContains(nearCacheNames, nearCache.getName());
        }
    }

    void doClearNearCacheAndClearAllNearCaches() {
        NearCacheManager nearCacheManager = createNearCacheManager();
        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME + "-" + i);
        }

        Collection<NearCache> nearCaches1 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches1.size());

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            assertTrue(nearCacheManager.clearNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + i));
        }

        Collection<NearCache> nearCaches2 = nearCacheManager.listAllNearCaches();
        // clear doesn't remove Near Cache, just clears it
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches2.size());

        nearCacheManager.clearAllNearCaches();
        Collection<NearCache> nearCaches3 = nearCacheManager.listAllNearCaches();
        // clear all doesn't remove Near Caches, just clears them
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches3.size());

        assertFalse(nearCacheManager.clearNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + DEFAULT_NEAR_CACHE_COUNT));
    }

    void doDestroyNearCacheAndDestroyAllNearCaches() {
        NearCacheManager nearCacheManager = createNearCacheManager();

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME + "-" + i);
        }

        Collection<NearCache> nearCaches1 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches1.size());

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            assertTrue(nearCacheManager.destroyNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + i));
        }

        Collection<NearCache> nearCaches2 = nearCacheManager.listAllNearCaches();
        // destroy also removes Near Cache
        assertEquals(0, nearCaches2.size());

        assertFalse(nearCacheManager.clearNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + DEFAULT_NEAR_CACHE_COUNT));

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME + "-" + i);
        }

        Collection<NearCache> nearCaches3 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches3.size());

        nearCacheManager.destroyAllNearCaches();
        Collection<NearCache> nearCaches4 = nearCacheManager.listAllNearCaches();
        // destroy all also removes Near Caches
        assertEquals(0, nearCaches4.size());
    }

    private NearCache createNearCache(NearCacheManager nearCacheManager, String name) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, DEFAULT_MEMORY_FORMAT);
        return nearCacheManager.getOrCreateNearCache(name, nearCacheConfig);
    }
}
