package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.config.NearCacheConfig;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class NearCacheManagerTestSupport extends CommonNearCacheTestSupport {

    protected static final int DEFAULT_NEAR_CACHE_COUNT = 5;

    protected abstract NearCacheManager createNearCacheManager();

    protected NearCache createNearCache(NearCacheManager nearCacheManager, String name) {
        return nearCacheManager.getOrCreateNearCache(name,
                                                     createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME,
                                                                           NearCacheConfig.DEFAULT_MEMORY_FORMAT),
                                                     createNearCacheContext());
    }

    protected void doCreateAndGetNearCache() {
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

    protected void doListNearCaches() {
        NearCacheManager nearCacheManager = createNearCacheManager();

        Collection<NearCache> nearCaches1 = nearCacheManager.listAllNearCaches();
        assertEquals(0, nearCaches1.size());

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME + "-" + i);
        }

        Collection<NearCache> nearCaches2 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches2.size());

        int i = 0;
        for (NearCache nearCache : nearCaches2) {
            assertEquals(DEFAULT_NEAR_CACHE_NAME + "-" + (i++), nearCache.getName());
        }
    }

    protected void doClearNearCacheAndClearAllNearCaches() {
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
        // Clear doesn't remove near cache, just clears it
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches2.size());

        nearCacheManager.clearAllNearCaches();
        Collection<NearCache> nearCaches3 = nearCacheManager.listAllNearCaches();
        // Clear all doesn't remove near caches, just clears them
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches3.size());

        assertFalse(nearCacheManager.clearNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + DEFAULT_NEAR_CACHE_COUNT));
    }

    protected void doDestroyNearCacheAndDestroyAllNearCaches() {
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
        // Destroy also removes near cache
        assertEquals(0, nearCaches2.size());

        assertFalse(nearCacheManager.clearNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + DEFAULT_NEAR_CACHE_COUNT));

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME + "-" + i);
        }

        Collection<NearCache> nearCaches3 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches3.size());

        nearCacheManager.destroyAllNearCaches();
        Collection<NearCache> nearCaches4 = nearCacheManager.listAllNearCaches();
        // Destroy all also removes near caches
        assertEquals(0, nearCaches4.size());
    }

}
