package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.cache.impl.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheManagerTest extends NearCacheManagerTestSupport {

    @Override
    protected NearCacheManager createNearCacheManager() {
        return new DefaultNearCacheManager();
    }

    @Test
    public void createAndGetNearCache() {
        doCreateAndGetNearCache();
    }

    @Test
    public void listNearCaches() {
        doListNearCaches();
    }

    @Test
    public void clearNearCacheAndClearAllNearCaches() {
        doClearNearCacheAndClearAllNearCaches();
    }

    @Test
    public void destroyNearCacheAndDestroyAllNearCaches() {
        doDestroyNearCacheAndDestroyAllNearCaches();
    }

}
