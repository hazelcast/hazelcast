package com.hazelcast.internal.nearcache;

import com.hazelcast.internal.nearcache.impl.DefaultNearCacheManager;
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
        return new DefaultNearCacheManager(ss, executionService, null);
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
