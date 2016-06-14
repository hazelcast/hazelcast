package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheDestroyTest extends HazelcastTestSupport {

    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instance1 = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();
    }

    @Test
    public void test_cacheDestroyOperation() throws ExecutionException, InterruptedException {
        final String CACHE_NAME = "MyCache";
        final String FULL_CACHE_NAME = HazelcastCacheManager.CACHE_MANAGER_PREFIX + CACHE_NAME;

        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(instance1);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache(CACHE_NAME, new CacheConfig());

        NodeEngineImpl nodeEngine1 = getNode(instance1).getNodeEngine();
        final ICacheService cacheService1 = nodeEngine1.getService(ICacheService.SERVICE_NAME);
        InternalOperationService operationService1 = nodeEngine1.getOperationService();

        NodeEngineImpl nodeEngine2 = getNode(instance2).getNodeEngine();
        final ICacheService cacheService2 = nodeEngine2.getService(ICacheService.SERVICE_NAME);

        assertNotNull(cacheService1.getCacheConfig(FULL_CACHE_NAME));
        assertNotNull(cacheService2.getCacheConfig(FULL_CACHE_NAME));

        // Invoke on single node and the operation is also forward to others nodes by the operation itself
        operationService1.invokeOnTarget(ICacheService.SERVICE_NAME,
                                         new CacheDestroyOperation(FULL_CACHE_NAME),
                                         nodeEngine1.getThisAddress());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cacheService1.getCacheConfig(FULL_CACHE_NAME));
                assertNull(cacheService2.getCacheConfig(FULL_CACHE_NAME));
            }
        });
    }

}
