package com.hazelcast.cache.instance;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheThroughHazelcastInstanceTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "MyCache";

    private ICache retrieveCache(HazelcastInstance instance, boolean getCache) {
        return retrieveCache(instance, CACHE_NAME, getCache);
    }

    private ICache retrieveCache(HazelcastInstance instance, String cacheName, boolean getCache) {
        return getCache
                ? instance.getCache(cacheName)
                : (ICache) instance.getDistributedObject(ICacheService.SERVICE_NAME,
                                                         HazelcastCacheManager.CACHE_MANAGER_PREFIX + cacheName);
    }

    protected Config createConfig() {
        return new Config();
    }

    protected CacheSimpleConfig createCacheSimpleConfig(String cacheName) {
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName(cacheName);
        return cacheSimpleConfig;
    }

    protected CacheConfig createCacheConfig(String cacheName) {
        return new CacheConfig(cacheName);
    }

    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return HazelcastServerCachingProvider.createCachingProvider(instance);
    }

    protected HazelcastInstance createInstance() {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory();
        return instanceFactory.newHazelcastInstance();
    }

    protected HazelcastInstance createInstance(Config config) {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory();
        return instanceFactory.newHazelcastInstance(config);
    }

    protected void shutdownOwnerInstance(HazelcastInstance instance) {
        instance.shutdown();
    }

    protected Class<? extends Exception> getInstanceNotActiveExceptionType() {
        return HazelcastInstanceNotActiveException.class;
    }

    @Test(expected = CacheNotExistsException.class)
    public void test_getCache_fails_when_thereIsNoCacheConfig() {
        do_test_retrieveCache_fails_when_thereIsNoCacheConfig(true);
    }

    @Test(expected = CacheNotExistsException.class)
    public void test_getDistributedObject_fails_when_thereIsNoCacheConfig() {
        do_test_retrieveCache_fails_when_thereIsNoCacheConfig(false);
    }

    private void do_test_retrieveCache_fails_when_thereIsNoCacheConfig(boolean getCache) {
        HazelcastInstance instance = createInstance();
        retrieveCache(instance, getCache);
    }

    @Test(expected = IllegalStateException.class)
    public void test_getCache_fails_when_jcacheLibIsNotAvailable() {
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
            ClassLoader classLoader = new NonJCacheAwareClassLoader();
            Config config = createConfig();
            config.setClassLoader(classLoader);
            Thread.currentThread().setContextClassLoader(classLoader);
            HazelcastInstance instance = createInstance(config);

            retrieveCache(instance, true);
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
    }

    @Test
    public void test_otherServices_works_when_jcacheLibIsNotAvailable() {
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
            ClassLoader classLoader = new NonJCacheAwareClassLoader();
            Config config = createConfig();
            config.setClassLoader(classLoader);
            Thread.currentThread().setContextClassLoader(classLoader);
            HazelcastInstance instance = createInstance(config);

            IMap map = instance.getMap(randomName());
            map.put(1, "Value-1");
            assertEquals("Value-1", map.get(1));
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
    }

    @Test
    public void test_getCache_succeeds_when_thereIsCacheConfig_and_definedInConfig() {
        do_test_retrieveCache_succeeds_when_thereIsCacheConfig_and_definedInConfig(true);
    }

    @Test
    public void test_getDistributedObject_succeeds_when_thereIsCacheConfig_and_definedInConfig() {
        do_test_retrieveCache_succeeds_when_thereIsCacheConfig_and_definedInConfig(false);
    }

    private void do_test_retrieveCache_succeeds_when_thereIsCacheConfig_and_definedInConfig(boolean getCache) {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HazelcastInstance instance = createInstance(config);

        Cache cache = retrieveCache(instance, getCache);
        assertNotNull(cache);
    }

    @Test
    public void test_getCache_returnsSameCache_whenThereIsCacheConfig_and_createdByCacheManager() {
        do_test_retrieveCache_returnsSameCache_whenThereIsCacheConfig_and_createdByCacheManager(true);
    }

    @Test
    public void test_getDistributedObject_returnsSameCache_whenThereIsCacheConfig_and_createdByCacheManager() {
        do_test_retrieveCache_returnsSameCache_whenThereIsCacheConfig_and_createdByCacheManager(false);
    }

    private void do_test_retrieveCache_returnsSameCache_whenThereIsCacheConfig_and_createdByCacheManager(boolean getCache) {
        HazelcastInstance instance = createInstance();

        CachingProvider cachingProvider = createCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        Cache cache1 = cacheManager.createCache(CACHE_NAME, createCacheConfig(CACHE_NAME));
        assertNotNull(cache1);

        Cache cache2 = retrieveCache(instance, getCache);
        assertNotNull(cache2);

        // Verify that they are same cache instance
        assertTrue(cache1 == cache2);
    }

    @Test
    public void test_getCache_returnsSameCache_when_thereIsCacheConfigWithURI_and_createdByCacheManager()
            throws URISyntaxException {
        do_test_retrieveCache_returnsSameCache_when_thereIsCacheConfigWithURI_and_createdByCacheManager(true);
    }

    @Test
    public void test_getDistributedObject_returnsSameCache_when_thereIsCacheConfigWithURI_and_createdByCacheManager()
            throws URISyntaxException {
        do_test_retrieveCache_returnsSameCache_when_thereIsCacheConfigWithURI_and_createdByCacheManager(false);
    }

    private void do_test_retrieveCache_returnsSameCache_when_thereIsCacheConfigWithURI_and_createdByCacheManager(boolean getCache)
            throws URISyntaxException {
        HazelcastInstance instance = createInstance();

        CachingProvider cachingProvider = createCachingProvider(instance);
        Properties properties = HazelcastCachingProvider.propertiesByInstanceItself(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager(new URI("MY-URI"),  null, properties);

        Cache cache1 = cacheManager.createCache(CACHE_NAME, createCacheConfig(CACHE_NAME));
        assertNotNull(cache1);

        Cache cache2 = retrieveCache(instance, "MY-URI/" + CACHE_NAME, getCache);
        assertNotNull(cache2);

        // Verify that they are same cache instance
        assertTrue(cache1 == cache2);
    }

    @Test(expected = CacheNotExistsException.class)
    public void test_getCache_fails_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByCacheManager()
            throws URISyntaxException {
        do_test_retrieveCache_fails_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByCacheManager(true);
    }

    @Test(expected = CacheNotExistsException.class)
    public void test_getDistributedObject_fails_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByCacheManager()
            throws URISyntaxException {
        do_test_retrieveCache_fails_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByCacheManager(false);
    }

    private void do_test_retrieveCache_fails_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByCacheManager(boolean getCache)
            throws URISyntaxException {
        HazelcastInstance instance = createInstance();

        CachingProvider cachingProvider = createCachingProvider(instance);
        Properties properties = HazelcastCachingProvider.propertiesByInstanceItself(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager(new URI("MY-URI"),  null, properties);

        Cache cache1 = cacheManager.createCache(CACHE_NAME, createCacheConfig(CACHE_NAME));
        assertNotNull(cache1);

        retrieveCache(instance, getCache);
    }

    @Test
    public void test_getCache_returnsSameCache_when_thereIsCacheConfig_and_createdByInstance() {
        do_test_retrieveCache_returnsSameCache_when_thereIsCacheConfig_and_createdByInstance(true);
    }

    @Test
    public void test_getDistributedObject_returnsSameCache_when_thereIsCacheConfig_and_createdByInstance() {
        do_test_retrieveCache_returnsSameCache_when_thereIsCacheConfig_and_createdByInstance(false);
    }

    private void do_test_retrieveCache_returnsSameCache_when_thereIsCacheConfig_and_createdByInstance(boolean getCache) {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HazelcastInstance instance = createInstance(config);

        Cache cache1 = retrieveCache(instance, getCache);
        assertNotNull(cache1);

        CachingProvider cachingProvider = createCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        Cache cache2 = cacheManager.getCache(CACHE_NAME);
        assertNotNull(cache2);

        // Verify that they are same cache instance
        assertTrue(cache1 == cache2);
    }

    @Test
    public void test_getCache_returnsDifferentCache_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByInstance()
            throws URISyntaxException {
        do_test_retrieveCache_returnsDifferentCache_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByInstance(true);
    }

    @Test
    public void test_getDistributedObject_returnsDifferentCache_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByInstance()
            throws URISyntaxException {
        do_test_retrieveCache_returnsDifferentCache_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByInstance(false);
    }

    private void do_test_retrieveCache_returnsDifferentCache_when_thereIsCacheConfigWithSameNameButDifferentFullName_and_createdByInstance(boolean getCache)
            throws URISyntaxException {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HazelcastInstance instance = createInstance(config);

        Cache cache1 = retrieveCache(instance, getCache);
        assertNotNull(cache1);

        CachingProvider cachingProvider = createCachingProvider(instance);
        Properties properties = HazelcastCachingProvider.propertiesByInstanceItself(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager(new URI("MY-URI"), null, properties);

        Cache cache2 = cacheManager.getCache(CACHE_NAME);
        assertNotNull(cache2);

        assertNotEquals(cache1, cache2);
    }

    @Test
    public void test_getCache_then_operateOnCache_fails_when_ownerInstanceIsShutdown() {
        do_test_operateOnCache_fails_when_ownerInstanceIsShutdown(true);
    }

    @Test
    public void test_getDistributedObject_then_operateOnCache_fails_when_ownerInstanceIsShutdown() {
        do_test_operateOnCache_fails_when_ownerInstanceIsShutdown(false);
    }

    private void do_test_operateOnCache_fails_when_ownerInstanceIsShutdown(boolean getCache) {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HazelcastInstance instance = createInstance(config);

        Cache cache = retrieveCache(instance, getCache);
        assertNotNull(cache);

        cache.put(1, 1);

        shutdownOwnerInstance(instance);

        try {
            cache.put(2, 2);
            fail("Operation on cache must fails because owner instance is not active!");
        } catch (Throwable t) {
            Class<? extends Exception> expectedExceptionType = getInstanceNotActiveExceptionType();
            Class<? extends Throwable> actualExceptionType = t.getClass();
            if (!expectedExceptionType.isAssignableFrom(actualExceptionType)) {
                fail("Expected exception type: " + expectedExceptionType
                        +  ", but actual exception type: " + actualExceptionType);
            }
        }
    }

    @Test
    public void test_getCache_then_cacheIsRemovedFromDistributedObject_when_cacheIsDestroyed() {
        do_test_cacheIsRemovedFromDistributedObject_when_cacheIsDestroyed(true);
    }

    @Test
    public void test_getDistributedObject_then_cacheIsRemovedFromDistributedObject_when_cacheIsDestroyed() {
        do_test_cacheIsRemovedFromDistributedObject_when_cacheIsDestroyed(false);
    }

    private void do_test_cacheIsRemovedFromDistributedObject_when_cacheIsDestroyed(boolean getCache) {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HazelcastInstance instance = createInstance(config);

        ICache cache = retrieveCache(instance, getCache);
        assertNotNull(cache);

        assertTrue(instance.getDistributedObjects().contains(cache));

        cache.destroy();

        assertFalse(instance.getDistributedObjects().contains(cache));
    }

    private static class NonJCacheAwareClassLoader extends ClassLoader {

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (name.startsWith("javax.cache.")) {
                throw new ClassNotFoundException("Couldn't load class " + name + ". Because JCache is disabled!");
            }
            return super.loadClass(name);
        }

    }

}
