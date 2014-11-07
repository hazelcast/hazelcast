/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;

import java.lang.reflect.Field;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class JCacheTypeChecks extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hz1;
    private HazelcastInstance hz2;

    private HazelcastServerCachingProvider cachingProvider1;
    private HazelcastServerCachingProvider cachingProvider2;

    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(2);
        hz1 = factory.newHazelcastInstance();
        hz2 = factory.newHazelcastInstance();
        cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(hz1);
        cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(hz2);
    }

    @After
    public void tear() {
        cachingProvider1.close();
        cachingProvider2.close();
        factory.shutdownAll();
    }

    @Test(expected = ClassCastException.class)
    public void test_check_types_on_put() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>()
                .setTypes(Integer.class, String.class);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm2 = cachingProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName, Integer.class, String.class));
            }
        });

        Cache rawCache = (Cache) cache;
        rawCache.put(new Object(), new Object());
    }

    @Test(expected = ClassCastException.class)
    public void test_check_types_on_get() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>();

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm2 = cachingProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName));
            }
        });

        Cache rawCache = (Cache) cache;
        rawCache.put(1, 1);

        Class<?> clazz = Class.forName("com.hazelcast.cache.impl.AbstractCacheProxyBase");
        Field configField = clazz.getDeclaredField("cacheConfig");
        configField.setAccessible(true);

        CacheConfig cacheConfig = (CacheConfig) configField.get(rawCache);
        cacheConfig.setTypes(Integer.class, String.class);

        cache.get(1);
    }

    @Test(expected = ClassCastException.class)
    public void test_check_types_on_getandremove() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>();

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm2 = cachingProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName));
            }
        });

        Cache rawCache = (Cache) cache;
        rawCache.put(1, 1);

        Class<?> clazz = Class.forName("com.hazelcast.cache.impl.AbstractCacheProxyBase");
        Field configField = clazz.getDeclaredField("cacheConfig");
        configField.setAccessible(true);

        CacheConfig cacheConfig = (CacheConfig) configField.get(rawCache);
        cacheConfig.setTypes(Integer.class, String.class);

        cache.getAndRemove(1);
    }

    @Test(expected = ClassCastException.class)
    public void test_check_types_on_replace() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>();

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm2 = cachingProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName));
            }
        });

        Cache rawCache = (Cache) cache;
        rawCache.put(1, 1);

        Class<?> clazz = Class.forName("com.hazelcast.cache.impl.AbstractCacheProxyBase");
        Field configField = clazz.getDeclaredField("cacheConfig");
        configField.setAccessible(true);

        CacheConfig cacheConfig = (CacheConfig) configField.get(rawCache);
        cacheConfig.setTypes(Integer.class, String.class);

        rawCache.replace(1, 3);
    }

    @Test(expected = ClassCastException.class)
    public void test_check_types_on_put_async() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>()
                .setTypes(Integer.class, String.class);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm2 = cachingProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName, Integer.class, String.class));
            }
        });

        ICache rawCache = (ICache) cache.unwrap(ICache.class);
        ICompletableFuture future = rawCache.putAsync(new Object(), new Object());
        future.get();
    }

    @Test(expected = ClassCastException.class)
    public void test_check_types_on_get_async() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>();

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm2 = cachingProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName));
            }
        });

        Cache rawCache = (Cache) cache;
        rawCache.put(1, 1);

        Class<?> clazz = Class.forName("com.hazelcast.cache.impl.AbstractCacheProxyBase");
        Field configField = clazz.getDeclaredField("cacheConfig");
        configField.setAccessible(true);

        CacheConfig cacheConfig = (CacheConfig) configField.get(rawCache);
        cacheConfig.setTypes(Integer.class, String.class);

        ICompletableFuture future = cache.unwrap(ICache.class).getAsync(1);
        future.get();
    }

    @Test(expected = ClassCastException.class)
    public void test_check_types_on_getandremove_async() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>();

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm2 = cachingProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName));
            }
        });

        Cache rawCache = (Cache) cache;
        rawCache.put(1, 1);

        Class<?> clazz = Class.forName("com.hazelcast.cache.impl.AbstractCacheProxyBase");
        Field configField = clazz.getDeclaredField("cacheConfig");
        configField.setAccessible(true);

        CacheConfig cacheConfig = (CacheConfig) configField.get(rawCache);
        cacheConfig.setTypes(Integer.class, String.class);

        ICompletableFuture future = cache.unwrap(ICache.class).getAndRemoveAsync(1);
        future.get();
    }

    @Test(expected = ClassCastException.class)
    public void test_check_types_on_replace_async() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>();

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm2 = cachingProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName));
            }
        });

        ICache rawCache = (ICache) cache.unwrap(ICache.class);
        rawCache.put(1, 1);

        Class<?> clazz = Class.forName("com.hazelcast.cache.impl.AbstractCacheProxyBase");
        Field configField = clazz.getDeclaredField("cacheConfig");
        configField.setAccessible(true);

        CacheConfig cacheConfig = (CacheConfig) configField.get(rawCache);
        cacheConfig.setTypes(Integer.class, String.class);

        ICompletableFuture future = rawCache.replaceAsync(1, 3);
        future.get();
    }
}
