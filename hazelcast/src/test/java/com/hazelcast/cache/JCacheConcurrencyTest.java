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
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class JCacheConcurrencyTest extends HazelcastTestSupport {

    private static final int CONCURRENCY_LEVEL = 100;

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

    @Test
    public void test_concurrect_cachemanager_creation() throws Exception {
        final URI uri = new URI(randomString());

        final Semaphore startSemaphore = new Semaphore(CONCURRENCY_LEVEL);
        final CountDownLatch endLatch = new CountDownLatch(CONCURRENCY_LEVEL);
        final AtomicReferenceArray<CacheManager> managers = new AtomicReferenceArray<CacheManager>(CONCURRENCY_LEVEL);

        startSemaphore.acquire(100);
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        // Sync all threads
                        startSemaphore.acquire();

                        // Create CacheManagers
                        CacheManager cacheManager = cachingProvider1.getCacheManager(uri, null);
                        managers.set(index, cacheManager);

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        endLatch.countDown();
                    }
                }
            }).start();
        }

        startSemaphore.release(100);
        endLatch.await(10, TimeUnit.MINUTES);

        CacheManager cacheManager = cachingProvider1.getCacheManager(uri, null);
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            assertTrue("Multiple CacheManagers found at index " + i + " => " + managers, cacheManager == managers.get(i));
        }
    }

    @Test
    public void test_concurrent_cache_creation() throws Exception {
        final String cacheName = randomMapName();

        final CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        final CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>()
                .setTypes(Integer.class, String.class);

        final Semaphore startSemaphore = new Semaphore(CONCURRENCY_LEVEL);
        final CountDownLatch endLatch = new CountDownLatch(CONCURRENCY_LEVEL);
        final AtomicReferenceArray<Cache> proxies = new AtomicReferenceArray<Cache>(CONCURRENCY_LEVEL);

        startSemaphore.acquire(100);
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        // Create a copy of the configuration
                        CompleteConfiguration<Integer, String> configCopy = new CacheConfig<Integer, String>(config);

                        // Sync all threads
                        startSemaphore.acquire();

                        // Create Proxy
                        Cache cache = cacheManager.createCache(cacheName, configCopy);
                        proxies.set(index, cache);

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        endLatch.countDown();
                    }
                }
            }).start();
        }

        startSemaphore.release(100);
        endLatch.await(10, TimeUnit.MINUTES);

        Cache cache = cacheManager.getCache(cacheName, Integer.class, String.class);
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            assertTrue("Multiple CacheProxies found at index " + i + " => " + proxies, cache == proxies.get(i));
        }
    }

    @Test
    public void test_concurrent_cachestatistics_creation() throws Exception {
        final String cacheName = randomMapName();

        final CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        final CompleteConfiguration<Integer, String> config = new CacheConfig<Integer, String>()
                .setTypes(Integer.class, String.class);

        final ICache cache = cacheManager.createCache(cacheName, config).unwrap(ICache.class);

        final Semaphore startSemaphore = new Semaphore(CONCURRENCY_LEVEL);
        final CountDownLatch endLatch = new CountDownLatch(CONCURRENCY_LEVEL);
        final AtomicReferenceArray<CacheStatistics> stats = new AtomicReferenceArray<CacheStatistics>(CONCURRENCY_LEVEL);

        startSemaphore.acquire(100);
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        // Sync all threads
                        startSemaphore.acquire();

                        // Create Statistics
                        CacheStatistics statistics = cache.getLocalCacheStatistics();
                        stats.set(index, statistics);

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        endLatch.countDown();
                    }
                }
            }).start();
        }

        startSemaphore.release(100);
        endLatch.await(10, TimeUnit.MINUTES);

        CacheStatistics statistics = cache.getLocalCacheStatistics();
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            assertTrue("Multiple CacheStatistics found at index " + i + " => " + stats, statistics == stats.get(i));
        }
    }
}
