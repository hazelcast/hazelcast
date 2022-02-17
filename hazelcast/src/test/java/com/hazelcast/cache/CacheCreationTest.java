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

package com.hazelcast.cache;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheCreationTest extends HazelcastTestSupport {

    private static final int THREAD_COUNT = 4;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void jsrSetup() {
        JsrTestUtil.setup();
    }

    @After
    public void teardown() {
        JsrTestUtil.cleanup();
    }

    @Test
    public void createSingleCache() {
        CachingProvider cachingProvider = createCachingProvider(getDeclarativeConfig());
        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache("xmlCache" + 1);
        cache.get(1);
    }

    @Test
    public void concurrentCacheCreation() throws InterruptedException {
        // see https://github.com/hazelcast/hazelcast/issues/17284
        String cacheName = "myCache";
        int threadCount = Runtime.getRuntime().availableProcessors() * 20;
        Config config = new Config().addCacheConfig(new CacheSimpleConfig().setName(cacheName));
        CacheManager cacheManager = createCachingProvider(config).getCacheManager();

        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger errorCounter = new AtomicInteger();
        Runnable getCache = () -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                Cache<?, ?> cache = cacheManager.getCache(cacheName);
                if (cache == null) {
                    System.out.println("getCache() returned null!");
                    errorCounter.incrementAndGet();
                }
            } catch (Throwable t) {
                t.printStackTrace();
                errorCounter.incrementAndGet();
            }
        };
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(getCache);
        }
        // start all threads at once
        startLatch.countDown();

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        assertEquals(0, errorCounter.get());
    }

    @Test
    public void createOrGetConcurrentlySingleCache_fromMultiProviders() {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        final CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    CachingProvider cachingProvider = createCachingProvider(getDeclarativeConfig());
                    Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache("xmlCache");
                    cache.get(1);
                    latch.countDown();
                }
            });
        }
        assertOpenEventually(latch);
        executorService.shutdown();
    }

    @Test
    public void createConcurrentlyMultipleCaches_fromMultipleProviders() {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        final CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            final String cacheName = "xmlCache" + i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    CachingProvider cachingProvider = createCachingProvider(getDeclarativeConfig());
                    Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache(cacheName);
                    cache.get(1);
                    latch.countDown();
                }
            });
        }
        assertOpenEventually(latch);
        executorService.shutdown();
    }

    @Test
    public void createInvalidCache_fromProgrammaticConfig_throwsException_fromCacheManager_getCache() {
        Config config = createInvalidConfig();
        CachingProvider cachingProvider = createCachingProvider(config);
        CacheManager defaultCacheManager = cachingProvider.getCacheManager();

        thrown.expect(IllegalArgumentException.class);
        defaultCacheManager.getCache("test");
    }

    @Test
    public void createInvalidCache_throwsException_fromCacheManager_createCache() {
        CachingProvider cachingProvider = createCachingProvider(createBasicConfig());
        CacheManager defaultCacheManager = cachingProvider.getCacheManager();

        thrown.expect(IllegalArgumentException.class);
        defaultCacheManager.createCache("test", createInvalidCacheConfig());
    }

    @Test
    public void createInvalidCache_fromDeclarativeConfig_throwsException_fromHazelcastInstanceCreation() {
        System.setProperty("hazelcast.config", "classpath:test-hazelcast-invalid-cache.xml");
        CachingProvider cachingProvider = Caching.getCachingProvider();

        thrown.expect(CacheException.class);
        cachingProvider.getCacheManager();
    }

    @Test
    public void getExistingCache_onNewCacheManager_afterManagerClosed() {
        CachingProvider provider = Caching.getCachingProvider();
        CacheManager manager = provider.getCacheManager();
        String cacheName = randomName();
        Cache cache = manager.createCache(cacheName, new CacheConfig());
        assertEquals(manager, cache.getCacheManager());

        manager.close();

        // cache is no longer managed
        assertNull(cache.getCacheManager());

        manager = provider.getCacheManager();
        cache = manager.getCache(cacheName);
        assertEquals(manager, cache.getCacheManager());
    }

    @Test
    public void getExistingCache_afterCacheClosed() {
        CachingProvider provider = Caching.getCachingProvider();
        CacheManager manager = provider.getCacheManager();
        String cacheName = randomName();
        Cache cache = manager.createCache(cacheName, new CacheConfig());
        assertEquals(manager, cache.getCacheManager());

        cache.close();

        // cache is no longer managed
        assertNull(cache.getCacheManager());

        cache = manager.getCache(cacheName);
        // cache is now managed
        assertEquals(manager, cache.getCacheManager());
    }

    protected CachingProvider createCachingProvider(Config hzConfig) {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(hzConfig);
        return createServerCachingProvider(hazelcastInstance);
    }

    private Config getDeclarativeConfig() {
        InputStream config = CacheCreationTest.class.getClassLoader().getResourceAsStream("test-hazelcast-real-jcache.xml");
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(config);
        return configBuilder.build();
    }

    // fails on OS due to NATIVE in-memory format, on EE due to invalid eviction config for NATIVE memory
    private Config createInvalidConfig() {
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setName("test")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(new EvictionConfig().setSize(1000)
                        .setMaxSizePolicy(ENTRY_COUNT).setEvictionPolicy(EvictionPolicy.LFU));

        return createBasicConfig()
                .addCacheConfig(cacheSimpleConfig);
    }

    private CacheConfig createInvalidCacheConfig() {
        return new CacheConfig("test")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(new EvictionConfig()
                        .setSize(1000)
                        .setMaxSizePolicy(ENTRY_COUNT)
                        .setEvictionPolicy(EvictionPolicy.LFU));
    }

    protected Config createBasicConfig() {
        Config config = new Config();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig()
                .setEnabled(false);
        joinConfig.getTcpIpConfig()
                .setEnabled(true)
                .setMembers(singletonList("127.0.0.1"));
        return config;
    }
}
