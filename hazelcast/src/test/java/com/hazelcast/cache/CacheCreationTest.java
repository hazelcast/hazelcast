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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheCreationTest extends HazelcastTestSupport {

    private static final int THREAD_COUNT = 4;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void jsrSetup() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void jsrTeardown() {
        JsrTestUtil.cleanup();
    }

    @After
    public void teardown() {
        HazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void createSingleCache() {
        CachingProvider cachingProvider = createCachingProvider(getDeclarativeConfig());
        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache("xmlCache" + 1);
        cache.get(1);
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

    // test special Cache proxy creation, required for compatibility with 3.6 clients
    // should be removed in 4.0
    @Test
    public void test_createSetupRef() {
        assumeFalse("test_createSetupRef is only applicable for Hazelcast members",
                ClassLoaderUtil.isClassAvailable(null, "com.hazelcast.client.HazelcastClient"));
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        try {
            DistributedObject setupRef = HazelcastTestSupport.getNodeEngineImpl(hz).getProxyService()
                    .getDistributedObject(CacheService.SERVICE_NAME, "setupRef");
            assertNotNull(setupRef);
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    protected CachingProvider createCachingProvider(Config hzConfig) {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(hzConfig);
        return HazelcastServerCachingProvider.createCachingProvider(hazelcastInstance);
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
                .setEvictionConfig(new EvictionConfig(1000, ENTRY_COUNT, EvictionPolicy.LFU));

        return createBasicConfig()
                .addCacheConfig(cacheSimpleConfig);
    }

    private CacheConfig createInvalidCacheConfig() {
        return new CacheConfig("test")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(new EvictionConfig(1000, ENTRY_COUNT, EvictionPolicy.LFU));
    }

    private Config createBasicConfig() {
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
