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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheUtil;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.spi.CachingProvider;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceItself;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.test.Accessors.getNode;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class CacheCreateUseDestroyTest extends HazelcastTestSupport {

    private static final MemorySize NATIVE_MEMORY_SIZE = new MemorySize(32, MemoryUnit.MEGABYTES);

    @Parameters(name = "{0}")
    public static Collection parameters() {
        return asList(
                new Object[]{OBJECT},
                new Object[]{BINARY},
                new Object[]{NATIVE}
        );
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    protected CacheManager defaultCacheManager;
    protected Cache<String, String> cache;
    protected ICacheService cacheService;

    @Before
    public void setup() {
        assumptions();
        JsrTestUtil.setup();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance member = factory.newHazelcastInstance(getConfig());
        CachingProvider provider = Caching.getCachingProvider();
        defaultCacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(member));
        cacheService = getNode(member).getNodeEngine().getService(ICacheService.SERVICE_NAME);
        CacheEntryListenerFactory.listener = null;
    }

    @After
    public void tearDown() {
        JsrTestUtil.cleanup();
    }

    protected void assumptions() {
        assumeThat(inMemoryFormat, not(NATIVE));
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();

        CacheSimpleEntryListenerConfig entryListenerConfig = new CacheSimpleEntryListenerConfig();
        entryListenerConfig.setCacheEntryListenerFactory("com.hazelcast.cache.impl.CacheCreateUseDestroyTest$CacheEntryListenerFactory");
        entryListenerConfig.setOldValueRequired(true);
        entryListenerConfig.setSynchronous(true);

        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setName("cache*")
                .setInMemoryFormat(inMemoryFormat)
                .setStatisticsEnabled(true)
                .setManagementEnabled(true)
                .setCacheEntryListeners(Collections.singletonList(entryListenerConfig));

        if (inMemoryFormat == NATIVE) {
            EvictionConfig evictionConfig = new EvictionConfig().setSize(90)
                    .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE).setEvictionPolicy(LFU);
            cacheSimpleConfig.setEvictionConfig(evictionConfig);

            NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                    .setEnabled(true)
                    .setSize(NATIVE_MEMORY_SIZE)
                    .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
            config.setNativeMemoryConfig(memoryConfig);
        }

        config.addCacheConfig(cacheSimpleConfig);
        return config;
    }

    @Test
    public void testCache_whenDestroyedByCacheManager() {
        String cacheName = randomMapName("cache");
        cache = defaultCacheManager.getCache(cacheName);
        cache.put("key", "value");
        cache.get("key");
        assertCreatedCache(cacheName);

        defaultCacheManager.destroyCache(cacheName);
        assertDestroyedCache(cacheName);
    }

    @Test
    public void testCache_whenDestroyedByICache_destroy() {
        String cacheName = randomMapName("cache");
        cache = defaultCacheManager.getCache(cacheName);
        DistributedObject internalCacheProxy = cache.unwrap(DistributedObject.class);
        cache.put("key", "value");
        cache.get("key");
        assertCreatedCache(cacheName);

        internalCacheProxy.destroy();
        assertDestroyedCache(cacheName);
    }

    private void assertCreatedCache(String cacheName) {
        assertStatistics(1);
        assertListenerCount(1, cacheName);
        assertMXBeanRegistrationStatus(true, cacheName, false);
        assertMXBeanRegistrationStatus(true, cacheName, true);
    }

    private void assertDestroyedCache(String cacheName) {
        assertStatistics(0);
        assertListenerCount(0, cacheName);
        assertTrue("CacheEntryListener was not properly closed", CacheEntryListenerFactory.listener.closed);
        assertMXBeanRegistrationStatus(false, cacheName, false);
        assertMXBeanRegistrationStatus(false, cacheName, true);
    }

    private void assertListenerCount(int expected, String cacheName) {
        String fullyQualifiedCacheName = CacheUtil.getDistributedObjectName(cacheName);
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(fullyQualifiedCacheName);
        assertEquals("Unexpected listener count", expected, cacheContext.getCacheEntryListenerCount());
    }

    private void assertStatistics(int expectedHits) {
        ICache iCache = cache.unwrap(ICache.class);
        assertEquals("Unexpected cache hits count", expectedHits, iCache.getLocalCacheStatistics().getCacheHits());
    }

    private void assertMXBeanRegistrationStatus(boolean expectedStatus, String cacheName, boolean forStats) {
        CacheConfig cacheConfig = cacheService.findCacheConfig(cacheName);
        String cacheManagerName = cacheConfig.getUriString();
        assertEquals(format("Unexpected MXBean registration status for cacheManager %s and cacheName %s", cacheManagerName,
                cacheName), expectedStatus, MXBeanUtil.isRegistered(cacheManagerName, cacheName, forStats));
    }

    public static class CacheEntryListener implements CacheEntryCreatedListener<String, String>, Serializable, Closeable {

        volatile boolean closed;

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }

    public static class CacheEntryListenerFactory implements Factory<CacheEntryListener> {

        public static volatile CacheEntryListener listener;

        @Override
        public CacheEntryListener create() {
            listener = new CacheEntryListener();
            return listener;
        }
    }
}
