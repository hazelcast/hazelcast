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

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;
import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.AdditionalServiceClassLoader;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.io.Closeable;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.CacheTestSupport.getCacheService;
import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceItself;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class CacheTenantControlTest extends HazelcastTestSupport {

    @Parameters(name = "tenantControl: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {true},
                {false}
        });
    }

    private static AtomicInteger saveCurrentCount = new AtomicInteger();
    private static AtomicInteger setTenantCount = new AtomicInteger();
    private static AtomicInteger closeTenantCount = new AtomicInteger();
    private static AtomicInteger unregisterTenantCount = new AtomicInteger();

    @Parameter
    public boolean hasTenantControl;

    private Config config;
    private String cacheName;

    @Before
    public void setup() {
        cacheName = randomName();
        config = new Config();
        if (hasTenantControl) {
            ClassLoader configClassLoader = new AdditionalServiceClassLoader(new URL[0],
                    CacheTenantControlTest.class.getClassLoader());
            config.setClassLoader(configClassLoader);
        }
        config.getCacheConfig("*");
        saveCurrentCount.set(0);
        setTenantCount.set(0);
        closeTenantCount.set(0);
        unregisterTenantCount.set(0);
    }

    @Test
    public void testTenantControl_whenCacheCreatedViaCacheManager() {
        HazelcastInstance hz = createHazelcastInstance(config);

        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(hz);
        CacheManager cacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(hz));
        Cache cache = cacheManager.createCache(cacheName, new CacheConfig());

        assertTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_whenCacheObtainedViaCacheManager() {
        HazelcastInstance hz = createHazelcastInstance(config);

        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(hz);
        CacheManager cacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(hz));
        Cache cache = cacheManager.getCache(cacheName);

        assertTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_whenCacheObtainedAsDistributedObject() {
        HazelcastInstance hz = createHazelcastInstance(config);
        ICache<Integer, Integer> cache = hz.getCacheManager().getCache(cacheName);

        assertTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_executionBeforeAfterOps() {
        Assume.assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HazelcastInstance hz = createHazelcastInstance(config);
        ICache<Integer, Integer> cache = hz.getCacheManager().getCache(cacheName);

        cache.put(1, 1);
        cache.get(1);
        cache.getAndPut(1, 2);

        cache.destroy();

        assertEquals(1, saveCurrentCount.get());
        assertEquals(3, setTenantCount.get());
        assertEquals(3, closeTenantCount.get());
        assertEquals(1, unregisterTenantCount.get());
    }

    private void assertTenantControlCreated(HazelcastInstance instance) {
        ICacheService cacheService = getCacheService(instance);
        CacheConfig cacheConfig = cacheService.getCacheConfig(CacheUtil.getDistributedObjectName(cacheName));
        assertNotNull("TenantControl should not be null", cacheConfig.getTenantControl());
        if (hasTenantControl) {
            assertInstanceOf(CountingTenantControl.class, cacheConfig.getTenantControl());
        } else {
            assertEquals(TenantControl.NOOP_TENANT_CONTROL, cacheConfig.getTenantControl());
        }
    }

    public static class CountingTenantControl implements TenantControl {

        @Override
        public TenantControl saveCurrentTenant(DestroyEventContext event) {
            saveCurrentCount.incrementAndGet();
            return this;
        }

        @Override
        public Closeable setTenant(boolean createRequestScope) {
            setTenantCount.incrementAndGet();
            return new Closeable() {
                @Override
                public void close() {
                    closeTenantCount.incrementAndGet();
                }
            };
        }

        @Override
        public void unregister() {
            unregisterTenantCount.incrementAndGet();
        }
    }


}
