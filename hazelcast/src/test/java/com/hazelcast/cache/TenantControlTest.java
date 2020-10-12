/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.AdditionalServiceClassLoader;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;
import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.spi.tenantcontrol.TenantControlFactory;
import com.hazelcast.spi.tenantcontrol.Tenantable;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.getCacheService;
import static com.hazelcast.cache.CacheTestSupport.getTenantControl;
import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceItself;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class TenantControlTest extends HazelcastTestSupport {
    private static final ThreadLocal<TenantControl> savedTenant = new ThreadLocal<>();
    private static final AtomicBoolean tenantFactoryInitialized = new AtomicBoolean();
    private static final AtomicInteger setTenantCount = new AtomicInteger();
    private static final AtomicInteger closeTenantCount = new AtomicInteger();
    private static final AtomicInteger registerTenantCount = new AtomicInteger();
    private static final AtomicInteger unregisterTenantCount = new AtomicInteger();
    private static final AtomicInteger clearedThreadInfoCount = new AtomicInteger();

    static final AtomicReference<DestroyEventContext> destroyEventContext = new AtomicReference<>(null);
    static volatile boolean classesAlwaysAvailable;

    @Parameter
    public boolean hasTenantControl;

    private String cacheName;

    @Parameters(name = "tenantControl: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Before
    public void setup() {
        cacheName = randomName();
        initState();
    }

    private Config getNewConfig() {
        return newConfig(hasTenantControl);
    }

    static Config newConfig() {
        return newConfig(true);
    }

    static Config newConfig(boolean hasTenantControl) {
        Config config = smallInstanceConfig();
        if (hasTenantControl) {
            ClassLoader configClassLoader = new AdditionalServiceClassLoader(new URL[0],
                    TenantControlTest.class.getClassLoader());
            config.setClassLoader(configClassLoader);
        }
        config.getCacheConfig("*");
        return config;
    }

    static void initState() {
        tenantFactoryInitialized.set(false);
        savedTenant.remove();
        setTenantCount.set(0);
        closeTenantCount.set(0);
        registerTenantCount.set(0);
        unregisterTenantCount.set(0);
        clearedThreadInfoCount.set(0);
        classesAlwaysAvailable = false;
    }

    @Test
    public void testTenantControl_whenCacheCreatedViaCacheManager() {
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());

        CachingProvider provider = createServerCachingProvider(hz);
        CacheManager cacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(hz));
        Cache cache = cacheManager.createCache(cacheName, new CacheConfig());

        assertTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_whenCacheObtainedViaCacheManager() {
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());

        CachingProvider provider = createServerCachingProvider(hz);
        CacheManager cacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(hz));
        Cache cache = cacheManager.getCache(cacheName);

        assertTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_whenCacheObtainedAsDistributedObject() {
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());
        ICache<Integer, Integer> cache = hz.getCacheManager().getCache(cacheName);

        assertTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_executionBeforeAfterOps() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());
        ICache<Integer, Integer> cache = hz.getCacheManager().getCache(cacheName);

        cache.put(1, 1);
        cache.get(1);
        cache.getAndPut(1, 2);

        cache.destroy();

        assertNotNull(savedTenant.get());
        // expecting tenant context is created & closed 4 times:
        // CachePutOperation
        // AbstractCacheRecordStore.getExpiryPolicy
        // CacheGetOperation
        // CachePutOperation
        // these operations don't require tenant control:
        // AddCacheConfigOperation
        // TenantControlReplicationOperation
        assertEquals(4, setTenantCount.get());
        assertEquals(4, closeTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEquals(1, unregisterTenantCount.get());
        assertEquals(3, clearedThreadInfoCount.get());
    }

    @Test
    public void testDestroyEventContext_destroyRemovesTenantControl() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());
        ICache<Integer, Integer> cache = hz.getCacheManager().getCache(cacheName);

        cache.put(1, 1);
        cache.get(1);
        cache.getAndPut(1, 2);

        destroyEventContext.get().tenantUnavailable();

        assertInstanceOf(CountingTenantControl.class, getTenantControl(hz, cache));
    }

    @Test
    public void basicMapTest() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HazelcastInstance hz = createHazelcastInstance(getNewConfig().setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1"));
        IMap<String, Integer> map = hz.getMap("MyMap");
        map.addEntryListener((EntryAddedListener<String, Integer>)
                (EntryEvent<String, Integer> event) -> System.out.format("Added: %s\n", event.getValue()), true);
        map.put("oneKey", 1);
        map.destroy();
        assertNotNull(savedTenant.get());
        // only PutOperation requires and establishes tenant context
        // event listener registration does not and map destroy
        // operation does not need tenant context
        assertEquals(1, setTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEquals(1, unregisterTenantCount.get());
    }

    private void assertTenantControlCreated(HazelcastInstance instance) {
        ICacheService cacheService = getCacheService(instance);
        CacheConfig<?, ?> cacheConfig = cacheService.getCacheConfig(CacheUtil.getDistributedObjectName(cacheName));
        assertNotNull("TenantControl should not be null", getTenantControl(instance, cacheConfig));
        if (hasTenantControl) {
            assertInstanceOf(CountingTenantControl.class, getTenantControl(instance, cacheConfig));
        } else {
            assertEquals(TenantControl.NOOP_TENANT_CONTROL, getTenantControl(instance, cacheConfig));
        }
    }

    public static class CountingTenantControl implements TenantControl {
        @Override
        public Closeable setTenant() {
            if (!isAvailable(null)) {
                throw new IllegalStateException("Tenant Not Available");
            }
            setTenantCount.incrementAndGet();
            return closeTenantCount::incrementAndGet;
        }

        @Override
        public void registerObject(@Nonnull DestroyEventContext event) {
            destroyEventContext.set(event);
            registerTenantCount.incrementAndGet();
        }

        @Override
        public void unregisterObject() {
            unregisterTenantCount.incrementAndGet();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public boolean isAvailable(@Nonnull Tenantable tenantable) {
            return true;
        }

        @Override
        public void clearThreadContext() {
            clearedThreadInfoCount.incrementAndGet();
        }
    }

    public static class CountingTenantControlFactory implements TenantControlFactory {
        @Override
        public TenantControl saveCurrentTenant() {
            if (tenantFactoryInitialized.compareAndSet(false, true)) {
                TenantControl tenantControl;
                if (savedTenant.get() == null) {
                    tenantControl = new CountingTenantControl();
                    savedTenant.set(tenantControl);
                } else {
                    tenantControl = savedTenant.get();
                }
                return tenantControl;
            } else if (savedTenant.get() != null) {
                return savedTenant.get();
            } else {
                return TenantControl.NOOP_TENANT_CONTROL;
            }
        }

        @Override
        public boolean isClassesAlwaysAvailable() {
            return classesAlwaysAvailable;
        }
    }
}
