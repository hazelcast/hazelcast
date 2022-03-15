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

package com.hazelcast.spi.tenantcontrol;

import com.hazelcast.cache.CacheUtil;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.getCacheService;
import static com.hazelcast.cache.CacheTestSupport.getTenantControl;
import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceItself;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class TenantControlTest extends TenantControlTestSupport {
    private static final ILogger LOGGER = Logger.getLogger(TenantControlTest.class);

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
        return newConfig(hasTenantControl, TenantControlTest.class.getClassLoader());
    }

    @Test
    public void testTenantControl_whenCacheCreatedViaCacheManager() {
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());

        CachingProvider provider = createServerCachingProvider(hz);
        CacheManager cacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(hz));
        @SuppressWarnings("unused")
        Cache<?, ?> cache = cacheManager.createCache(cacheName, new CacheConfig<>());

        assertCacheTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_whenCacheObtainedViaCacheManager() {
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());

        CachingProvider provider = createServerCachingProvider(hz);
        CacheManager cacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(hz));

        @SuppressWarnings("unused")
        Cache<?, ?> cache = cacheManager.getCache(cacheName);

        assertCacheTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_whenCacheObtainedAsDistributedObject() {
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());

        @SuppressWarnings("unused")
        ICache<Integer, Integer> cache = hz.getCacheManager().getCache(cacheName);

        assertCacheTenantControlCreated(hz);
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
        assertEqualsEventually(1, unregisterTenantCount);
        // thread context should be cleared at least twice.
        // in most cases it would be three times, but there is a case when the structure
        // gets destroyed before operation completes, in which case it's valid
        // to only have thread context cleared only twice
        assertTrueEventually(() -> assertThat("thread context not cleared enough times",
                clearedThreadInfoCount.get(), greaterThanOrEqualTo(2)), 10);
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
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());
        IMap<String, Integer> map = hz.getMap(randomName());
        map.addEntryListener((EntryAddedListener<String, Integer>)
                (EntryEvent<String, Integer> event) -> LOGGER.info("Added: " + event.getValue()), true);
        map.put("oneKey", 1);
        map.destroy();
        assertNotNull(savedTenant.get());
        // only PutOperation requires and establishes tenant context
        // event listener registration does not and map destroy
        // operation does not need tenant context
        assertEquals(1, setTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEqualsEventually(1, unregisterTenantCount);
    }

    @Test
    public void basicSetTest() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());
        ISet<Integer> set = hz.getSet(randomName());
        set.add(1);
        set.add(1);
        set.remove(1);
        set.clear();
        set.destroy();
        assertNotNull(savedTenant.get());
        // set tenant is called twice for add, once for remove and clear
        assertEquals(4, setTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEqualsEventually(1, unregisterTenantCount);
    }

    @Test
    public void basicQueueTest() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());
        IQueue<Integer> q = hz.getQueue(randomName());
        q.add(1);
        q.add(1);
        q.remove(1);
        q.clear();
        q.destroy();
        assertNotNull(savedTenant.get());
        // set tenant is called twice for add, once for remove and clear
        assertEquals(4, setTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEqualsEventually(1, unregisterTenantCount);
    }

    @Test
    public void basicListTest() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HazelcastInstance hz = createHazelcastInstance(getNewConfig());
        IList<Integer> l = hz.getList(randomName());
        l.add(1);
        l.add(1);
        l.remove(1);
        l.clear();
        l.destroy();
        assertNotNull(savedTenant.get());
        // set tenant is called twice for add, once for remove and clear
        assertEquals(4, setTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEqualsEventually(1, unregisterTenantCount);
    }

    private void assertCacheTenantControlCreated(HazelcastInstance instance) {
        ICacheService cacheService = getCacheService(instance);
        CacheConfig<?, ?> cacheConfig
                = cacheService.getCacheConfig(CacheUtil.getDistributedObjectName(cacheName));
        TenantControl tc = getTenantControl(instance, cacheConfig);
        assertNotNull("TenantControl should not be null", tc);
        if (hasTenantControl) {
            assertInstanceOf(CountingTenantControl.class, tc);
        } else {
            assertEquals(TenantControl.NOOP_TENANT_CONTROL, tc);
        }
    }
}
