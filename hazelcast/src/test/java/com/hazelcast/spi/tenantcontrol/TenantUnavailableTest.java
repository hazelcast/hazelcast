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

import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.getCacheService;
import static com.hazelcast.cache.HazelcastCacheManager.CACHE_MANAGER_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * App Server reload / app not loaded tests
 *
 * @author lprimak
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TenantUnavailableTest extends TenantControlTestSupport {
    private String cacheName;
    private static final Set<String> disallowClassNames = new HashSet<>();
    private static final CountDownLatch latch = new CountDownLatch(1);
    private static boolean classLoadingFailed = false;

    @Before
    public void setup() {
        cacheName = randomName();
        initState();
    }

    @Test
    public void testOperationDelayedWhenTenantUnavailable() {
        tenantAvailable.set(false);
        HazelcastInstance hz = createHazelcastInstance(newConfig());
        IMap<String, Integer> map = hz.getMap(randomName());
        spawn(() -> map.put("key", 1));
        assertTrueEventually(() -> assertTrue(tenantAvailableCount.get() > 50));

        tenantAvailable.set(true);
        assertTrueEventually(() -> assertEquals((Integer) 1, map.get("key")));
    }

    @Test
    public void testCacheWithTypesWithoutClassLoader() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(newConfig());
        CacheConfig<KeyType, ValueType> cacheConfig = new CacheConfig<>();
        cacheConfig.setTypes(KeyType.class, ValueType.class);
        Cache<KeyType, ValueType> cache1 = createServerCachingProvider(hz1)
                .getCacheManager()
                .createCache(cacheName, cacheConfig);
        cache1.put(new KeyType(), new ValueType());
        assertInstanceOf(ValueType.class, cache1.get(new KeyType()));

        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfig(true, new SimulateNonExistantClassLoader()));
        ICacheService cacheService = getCacheService(hz2);
        disallowClassNames.add(KeyType.class.getName());
        hz1.shutdown(); // force migration
        CacheManager cacheManager = createServerCachingProvider(hz2).getCacheManager();
        Cache<KeyType, ValueType> cache2 = cacheManager.getCache(cacheName);
        disallowClassNames.clear();
        assertInstanceOf(ValueType.class, cache2.get(new KeyType()));

        destroyEventContext.get().tenantUnavailable();
        disallowClassNames.add(KeyType.class.getName());

        cacheConfig = cacheService.getCacheConfig(CACHE_MANAGER_PREFIX + cacheName);
        Cache<KeyType, ValueType> cache3 = cacheManager.getCache(cacheName);
        assertInstanceOf(ValueType.class, cache3.get(new KeyType()));
        Assert.assertFalse("Class Loading Failed", classLoadingFailed);
    }

    @Test
    public void testMigrationWithUnavailableClasses() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(newConfig());
        CacheConfig<String, ValueType> cacheConfig = new CacheConfig<>();
        cacheConfig.setTypes(String.class, ValueType.class);

        @SuppressWarnings({"unchecked", "rawtypes"})
        CacheProxy<String, ValueType> cache1 = (CacheProxy) createServerCachingProvider(hz1)
                .getCacheManager().createCache(cacheName, cacheConfig);
        ValueType value = new ValueType();

        cache1.put(generateKeyForPartition(hz1, 0), value);
        cache1.put(generateKeyForPartition(hz1, 1), value);

        disallowClassNames.add(ValueType.class.getName());
        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfig(true,
                new SimulateNonExistantClassLoader()).setLiteMember(true));
        hz1.getPartitionService().addMigrationListener(new MigrationListenerImpl());
        CacheManager cacheManager = createServerCachingProvider(hz2).getCacheManager();
        Cache<String, ValueType> cache2 = cacheManager.getCache(cacheName);
        // force migration
        hz2.getCluster().promoteLocalLiteMember();
        latch.await(); // await migration
        disallowClassNames.clear();
        Iterator<Cache.Entry<String, ValueType>> it2 = cache2.iterator();

        assertTrue("Iterator should not be empty", it2.hasNext());
        while (it2.hasNext()) {
            Cache.Entry<String, ValueType> entry = it2.next();
            assertInstanceOf(ValueType.class, entry.getValue());
        }
        Assert.assertFalse("Class Loading Failed", classLoadingFailed);
    }

    public static class SimulateNonExistantClassLoader extends URLClassLoader {
        public SimulateNonExistantClassLoader() {
            super(new URL[0], TenantControlTest.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (disallowClassNames.contains(name)) {
                classLoadingFailed = true;
                ExceptionUtil.sneakyThrow(new IllegalStateException(String.format("Unavailable Class %s", name)));
            }
            return super.loadClass(name, resolve);
        }
    }

    public static class KeyType implements Serializable {
    }

    public static class ValueType implements Serializable {
    }

    private static class MigrationListenerImpl implements MigrationListener {
        @Override
        public void migrationStarted(MigrationState state) {
        }

        @Override
        public void migrationFinished(MigrationState state) {
            latch.countDown();
        }

        @Override
        public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
        }

        @Override
        public void replicaMigrationFailed(ReplicaMigrationEvent event) {
        }
    }
}
