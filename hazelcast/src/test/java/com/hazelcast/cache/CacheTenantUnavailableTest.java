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

import static com.hazelcast.cache.CachePartitionIteratorMigrationTest.putValuesToPartition;
import static com.hazelcast.cache.CacheTenantControlTest.destroyEventContext;
import static com.hazelcast.cache.CacheTenantControlTest.classesAlwaysAvailable;
import static com.hazelcast.cache.CacheTenantControlTest.initState;
import static com.hazelcast.cache.CacheTenantControlTest.newConfig;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.getCacheService;
import static com.hazelcast.cache.HazelcastCacheManager.CACHE_MANAGER_PREFIX;
import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.CacheManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * App Server reload / app not loaded tests
 *
 * @author lprimak
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheTenantUnavailableTest extends HazelcastTestSupport {
    private String cacheName;
    private static final Set<String> disallowClassNames = new HashSet<>();
    private static final CountDownLatch latch = new CountDownLatch(1);
    private static boolean classLoadingFailed;

    @Before
    public void setup() {
        cacheName = randomName();
        classLoadingFailed = false;
        initState();
        classesAlwaysAvailable = false;
    }

    @Test
    public void testCacheWithTypesWithoutClassLoader() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(newConfig());
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setTypes(KeyType.class, ValueType.class);
        Cache cache1 = createServerCachingProvider(hz1).getCacheManager().createCache(cacheName, cacheConfig);
        cache1.put(new KeyType(), new ValueType());
        assertInstanceOf(ValueType.class, cache1.get(new KeyType()));

        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfig());
        ICacheService cacheService = getCacheService(hz2);
        disallowClassNames.add(KeyType.class.getName());
        hz1.shutdown(); // force migration
        CacheManager cacheManager = createServerCachingProvider(hz2).getCacheManager();
        Cache cache2 = cacheManager.getCache(cacheName);
        disallowClassNames.clear();
        assertInstanceOf(ValueType.class, cache2.get(new KeyType()));

        destroyEventContext.get().tenantUnavailable();
        disallowClassNames.add(KeyType.class.getName());

        cacheConfig = cacheService.getCacheConfig(CACHE_MANAGER_PREFIX + cacheName);
        Cache cache3 = cacheManager.getCache(cacheName);
        assertInstanceOf(ValueType.class, cache3.get(new KeyType()));
        Assert.assertFalse("Class Loading Failed", classLoadingFailed);
    }

    @Test
    public void testMigrationWithUnavailableClasses() throws InterruptedException {
        classesAlwaysAvailable = false;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(newConfig());
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setTypes(String.class, ValueType.class);

        CacheProxy<String, ValueType> cache1 = (CacheProxy<String, ValueType>) createServerCachingProvider(hz1)
                .getCacheManager().createCache(cacheName, cacheConfig);
        ValueType value = new ValueType();
        putValuesToPartition(hz1, cache1, value, 0, 1);
        putValuesToPartition(hz1, cache1, value, 1, 1);

        disallowClassNames.add(ValueType.class.getName());
        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfig().setLiteMember(true));
        hz1.getPartitionService().addMigrationListener(new MigrationListenerImpl());
        CacheManager cacheManager = createServerCachingProvider(hz2).getCacheManager();
        Cache cache2 = cacheManager.getCache(cacheName);
        // force migration
        hz2.getCluster().promoteLocalLiteMember();
        latch.await(); // await migration
        disallowClassNames.clear();
        Iterator it2 = cache2.iterator();
        Assert.assertNotNull("Iterator should not be empty", it2.hasNext());
        while (it2.hasNext()) {
            Cache.Entry<String, ValueType> entry = (Cache.Entry<String, ValueType>) it2.next();
            assertInstanceOf(ValueType.class, entry.getValue());
        }
        Assert.assertFalse("Class Loading Failed", classLoadingFailed);
    }

    public static class SimulateNonExistantClassLoader extends URLClassLoader {
        public SimulateNonExistantClassLoader() {
            super(new URL[0], CacheTenantControlTest.class.getClassLoader());
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
