/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.cache;

import static com.hazelcast.cache.CacheTenantControlTest.destroyEventContext;
import static com.hazelcast.cache.CacheTenantControlTest.setTenantCount;
import static com.hazelcast.cache.HazelcastCacheManager.CACHE_MANAGER_PREFIX;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfigAccessor;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.AdditionalServiceClassLoader;
import com.hazelcast.util.ExceptionUtil;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.CacheManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * App Server reload / app not loaded tests
 *
 * @author lprimak
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheTenantUnavailableTest extends HazelcastTestSupport {
    private Config config;
    private String cacheName;
    private static final Set<String> disallowClassNames = new HashSet<>();

    @Before
    public void setup() {
        cacheName = randomName();
        config = new Config();
        ClassLoader configClassLoader = new AdditionalServiceClassLoader(new URL[0],
                new SimulateNonExistantClassLoader());
        config.setClassLoader(configClassLoader);
        config.getCacheConfig("*");
        setTenantCount.set(0);
        disallowClassNames.clear();
    }

    @Test
    public void testCacheWithTypesWithoutClassLoader() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setTypes(KeyType.class, ValueType.class);
        Cache cache1 = HazelcastServerCachingProvider.createCachingProvider(hz1).getCacheManager().createCache(cacheName, cacheConfig);
        cache1.put(new KeyType(), new ValueType());
        assertInstanceOf(ValueType.class, cache1.get(new KeyType()));

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz2);
        CacheService cacheService = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
        disallowClassNames.add(KeyType.class.getName());
        hz1.shutdown(); // force migration
        CacheManager cacheManager = HazelcastServerCachingProvider.createCachingProvider(hz2).getCacheManager();
        Cache cache2 = cacheManager.getCache(cacheName);
        disallowClassNames.clear();
        assertInstanceOf(ValueType.class, cache2.get(new KeyType()));

        destroyEventContext.get().destroy(cache2);
        disallowClassNames.add(KeyType.class.getName());

        cacheConfig = cacheService.getCacheConfig(CACHE_MANAGER_PREFIX + cacheName);
        TenantControl tenantControl = CacheConfigAccessor.getTenantControl(cacheConfig);
        cacheService.setTenantControl(cacheConfig);
        Cache cache3 = cacheManager.getCache(cacheName);
        assertInstanceOf(ValueType.class, cache3.get(new KeyType()));
    }


    public static class SimulateNonExistantClassLoader extends URLClassLoader {
        public SimulateNonExistantClassLoader() {
            super(new URL[0], CacheTenantControlTest.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (disallowClassNames.contains(name)) {
                ExceptionUtil.sneakyThrow(new IllegalStateException(String.format("Unavailable Class %s", name)));
            }
            return super.loadClass(name, resolve);
        }
    }

    public static class KeyType implements Serializable {
    }

    public static class ValueType implements Serializable {
    }
}
