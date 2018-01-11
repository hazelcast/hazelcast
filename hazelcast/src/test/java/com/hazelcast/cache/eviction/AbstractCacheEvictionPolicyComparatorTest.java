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

package com.hazelcast.cache.eviction;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheEvictionPolicyComparator;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.test.HazelcastTestSupport;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public abstract class AbstractCacheEvictionPolicyComparatorTest extends HazelcastTestSupport {

    protected static final String CACHE_NAME = "MyCache";

    abstract protected CachingProvider createCachingProvider(HazelcastInstance instance);

    abstract protected HazelcastInstance createInstance(Config config);

    abstract protected ConcurrentMap getUserContext(HazelcastInstance hazelcastInstance);

    protected Config createConfig() {
        return new Config();
    }

    protected CacheConfig<Integer, String> createCacheConfig(String cacheName) {
        return new CacheConfig<Integer, String>(cacheName);
    }

    void testEvictionPolicyComparator(EvictionConfig evictionConfig, int iterationCount) {
        HazelcastInstance instance = createInstance(createConfig());
        CachingProvider cachingProvider = createCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheConfig<Integer, String> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setEvictionConfig(evictionConfig);
        Cache<Integer, String> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        for (int i = 0; i < iterationCount; i++) {
            cache.put(i, "Value-" + i);
        }

        AtomicLong callCounter = (AtomicLong) getUserContext(instance).get("callCounter");
        assertTrue(callCounter.get() > 0);
    }

    public static class MyEvictionPolicyComparator
            extends CacheEvictionPolicyComparator<Integer, String>
            implements HazelcastInstanceAware {

        private final AtomicLong callCounter = new AtomicLong();

        @Override
        public int compare(CacheEntryView<Integer, String> e1, CacheEntryView<Integer, String> e2) {
            Integer key1 = e1.getKey();
            String value1 = e1.getValue();

            assertNotNull(key1);
            assertNotNull(value1);
            assertEquals("Value-" + key1, value1);
            assertTrue(e1.getCreationTime() > 0);
            assertEquals(CacheRecord.TIME_NOT_AVAILABLE, e1.getLastAccessTime());
            assertEquals(0, e1.getAccessHit());

            Integer key2 = e2.getKey();
            String value2 = e2.getValue();

            assertNotNull(key2);
            assertNotNull(value2);
            assertEquals("Value-" + key2, value2);
            assertTrue(e2.getCreationTime() > 0);
            assertEquals(CacheRecord.TIME_NOT_AVAILABLE, e2.getLastAccessTime());
            assertEquals(0, e2.getAccessHit());

            callCounter.incrementAndGet();

            return CacheEvictionPolicyComparator.BOTH_OF_ENTRIES_HAVE_SAME_PRIORITY_TO_BE_EVICTED;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            hazelcastInstance.getUserContext().put("callCounter", callCounter);
        }
    }
}
