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

import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Iterator;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CachePartitionIteratorTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean prefetchValues;

    @Parameterized.Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    private CachingProvider cachingProvider;
    private HazelcastInstance server;

    @Before
    public void init() {
        server = createHazelcastInstance();
        cachingProvider = createCachingProvider();
    }

    protected CachingProvider createCachingProvider() {
        return HazelcastServerCachingProvider.createCachingProvider(server);
    }

    private <K, V> CacheProxy<K, V> getCacheProxy() {
        String cacheName = randomString();
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheConfig<K, V> config = new CacheConfig<K, V>();
        config.getEvictionConfig().setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT).setSize(10000000);
        return (CacheProxy<K, V>) cacheManager.createCache(cacheName, config);

    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyPartition() throws Exception {
        CacheProxy<Integer, Integer> cache = getCacheProxy();
        Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator(10, 1, prefetchValues);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyPartition() throws Exception {
        CacheProxy<String, String> cache = getCacheProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        cache.put(key, value);

        Iterator<Cache.Entry<String, String>> iterator = cache.iterator(10, 1, prefetchValues);
        assertTrue(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition() throws Exception {
        CacheProxy<String, String> cache = getCacheProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        cache.put(key, value);

        Iterator<Cache.Entry<String, String>> iterator = cache.iterator(10, 1, prefetchValues);
        Cache.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition_and_HasNext_Returns_False_when_Item_Consumed() throws Exception {
        CacheProxy<String, String> cache = getCacheProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        cache.put(key, value);

        Iterator<Cache.Entry<String, String>> iterator = cache.iterator(10, 1, prefetchValues);
        Cache.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
        boolean hasNext = iterator.hasNext();
        assertFalse(hasNext);
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() throws Exception {
        CacheProxy<String, String> cache = getCacheProxy();
        String value = randomString();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(server, 42);
            cache.put(key, value);
        }
        Iterator<Cache.Entry<String, String>> iterator = cache.iterator(10, 42, prefetchValues);
        for (int i = 0; i < count; i++) {
            Cache.Entry entry = iterator.next();
            assertEquals(value, entry.getValue());

        }
    }
}
