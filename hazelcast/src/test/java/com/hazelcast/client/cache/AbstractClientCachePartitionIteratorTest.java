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

package com.hazelcast.client.cache;

import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Test;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Iterator;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public abstract class AbstractClientCachePartitionIteratorTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean prefetchValues;

    @Parameterized.Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    protected TestHazelcastFactory factory;
    protected CachingProvider cachingProvider;
    protected HazelcastInstance server;

    @After
    public void teardown() {
        factory.terminateAll();
    }

    private <T> Iterator<Cache.Entry<T, T>> getIterator(ICacheInternal<T, T> cache) {
        return cache.iterator(10, 1, prefetchValues);
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyPartition() throws Exception {
        ICacheInternal<Integer, Integer> cache = getCacheProxy();
        Iterator<Cache.Entry<Integer, Integer>> iterator = getIterator(cache);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyPartition() throws Exception {
        ICacheInternal<String, String> cache = getCacheProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        cache.put(key, value);

        Iterator<Cache.Entry<String, String>> iterator = getIterator(cache);
        assertTrue(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition() throws Exception {
        ICacheInternal<String, String> cache = getCacheProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        cache.put(key, value);

        Iterator<Cache.Entry<String, String>> iterator = getIterator(cache);
        Cache.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition_and_HasNext_Returns_False_when_Item_Consumed() throws Exception {
        ICacheInternal<String, String> cache = getCacheProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        cache.put(key, value);

        Iterator<Cache.Entry<String, String>> iterator = getIterator(cache);
        Cache.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
        boolean hasNext = iterator.hasNext();
        assertFalse(hasNext);
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() throws Exception {
        ICacheInternal<String, String> cache = getCacheProxy();
        String value = randomString();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(server, 1);
            cache.put(key, value);
        }
        Iterator<Cache.Entry<String, String>> iterator = getIterator(cache);
        for (int i = 0; i < count; i++) {
            Cache.Entry entry = iterator.next();
            assertEquals(value, entry.getValue());
        }
    }

    private <K, V> ICacheInternal<K, V> getCacheProxy() {
        String cacheName = randomString();
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheConfig<K, V> config = new CacheConfig<K, V>();
        config.getEvictionConfig().setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT).setSize(10000000);
        return (ICacheInternal<K, V>) cacheManager.createCache(cacheName, config);
    }
}
