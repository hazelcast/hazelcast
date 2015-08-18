/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.ICache;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientCacheTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastClientCachingProvider cachingProvider;

    @Before
    public void init() {
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastClient();
        cachingProvider = HazelcastClientCachingProvider.createCachingProvider(hz);
    }

    @After
    public void tear() {
        cachingProvider.close();
        factory.shutdownAll();
    }


    @Test
    public void testRemoveAsync() throws ExecutionException, InterruptedException {
        String cacheName = randomString();

        ICache<String, String> cache = (ICache<String, String>) getCache(cacheName);

        String key = randomString();
        String value = randomString();
        cache.put(key, value);
        ICompletableFuture<Boolean> future = cache.removeAsync(key);
        assertTrue(future.get());
    }

    @Test
    public void testRemoveAsyncWhenEntryNotFound() throws ExecutionException, InterruptedException {
        String cacheName = randomString();

        ICache<String, String> cache = (ICache<String, String>) getCache(cacheName);

        ICompletableFuture<Boolean> future = cache.removeAsync(randomString());
        assertFalse(future.get());
    }

    @Test
    public void testRemoveAsync_withOldValue() throws ExecutionException, InterruptedException {
        String cacheName = randomString();

        ICache<String, String> cache = (ICache<String, String>) getCache(cacheName);

        String key = randomString();
        String value = randomString();
        cache.put(key, value);
        ICompletableFuture<Boolean> future = cache.removeAsync(key, value);
        assertTrue(future.get());
    }

    @Test
    public void testRemoveAsyncWhenEntryNotFound_withOldValue() throws ExecutionException, InterruptedException {
        String cacheName = randomString();
        ICache<String, String> cache = (ICache<String, String>) getCache(cacheName);
        ICompletableFuture<Boolean> future = cache.removeAsync(randomString(), randomString());
        assertFalse(future.get());

    }

    private Cache<String, String> getCache(String cacheName) {
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheConfig<String, String> config = new CacheConfig<String, String>();
        return cacheManager.createCache(cacheName, config);
    }

}

