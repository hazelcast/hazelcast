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
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListener;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientReadWriteThroughJCacheTests extends HazelcastTestSupport {

    private TestHazelcastFactory testHazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance server1;
    private HazelcastInstance server2;
    private HazelcastInstance client;

    private HazelcastServerCachingProvider serverProvider1;
    private HazelcastServerCachingProvider serverProvider2;
    private HazelcastClientCachingProvider clientProvider;

    @Before
    public void setup() {
        server1 = testHazelcastFactory.newHazelcastInstance();
        server2 = testHazelcastFactory.newHazelcastInstance();

        client = testHazelcastFactory.newHazelcastClient();

        serverProvider1 = HazelcastServerCachingProvider.createCachingProvider(server1);
        serverProvider2 = HazelcastServerCachingProvider.createCachingProvider(server2);
        clientProvider = HazelcastClientCachingProvider.createCachingProvider(client);
    }

    @After
    public void tearDown() {
        clientProvider.close();
        serverProvider1.close();
        serverProvider2.close();
        testHazelcastFactory.shutdownAll();
    }

    @Test
    public void getAllShouldReadThroughCacheLoaderSuccessfully() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = clientProvider.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, Integer> config = new CacheConfig<Integer, Integer>()
                .setTypes(Integer.class, Integer.class)
                .setReadThrough(true)
                .setCacheLoaderFactory(FactoryBuilder.factoryOf(new GetAllAsyncCacheLoader()));

        Cache<Integer, Integer> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm1 = serverProvider1.getCacheManager();
                assertNotNull(cm1.getCache(cacheName, Integer.class, Integer.class));
                CacheManager cm2 = serverProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName, Integer.class, Integer.class));
            }
        });

        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < 150; i++) {
            keys.add(i);
        }

        Map<Integer, Integer> loaded = cache.getAll(keys);
        assertEquals(100, loaded.size());
    }

    @Test
    public void loadAllShouldReadThroughCacheLoaderSuccessfully() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = clientProvider.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CompleteConfiguration<Integer, Integer> config = new CacheConfig<Integer, Integer>()
                .setTypes(Integer.class, Integer.class)
                .setReadThrough(true)
                .setCacheLoaderFactory(FactoryBuilder.factoryOf(new GetAllAsyncCacheLoader()));

        Cache<Integer, Integer> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CacheManager cm1 = serverProvider1.getCacheManager();
                assertNotNull(cm1.getCache(cacheName, Integer.class, Integer.class));
                CacheManager cm2 = serverProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName, Integer.class, Integer.class));
            }
        });

        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < 150; i++) {
            keys.add(i);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        cache.loadAll(keys, false, new CompletionListener() {
            @Override
            public void onCompletion() {
                latch.countDown();
            }

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
                latch.countDown();
            }
        });
        latch.await();
        assertEquals(100, cache.unwrap(ICache.class).size());
    }

    public static class GetAllAsyncCacheLoader implements CacheLoader<Integer, Integer>, Serializable {

        @Override
        public Integer load(Integer key) {
            return key != null && key < 100 ? key : null;
        }

        @Override
        public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            Map<Integer, Integer> result = new HashMap<Integer, Integer>();
            for (Integer key : keys) {
                Integer value = load(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }
    }

    // https://github.com/hazelcast/hazelcast/issues/6676
    @Test
    public void cacheLoaderShouldBeAbleToUsedOnlyAtServerSide() {
        final String cacheName = randomName();
        CacheManager serverCacheManager = serverProvider1.getCacheManager();

        CompleteConfiguration<Integer, String> config =
                new CacheConfig<Integer, String>()
                        .setTypes(Integer.class, String.class)
                        .setReadThrough(true)
                        .setCacheLoaderFactory(new ServerSideCacheLoaderFactory());

        serverCacheManager.createCache(cacheName, config);

        CacheManager clientCacheManager = clientProvider.getCacheManager();
        Cache<Integer, String> cache = clientCacheManager.getCache(cacheName, Integer.class, String.class);

        assertNotNull(cache);

        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            keys.add(i);
        }

        Map<Integer, String> loaded = cache.getAll(keys);
        assertEquals(keys.size(), loaded.size());
        for (Map.Entry<Integer, String> entry  : loaded.entrySet()) {
            assertEquals(ServerSideCacheLoader.valueOf(entry.getKey()), entry.getValue());
        }
    }

    public static class ServerSideCacheLoaderFactory
            implements Factory<ServerSideCacheLoader>, HazelcastInstanceAware {

        private transient HazelcastInstance hazelcastInstance;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public ServerSideCacheLoader create() {
            if (hazelcastInstance instanceof HazelcastInstanceImpl) {
                return new ServerSideCacheLoader();
            } else {
                throw new IllegalStateException("This factory can only be used at server side!");
            }
        }

    }

    private static class ServerSideCacheLoader implements CacheLoader<Integer, String> {

        static String valueOf(Integer key) {
            return "value-of-" + key;
        }

        @Override
        public String load(Integer key) {
            return valueOf(key);
        }

        @Override
        public Map<Integer, String> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            Map<Integer, String> result = new HashMap<Integer, String>();
            for (Integer key : keys) {
                String value = load(key);
                result.put(key, value);
            }
            return result;
        }

    }

}
