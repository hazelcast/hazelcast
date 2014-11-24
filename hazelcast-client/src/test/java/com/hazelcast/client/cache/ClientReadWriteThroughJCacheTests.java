/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
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

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientReadWriteThroughJCacheTests extends HazelcastTestSupport {

    private static HazelcastInstance hz1;
    private static HazelcastInstance hz2;
    private static HazelcastInstance client;

    private static HazelcastServerCachingProvider cachingProvider1;
    private static HazelcastServerCachingProvider cachingProvider2;
    private static HazelcastClientCachingProvider cachingProvider3;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().addMember("127.0.0.1");

        hz1 = Hazelcast.newHazelcastInstance(config);
        hz2 = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1");
        client = HazelcastClient.newHazelcastClient(clientConfig);

        cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(hz1);
        cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(hz2);
        cachingProvider3 = HazelcastClientCachingProvider.createCachingProvider(client);
    }

    @AfterClass
    public static void tear() {
        cachingProvider1.close();
        cachingProvider2.close();
        cachingProvider3.close();
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test_getall_readthrough() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider3.getCacheManager();
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
                CacheManager cm1 = cachingProvider1.getCacheManager();
                assertNotNull(cm1.getCache(cacheName, Integer.class, Integer.class));
                CacheManager cm2 = cachingProvider2.getCacheManager();
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
    public void test_loadall_readthrough() throws Exception {
        final String cacheName = randomMapName();

        CacheManager cacheManager = cachingProvider3.getCacheManager();
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
                CacheManager cm1 = cachingProvider1.getCacheManager();
                assertNotNull(cm1.getCache(cacheName, Integer.class, Integer.class));
                CacheManager cm2 = cachingProvider2.getCacheManager();
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
            for (Integer key : keys ) {
                Integer value = load(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }
    }

}
