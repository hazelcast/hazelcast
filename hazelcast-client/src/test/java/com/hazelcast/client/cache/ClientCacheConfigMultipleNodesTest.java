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

package com.hazelcast.client.cache;

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.cache.jsr.JsrClientTestUtil;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientCacheConfigMultipleNodesTest {

    @Before
    public void init() {
        JsrClientTestUtil.setup();
    }

    @After
    public void tearDown() {
        JsrClientTestUtil.cleanup();
    }

    @Test
    public void createCacheConfigOnAllNodes() {
        final String CACHE_NAME = "myCache";

        HazelcastInstance client = null;
        HazelcastInstance server1 = null;
        HazelcastInstance server2 = null;

        try {
            Config config = new Config();
            CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                    .setName(CACHE_NAME)
                    .setBackupCount(1); // Be sure that cache put operation is mirrored to backup node
            config.addCacheConfig(cacheSimpleConfig);

            // Create servers with configured caches
            server1 = Hazelcast.newHazelcastInstance(config);
            server2 = Hazelcast.newHazelcastInstance(config);

            ICacheService cacheService1 = getCacheService(server1);
            ICacheService cacheService2 = getCacheService(server2);

            // Create the hazelcast client instance
            client = HazelcastClient.newHazelcastClient();

            // Create the client cache manager
            CachingProvider cachingProvider =
                    HazelcastClientCachingProvider.createCachingProvider(client);
            CacheManager cacheManager = cachingProvider.getCacheManager();

            Cache<String, String> cache = cacheManager.getCache(CACHE_NAME);

            assertNotNull(cache);

            CacheConfig cacheConfig = cache.getConfiguration(CacheConfig.class);

            assertNotNull(cacheService1.getCacheConfig(cacheConfig.getNameWithPrefix()));
            assertNotNull(cacheService2.getCacheConfig(cacheConfig.getNameWithPrefix()));

            // First attempt to use the cache will trigger to create its record store.
            // So, we are testing also this case. There should not be any exception.
            // In here, we are testing both of nodes since there is a backup,
            // put is also applied to other (backup node).
            cache.put("key", "value");
        } finally {
            if (client != null) {
                client.shutdown();
            }
            if (server1 != null) {
                server1.shutdown();
            }
            if (server2 != null) {
                server2.shutdown();
            }
        }
    }

    private static ICacheService getCacheService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(ICacheService.SERVICE_NAME);
    }
}
