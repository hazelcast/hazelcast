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

import com.hazelcast.cache.BasicCacheTest;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class ClientCacheTest
        extends HazelcastTestSupport {

//    private HazelcastInstance client;
    private CacheManager cacheManager;

    @Before
    public void init() {

        Config config = new Config();
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

//        ClientConfig clientConfig = new ClientConfig();

//        client = HazelcastClient.newHazelcastClient(clientConfig);

        cacheManager = Caching.getCachingProvider().getCacheManager();
    }

    @After
    public void onTearDown() {
        cacheManager.close();
    }

    @Test
    public void testSimple(){
        assertNotNull(cacheManager);

        CacheConfig<Integer,String> config =  new CacheConfig<Integer, String>();

        final Cache<Integer, String> cache = cacheManager.createCache("default", config);
        assertNotNull(cache);

        cache.put(1,"test");

        assertEquals("test", cache.get(1));


        final Cache<String, Object> cache2 = cacheManager.getCache("default2",String.class, Object.class);
        assertNull(cache2);
    }

    @Test
    public void testCompletionTest()
            throws InterruptedException {
        final String cacheName = "simpleCache";

        final CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        BasicCacheTest.SimpleEntryListener<Integer, String> listener = new BasicCacheTest.SimpleEntryListener<Integer, String>();
        final MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration =
                new MutableCacheEntryListenerConfiguration<Integer, String>(FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key1 = 1;
        String value1 = "value1";
        cache.put(key1, value1);
        assertEquals(1, listener.created.get());

        Integer key2 = 2;
        String value2 = "value2";
        cache.put(key2, value2);
        assertEquals(2, listener.created.get());

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(key1);
        keys.add(key2);
        cache.removeAll(keys);

        assertEquals(2, listener.removed.get());

        //        cacheManager.destroyCache(cacheName);
    }
}
