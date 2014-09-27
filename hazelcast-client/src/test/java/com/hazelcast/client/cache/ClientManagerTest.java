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

import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientManagerTest {

    final URI uri1 = new File("hazelcast-client/src/test/java/com/hazelcast/client/cache/hazelcast-client-c1.xml").toURI();
    final URI uri2 = new File("hazelcast-client/src/test/java/com/hazelcast/client/cache/hazelcast-client-c2.xml").toURI();

    @Before
    public void init() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().setPort(5701);
        config.getGroupConfig().setName("cluster1");
        config.getGroupConfig().setPassword("cluster1pass");

        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);

        Config config2 = new Config();
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().setPort(5702);
        config2.getGroupConfig().setName("cluster2");
        config2.getGroupConfig().setPassword("cluster2pass");

        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config2);
    }

    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMultiClusterMultipleClients()
            throws MalformedURLException, URISyntaxException {
        final String cacheName = "test";
        final String key1 = "key1";
        final String valuecm1 = "Value-is-cm1";
        final String valuecm2 = "Value-is-cm2";

        final HazelcastClientCachingProvider cachingProvider = new HazelcastClientCachingProvider();
        final CacheManager cm1 = cachingProvider.getCacheManager(uri1, null);
        final CacheManager cm2 = cachingProvider.getCacheManager(uri2, null);
        final CacheConfig<String, String> cacheConfig = new CacheConfig<String, String>();
        final Cache<String, String> cache1 = cm1.createCache(cacheName, cacheConfig);
        final Cache<String, String> cache2 = cm2.createCache(cacheName, cacheConfig);

        cache1.put(key1, valuecm1);
        cache2.put(key1, valuecm2);

        assertEquals(valuecm1, cache1.get(key1));
        assertEquals(valuecm2, cache2.get(key1));

        cachingProvider.close(uri1, null);
        cachingProvider.close(uri2, null);
//        cm1.close();
//        cm2.close();

        final CacheManager cm11 = cachingProvider.getCacheManager(uri1, null);

        final Cache<String, String> cache11 = cm11.getCache(cacheName);

        assertEquals(valuecm1, cache11.get(key1));

        cm11.close();
    }
}
