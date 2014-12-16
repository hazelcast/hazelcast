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

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientCacheConfigTest {

    private final URL configUrl1 = getClass().getClassLoader().getResource("hazelcast-client-c1.xml");
    private final URL configUrl2 = getClass().getClassLoader().getResource("hazelcast-client-c2.xml");
    private final CacheSimpleConfig simpleConfig = new CacheSimpleConfig().setName("pre-configured-cache");

    @BeforeClass
    public static void setupClass() {
        HazelcastClient.shutdownAll();
    }

    @Before
    public void init() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().setPort(5701);
        config.getGroupConfig().setName("cluster1");
        config.getGroupConfig().setPassword("cluster1pass");
        config.addCacheConfig(simpleConfig);
        Hazelcast.newHazelcastInstance(config);

        Config config2 = new Config();
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().setPort(5702);
        config2.getGroupConfig().setName("cluster2");
        config2.getGroupConfig().setPassword("cluster2pass");
        config.addCacheConfig(simpleConfig);
        Hazelcast.newHazelcastInstance(config2);
    }

    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void cacheManagerByLocationClasspathTest()
            throws URISyntaxException {
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size() );
        URI uri1 = new URI("MY-SCOPE");

        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, "classpath:hazelcast-client-c1.xml");
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri1, null, properties);
        assertNotNull(cacheManager);

        assertEquals(1, HazelcastClient.getAllHazelcastClients().size() );
        Caching.getCachingProvider().close();

        cacheManager.close();

        assertEquals(0, HazelcastClient.getAllHazelcastClients().size() );

    }

    @Test
    public void cacheManagerByLocationFileTest()
            throws URISyntaxException {
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size() );
        URI uri = new URI("MY-SCOPE");

        String urlStr = configUrl1.toString();
        assertEquals("file", urlStr.substring(0,4));
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, urlStr);
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri, null, properties);
        assertNotNull(cacheManager);

        URI uri2 = new URI("MY-SCOPE-OTHER");
        String urlStr2 = configUrl2.toString();
        assertEquals("file", urlStr2.substring(0,4));
        Properties properties2 = new Properties();
        properties2.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, urlStr2);
        CacheManager cacheManager2 = Caching.getCachingProvider().getCacheManager(uri2, null, properties2);
        assertNotNull(cacheManager2);

        assertEquals(2, HazelcastClient.getAllHazelcastClients().size());
        Caching.getCachingProvider().close();
    }

    @Test
    public void cacheManagerByInstanceNameTest()
            throws URISyntaxException {
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size() );
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("cluster1");
        clientConfig.getGroupConfig().setPassword("cluster1pass");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        String instanceName =client.getName();

        URI uri1 = new URI("MY-SCOPE");
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME, instanceName);
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri1, null, properties);
        assertNotNull(cacheManager);

        assertEquals(1, HazelcastClient.getAllHazelcastClients().size() );
        client.shutdown();

        Caching.getCachingProvider().close();
    }

    @Test
    public void testGetPreConfiguredCache() {
        ClientConfig config = new ClientConfig();
        config.getGroupConfig().setName("cluster1");
        config.getGroupConfig().setPassword("cluster1pass");

        for (int i = 0; i < 4; i++) {
            HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
            CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
            CacheManager cacheManager = provider.getCacheManager();

            Cache<Object, Object> cache = cacheManager.getCache(simpleConfig.getName());
            Assert.assertNotNull("Cache cannot be retrieved on client: " + i, cache);
        }
    }
}
