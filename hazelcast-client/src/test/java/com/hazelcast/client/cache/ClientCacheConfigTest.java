/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
        URI uri1 = new URI("MY-SCOPE");

        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, "classpath:hazelcast-client-c1.xml");
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri1, null, properties);
        assertNotNull(cacheManager);

        assertEquals(1, HazelcastClient.getAllHazelcastClients().size());
        Caching.getCachingProvider().close();

        cacheManager.close();

        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
    }

    @Test
    public void cacheManagerByLocationFileTest()
            throws URISyntaxException {
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
        URI uri = new URI("MY-SCOPE");

        String urlStr = configUrl1.toString();
        assertEquals("file", urlStr.substring(0, 4));
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, urlStr);
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri, null, properties);
        assertNotNull(cacheManager);

        URI uri2 = new URI("MY-SCOPE-OTHER");
        String urlStr2 = configUrl2.toString();
        assertEquals("file", urlStr2.substring(0, 4));
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
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
        String instanceName = "ClientInstanceTest";

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("cluster1");
        clientConfig.getGroupConfig().setPassword("cluster1pass");
        clientConfig.setInstanceName(instanceName);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        assertEquals(instanceName, client.getName());

        URI uri1 = new URI("MY-SCOPE");
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME, instanceName);
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri1, null, properties);
        assertNotNull(cacheManager);

        assertEquals(1, HazelcastClient.getAllHazelcastClients().size());
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

    @Test
    public void createCacheConfigOnAllNodes() {
        final String CACHE_NAME = "myCache";

        HazelcastInstance client = null;
        HazelcastInstance server1 = null;
        HazelcastInstance server2 = null;

        try {
            Config config = new Config();
            CacheSimpleConfig cacheSimpleConfig =
                    new CacheSimpleConfig()
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

    private ICacheService getCacheService(HazelcastInstance instance) {
        Node node = TestUtil.getNode(instance);
        return node.getNodeEngine().getService(ICacheService.SERVICE_NAME);
    }

}
