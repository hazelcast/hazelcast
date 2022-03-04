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

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.jsr.JsrClientTestUtil;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.JarUtil;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.TestEnvironment.isSolaris;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.util.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientCacheConfigTest extends HazelcastTestSupport {

    private final URL configUrl1 = getClass().getClassLoader().getResource("hazelcast-client-c1.xml");
    private final URL configUrl2 = getClass().getClassLoader().getResource("hazelcast-client-c2.xml");
    private final CacheSimpleConfig simpleConfig = new CacheSimpleConfig().setName("pre-configured-cache");

    @Before
    public void init() {
        JsrClientTestUtil.setup();

        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().setPort(5701);
        config.setClusterName("cluster1");
        config.addCacheConfig(simpleConfig);
        Hazelcast.newHazelcastInstance(config);

        Config config2 = new Config();
        config2.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config2.getNetworkConfig().setPort(5702);
        config2.setClusterName("cluster2");
        config.addCacheConfig(simpleConfig);
        Hazelcast.newHazelcastInstance(config2);
    }

    @After
    public void tearDown() {
        JsrClientTestUtil.cleanup();
    }

    @Test
    public void cacheManagerByLocationClasspathTest() throws Exception {
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
        URI uri = new URI("MY-SCOPE");

        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, "classpath:hazelcast-client-c1.xml");
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri, null, properties);
        assertNotNull(cacheManager);

        assertEquals(1, HazelcastClient.getAllHazelcastClients().size());

        Caching.getCachingProvider().close();
        cacheManager.close();

        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
    }

    @Test
    public void cacheCacheManagerByLocationJarFileTest() throws Exception {
        File jcacheConfigFile = File.createTempFile("jcache_config_", ".jar");
        JarUtil.createJarFile(
                "src/test/resources/",
                newArrayList("hazelcast-client-c1.xml"),
                jcacheConfigFile.getAbsolutePath()
        );

        URI uri = new URI("jar:" + jcacheConfigFile.toURI() + "!/hazelcast-client-c1.xml");
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri, null, new Properties());
        assertThat(cacheManager).isNotNull();

        HazelcastClientCacheManager clientCacheManager = cacheManager.unwrap(HazelcastClientCacheManager.class);
        assertThat(clientCacheManager.getHazelcastInstance().getName()).isEqualTo("client-cluster1");
    }

    @Test
    public void cacheManagerByLocationFileTest() throws Exception {
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
    }

    @Test
    public void cacheManagerByInstanceNameTest() throws URISyntaxException {
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
        String instanceName = "ClientInstanceTest";

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getAutoDetectionConfig().setEnabled(false);
        clientConfig.setClusterName("cluster1");
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
    }

    @Test
    public void testGetPreConfiguredCache() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getAutoDetectionConfig().setEnabled(false);
        config.setClusterName("cluster1");

        for (int i = 0; i < 4; i++) {
            HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
            CachingProvider provider = createClientCachingProvider(client);
            CacheManager cacheManager = provider.getCacheManager();

            Cache<Object, Object> cache = cacheManager.getCache(simpleConfig.getName());
            assertNotNull("Cache cannot be retrieved on client: " + i, cache);
        }
    }

    @Test
    public void createCacheConfigOnAllNodes() {
        final String CACHE_NAME = "myCache";

        HazelcastInstance client = null;
        HazelcastInstance server1 = null;
        HazelcastInstance server2 = null;

        try {
            Config config = new Config().setClusterName(getTestMethodName());
            if (isSolaris()) {
                config.setProperty(ClusterProperty.MULTICAST_SOCKET_SET_INTERFACE.getName(), "false");
            }
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
            ClientConfig clientConfig = new ClientConfig().setClusterName(getTestMethodName());
            client = HazelcastClient.newHazelcastClient(clientConfig);

            // Create the client cache manager
            CachingProvider cachingProvider = createClientCachingProvider(client);
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
