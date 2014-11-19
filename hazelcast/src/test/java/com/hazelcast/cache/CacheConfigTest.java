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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheConfigTest {

    private final URL configUrl1 = getClass().getClassLoader().getResource("test-hazelcast-jcache.xml");
    private final URL configUrl2 = getClass().getClassLoader().getResource("test-hazelcast-jcache2.xml");

    @Before
    @After
    public void cleanup() {
        HazelcastInstanceFactory.terminateAll();
        Caching.getCachingProvider().close();
    }

    @Test
    public void cacheConfigXmlTest() throws IOException {
        Config config1 = new XmlConfigBuilder(configUrl1).build();

        assertEquals("test-group1",config1.getGroupConfig().getName());
        assertEquals("test-pass1",config1.getGroupConfig().getPassword());

        CacheSimpleConfig cacheConfig1 = config1.getCacheConfig("cache1");
        assertEquals("com.hazelcast.cache.MyCacheLoaderFactory", cacheConfig1.getCacheLoaderFactory());
        assertEquals("com.hazelcast.cache.MyCacheWriterFactory", cacheConfig1.getCacheWriterFactory());
        assertEquals("com.hazelcast.cache.MyExpirePolicyFactory", cacheConfig1.getExpiryPolicyFactory());
        assertTrue(cacheConfig1.isReadThrough());
        assertTrue(cacheConfig1.isWriteThrough());
        assertTrue(cacheConfig1.isStatisticsEnabled());
        assertTrue(cacheConfig1.isManagementEnabled());

        assertNotNull(cacheConfig1.getMaxSizeConfig());
        assertEquals(50, cacheConfig1.getMaxSizeConfig().getSize());
        assertEquals(MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE,
                cacheConfig1.getMaxSizeConfig().getMaxSizePolicy());

        List<CacheSimpleEntryListenerConfig> cacheEntryListeners = cacheConfig1.getCacheEntryListeners();
        assertEquals(2, cacheEntryListeners.size());

        CacheSimpleEntryListenerConfig listenerConfig0 = cacheEntryListeners.get(0);
        assertFalse(listenerConfig0.isSynchronous());
        assertFalse(listenerConfig0.isOldValueRequired());
        assertEquals("com.hazelcast.cache.MyEntryListenerFactory",
                listenerConfig0.getCacheEntryListenerFactory());
        assertEquals("com.hazelcast.cache.MyEntryEventFilterFactory",
                listenerConfig0.getCacheEntryEventFilterFactory());

        CacheSimpleEntryListenerConfig listenerConfig1 = cacheEntryListeners.get(1);
        assertTrue(listenerConfig1.isSynchronous());
        assertTrue(listenerConfig1.isOldValueRequired());
        assertEquals("com.hazelcast.cache.MySyncEntryListenerFactory",
                listenerConfig1.getCacheEntryListenerFactory());
        assertEquals("com.hazelcast.cache.MySyncEntryEventFilterFactory",
                listenerConfig1.getCacheEntryEventFilterFactory());
    }

    @Test
    public void cacheManagerByLocationClasspathTest() throws URISyntaxException {
        URI uri1 = new URI("MY-SCOPE");
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION,
                "classpath:test-hazelcast-jcache.xml");
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri1, null, properties);
        assertNotNull(cacheManager);

        Cache<Integer, String> testCache = cacheManager.getCache("testCache", Integer.class, String.class);
        assertNotNull(testCache);
    }

    @Test
    public void cacheManagerByLocationFileTest() throws URISyntaxException {
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

        assertEquals(2, Hazelcast.getAllHazelcastInstances().size());
    }

    @Test
    public void cacheManagerByInstanceNameTest() throws URISyntaxException {
        final String instanceName= "instanceName66";
        Config config = new Config();
        config.setInstanceName(instanceName);
        Hazelcast.newHazelcastInstance(config);

        URI uri1 = new URI("MY-SCOPE");
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME, instanceName);
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri1, null, properties);
        assertNotNull(cacheManager);

        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
    }

    @Test
    public void defaultCacheTest() {
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();
        assertNotNull(cacheManager);

        Cache testCache = cacheManager.getCache("default");
        assertNull(testCache);
    }

    @Test
    public void testGetPreConfiguredCache() {
        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig().setName("test"));

        int count = 4;
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(count);
        for (int i = 0; i < count; i++) {
            HazelcastInstance instance = factory.newHazelcastInstance(config);
            CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(instance);
            CacheManager cacheManager = provider.getCacheManager();

            Cache<Object, Object> cache = cacheManager.getCache("test");
            Assert.assertNotNull("Pre-configured cache cannot be retrieved on instance: " + i, cache);
        }
    }
}
