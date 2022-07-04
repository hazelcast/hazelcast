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

package com.hazelcast.config;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.JarUtil;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.spi.CachingProvider;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.util.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheConfigTest extends HazelcastTestSupport {

    private final URL configUrl1 = getClass().getClassLoader().getResource("test-hazelcast-jcache.xml");
    private final URL configUrl2 = getClass().getClassLoader().getResource("test-hazelcast-jcache2.xml");

    @Before
    public void setUp() {
        JsrTestUtil.setup();
    }

    @After
    public void cleanup() {
        HazelcastInstanceFactory.terminateAll();
        JsrTestUtil.cleanup();
    }

    @Test
    public void testHashCode() {
        CacheConfig cacheConfig = new CacheConfig();
        assertTrue(cacheConfig.hashCode() != 0);
    }

    @Test
    public void testCacheConfigLoaderWriterXml() throws Exception {
        Config config = new XmlConfigBuilder(configUrl2).build();

        CacheSimpleConfig simpleConfig = config.getCacheConfig("cache3");
        CacheConfig<Object, String> cacheConfig = new CacheConfig<Object, String>(simpleConfig);

        Factory<CacheWriter<? super Object, ? super String>> writerFactory = cacheConfig.getCacheWriterFactory();
        CacheWriter<? super Object, ? super String> cacheWriter = writerFactory.create();
        assertTrue(cacheWriter instanceof EmptyCacheWriter);

        Factory<CacheLoader<Object, String>> loaderFactory = cacheConfig.getCacheLoaderFactory();
        CacheLoader<Object, String> cacheLoader = loaderFactory.create();
        assertTrue(cacheLoader instanceof MyCacheLoader);
    }

    @Test
    public void testCacheConfigLoaderWriter() throws Exception {
        CacheSimpleConfig simpleConfig = new CacheSimpleConfig();
        simpleConfig.setCacheLoader(MyCacheLoader.class.getName());
        simpleConfig.setCacheWriter(EmptyCacheWriter.class.getName());

        CacheConfig cacheConfig = new CacheConfig(simpleConfig);

        CacheLoader loader = (CacheLoader) cacheConfig.getCacheLoaderFactory().create();
        CacheWriter writer = (CacheWriter) cacheConfig.getCacheWriterFactory().create();

        assertTrue(loader instanceof MyCacheLoader);
        assertTrue(writer instanceof EmptyCacheWriter);
    }

    @Test
    public void cacheConfigXmlTest() throws IOException {
        Config config1 = new XmlConfigBuilder(configUrl1).build();

        assertEquals("test-cluster1", config1.getClusterName());

        CacheSimpleConfig cacheConfig1 = config1.getCacheConfig("cache1");
        assertEquals("com.hazelcast.config.CacheConfigTest$MyCacheLoaderFactory",
                cacheConfig1.getCacheLoaderFactory());
        assertEquals("com.hazelcast.config.CacheConfigTest$MyCacheWriterFactory",
                cacheConfig1.getCacheWriterFactory());
        assertEquals("com.hazelcast.config.CacheConfigTest$MyExpirePolicyFactory",
                cacheConfig1.getExpiryPolicyFactoryConfig().getClassName());
        assertTrue(cacheConfig1.isReadThrough());
        assertTrue(cacheConfig1.isWriteThrough());
        assertTrue(cacheConfig1.isStatisticsEnabled());
        assertTrue(cacheConfig1.isManagementEnabled());

        EvictionConfig evictionConfig = cacheConfig1.getEvictionConfig();
        assertNotNull(evictionConfig);
        assertEquals(EvictionPolicy.LFU, evictionConfig.getEvictionPolicy());
        assertEquals(50, evictionConfig.getSize());
        assertEquals(MaxSizePolicy.ENTRY_COUNT, evictionConfig.getMaxSizePolicy());

        List<CacheSimpleEntryListenerConfig> cacheEntryListeners = cacheConfig1.getCacheEntryListeners();
        assertEquals(2, cacheEntryListeners.size());

        CacheSimpleEntryListenerConfig listenerConfig0 = cacheEntryListeners.get(0);
        assertFalse(listenerConfig0.isSynchronous());
        assertFalse(listenerConfig0.isOldValueRequired());
        assertEquals("com.hazelcast.config.CacheConfigTest$MyEntryListenerFactory",
                listenerConfig0.getCacheEntryListenerFactory());
        assertEquals("com.hazelcast.config.CacheConfigTest$MyEntryEventFilterFactory",
                listenerConfig0.getCacheEntryEventFilterFactory());

        CacheSimpleEntryListenerConfig listenerConfig1 = cacheEntryListeners.get(1);
        assertTrue(listenerConfig1.isSynchronous());
        assertTrue(listenerConfig1.isOldValueRequired());
        assertEquals("com.hazelcast.config.CacheConfigTest$MySyncEntryListenerFactory",
                listenerConfig1.getCacheEntryListenerFactory());
        assertEquals("com.hazelcast.config.CacheConfigTest$MySyncEntryEventFilterFactory",
                listenerConfig1.getCacheEntryEventFilterFactory());

        WanReplicationRef wanRefCacheConfig = config1.getCacheConfig("wanRefTestCache").getWanReplicationRef();
        assertEquals("testWanRef", wanRefCacheConfig.getName());
        assertEquals("TestMergePolicy", wanRefCacheConfig.getMergePolicyClassName());
        assertTrue(wanRefCacheConfig.isRepublishingEnabled());

        WanReplicationRef wanRefDisabledRepublishingTestCache =
                config1.getCacheConfig("wanRefDisabledRepublishingTestCache").getWanReplicationRef();
        assertFalse(wanRefDisabledRepublishingTestCache.isRepublishingEnabled());

        assertTrue(cacheConfig1.isDisablePerEntryInvalidationEvents());
    }

    @Test
    public void cacheConfigXmlTest_CustomEvictionPolicyComparator() throws IOException {
        Config config = new XmlConfigBuilder(configUrl1).build();

        CacheSimpleConfig cacheConfig = config.getCacheConfig("cacheWithCustomEvictionPolicyComparator");
        assertNotNull(cacheConfig);

        EvictionConfig evictionConfig = cacheConfig.getEvictionConfig();
        assertNotNull(evictionConfig);
        assertEquals(50, evictionConfig.getSize());
        assertEquals(MaxSizePolicy.ENTRY_COUNT, evictionConfig.getMaxSizePolicy());
        assertEquals("my-custom-eviction-policy-comparator", evictionConfig.getComparatorClassName());
    }

    @Test
    public void cacheConfigXmlTest_TimedCreatedExpiryPolicyFactory() throws IOException {
        Config config1 = new XmlConfigBuilder(configUrl1).build();

        CacheSimpleConfig cacheWithTimedCreatedExpiryPolicyFactoryConfig =
                config1.getCacheConfig("cacheWithTimedCreatedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedCreatedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.CREATED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(1, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.DAYS, durationConfig.getTimeUnit());
    }

    @Test
    public void cacheConfigXmlTest_TimedAccessedExpiryPolicyFactory() throws IOException {
        Config config1 = new XmlConfigBuilder(configUrl1).build();

        CacheSimpleConfig cacheWithTimedAccessedExpiryPolicyFactoryConfig =
                config1.getCacheConfig("cacheWithTimedAccessedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedAccessedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.ACCESSED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(2, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.HOURS, durationConfig.getTimeUnit());
    }

    @Test
    public void cacheConfigXmlTest_TimedModifiedExpiryPolicyFactory() throws IOException {
        Config config1 = new XmlConfigBuilder(configUrl1).build();

        CacheSimpleConfig cacheWithTimedModifiedExpiryPolicyFactoryConfig =
                config1.getCacheConfig("cacheWithTimedModifiedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedModifiedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.MODIFIED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(3, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.MINUTES, durationConfig.getTimeUnit());
    }

    @Test
    public void cacheConfigXmlTest_TimedModifiedTouchedPolicyFactory() throws IOException {
        Config config1 = new XmlConfigBuilder(configUrl1).build();

        CacheSimpleConfig cacheWithTimedTouchedExpiryPolicyFactoryConfig =
                config1.getCacheConfig("cacheWithTimedTouchedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedTouchedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.TOUCHED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(4, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.SECONDS, durationConfig.getTimeUnit());
    }

    @Test
    public void cacheConfigXmlTest_TimedEternalTouchedPolicyFactory() throws IOException {
        Config config1 = new XmlConfigBuilder(configUrl1).build();

        CacheSimpleConfig cacheWithTimedEternalExpiryPolicyFactoryConfig =
                config1.getCacheConfig("cacheWithTimedEternalExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedEternalExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.ETERNAL, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
    }

    @Test
    public void cacheConfigXmlTest_DefaultMergePolicy() throws IOException {
        Config config = new XmlConfigBuilder(configUrl1).build();

        CacheSimpleConfig cacheWithDefaultMergePolicyConfig = config.getCacheConfig("cacheWithDefaultMergePolicy");

        assertNotNull(cacheWithDefaultMergePolicyConfig);
        assertEquals(MergePolicyConfig.DEFAULT_MERGE_POLICY,
                cacheWithDefaultMergePolicyConfig.getMergePolicyConfig().getPolicy());
    }

    @Test
    public void cacheConfigXmlTest_CustomMergePolicy() throws IOException {
        Config config = new XmlConfigBuilder(configUrl1).build();

        CacheSimpleConfig cacheWithCustomMergePolicyConfig = config.getCacheConfig("cacheWithCustomMergePolicy");

        assertNotNull(cacheWithCustomMergePolicyConfig);
        assertEquals("MyDummyMergePolicy", cacheWithCustomMergePolicyConfig.getMergePolicyConfig().getPolicy());
    }

    @Test
    public void cacheConfigXmlTest_constructingToCacheConfig() throws Exception {
        Config config1 = new XmlConfigBuilder(configUrl1).build();

        assertEquals("test-cluster1", config1.getClusterName());

        CacheSimpleConfig cacheSimpleConfig1 = config1.getCacheConfig("cache1");
        CacheConfig cacheConfig1 = new CacheConfig(cacheSimpleConfig1);
        assertTrue(cacheConfig1.getCacheLoaderFactory() instanceof MyCacheLoaderFactory);
        assertTrue(cacheConfig1.getCacheWriterFactory() instanceof MyCacheWriterFactory);
        assertTrue(cacheConfig1.getExpiryPolicyFactory() instanceof MyExpirePolicyFactory);
        assertTrue(cacheConfig1.isReadThrough());
        assertTrue(cacheConfig1.isWriteThrough());
        assertTrue(cacheConfig1.isStatisticsEnabled());
        assertTrue(cacheConfig1.isManagementEnabled());

        assertNotNull(cacheConfig1.getEvictionConfig());
        assertEquals(50, cacheConfig1.getEvictionConfig().getSize());
        assertEquals(MaxSizePolicy.ENTRY_COUNT, cacheConfig1.getEvictionConfig().getMaxSizePolicy());
        assertEquals(EvictionPolicy.LFU, cacheConfig1.getEvictionConfig().getEvictionPolicy());

        assertTrue(cacheConfig1.isDisablePerEntryInvalidationEvents());
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
    public void cacheCacheManagerByLocationJarFileTest() throws Exception {
        File jcacheConfigFile = File.createTempFile("jcache_config_", ".jar");
        JarUtil.createJarFile(
                "src/test/resources/",
                newArrayList("test-hazelcast-jcache.xml"),
                jcacheConfigFile.getAbsolutePath()
        );

        URI uri = new URI("jar:" + jcacheConfigFile.toURI() + "!/test-hazelcast-jcache.xml");
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri, null, new Properties());
        assertThat(cacheManager).isNotNull();

        HazelcastServerCacheManager serverCacheManager = cacheManager.unwrap(HazelcastServerCacheManager.class);
        assertThat(serverCacheManager.getHazelcastInstance().getName()).isEqualTo("test-hazelcast-jcache");
    }

    @Test
    public void cacheManagerByLocationFileTest() throws URISyntaxException {
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

        assertEquals(2, Hazelcast.getAllHazelcastInstances().size());
    }

    @Test
    public void cacheManagerByInstanceNameTest() throws URISyntaxException {
        final String instanceName = randomName();
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
    public void instanceNameShouldBeUsedIfItIsSpecified()
            throws URISyntaxException, IOException {
        Config config = new XmlConfigBuilder(configUrl1).build();
        URI uri = new URI("MY-SCOPE");
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, configUrl1.toString());
        HazelcastCacheManager cacheManager =
                (HazelcastCacheManager) Caching.getCachingProvider().getCacheManager(uri, null, properties);

        assertNotNull(cacheManager);
        assertEquals(config.getInstanceName(), cacheManager.getHazelcastInstance().getName());
    }

    @Test
    public void configUrlShouldBeUsedAsInstanceNameIfInstanceNameIsNotSpecified() throws Exception {
        URI uri = new URI("MY-SCOPE");
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, configUrl2.toString());
        HazelcastCacheManager cacheManager =
                (HazelcastCacheManager) Caching.getCachingProvider().getCacheManager(uri, null, properties);

        assertNotNull(cacheManager);
        assertEquals(configUrl2.toString(), cacheManager.getHazelcastInstance().getName());
    }

    @Test
    public void instanceNamePropertyShouldBeUsedWhenNoInstanceNameIsSpecified() throws Exception {
        String instanceName = randomName();
        URI uri = new URI("MY-SCOPE");
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, configUrl2.toString());
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME, instanceName);
        HazelcastCacheManager cacheManager =
                (HazelcastCacheManager) Caching.getCachingProvider().getCacheManager(uri, null, properties);

        assertNotNull(cacheManager);
        assertEquals(instanceName, cacheManager.getHazelcastInstance().getName());
    }

    @Test
    public void instanceNamePropertyShouldBeUsedEvenThoughInstanceNameIsSpecifiedInTheConfig() throws Exception {
        String instanceName = randomName();
        URI uri = new URI("MY-SCOPE");
        Properties properties = new Properties();
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION, configUrl1.toString());
        properties.setProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME, instanceName);
        HazelcastCacheManager cacheManager =
                (HazelcastCacheManager) Caching.getCachingProvider().getCacheManager(uri, null, properties);

        assertNotNull(cacheManager);
        assertEquals(instanceName, cacheManager.getHazelcastInstance().getName());
    }

    @Test
    public void defaultCacheTest() {
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();
        assertNotNull(cacheManager);

        Cache testCache = cacheManager.getCache("default");
        assertNull(testCache);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setAsyncBackupCount_whenItsNegative() {
        CacheConfig config = new CacheConfig();
        config.setAsyncBackupCount(-1);
    }

    @Test
    public void setAsyncBackupCount_whenItsZero() {
        CacheConfig config = new CacheConfig();
        config.setAsyncBackupCount(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setAsyncBackupCount_whenTooLarge() {
        CacheConfig config = new CacheConfig();
        config.setAsyncBackupCount(200); //max allowed is 6..
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBackupCount_whenItsNegative() {
        CacheConfig config = new CacheConfig();
        config.setBackupCount(-1);
    }

    @Test
    public void setBackupCount_whenItsZero() {
        CacheConfig config = new CacheConfig();
        config.setBackupCount(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBackupCount_whenTooLarge() {
        CacheConfig config = new CacheConfig();
        config.setBackupCount(200); //max allowed is 6..
    }

    @Test(expected = NullPointerException.class)
    public void givenNullMerkleTreeConfig_throws_NPE() {
        new CacheConfig()
                .setMerkleTreeConfig(null);
    }

    @Test
    public void createCache_WhenCacheConfigIsNull() {
        String cacheName = "cacheNull";

        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();

        try {
            cacheManager.createCache(cacheName, (Configuration<Object, Object>) null);
            fail("NullPointerException expected");
        } catch (NullPointerException expected) {
            ignore(expected);
        }
    }

    @Test
    public void testGetPreConfiguredCache() {
        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig().setName("test"));

        int count = 4;
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(count);
        for (int i = 0; i < count; i++) {
            HazelcastInstance instance = factory.newHazelcastInstance(config);
            CachingProvider provider = createServerCachingProvider(instance);
            CacheManager cacheManager = provider.getCacheManager();

            Cache<Object, Object> cache = cacheManager.getCache("test");
            assertNotNull("Pre-configured cache cannot be retrieved on instance: " + i, cache);
        }
    }

    @Test
    public void testEntryListenerFactoryFromSimpleCacheConfig() {
        String cacheName = randomString();
        Config config = createConfig(cacheName);
        HazelcastInstance instance = createHazelcastInstance(config);
        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        Cache<Object, Object> cache = cacheManager.getCache(cacheName);
        EntryListener.latch = new CountDownLatch(1);
        cache.put(randomString(), randomString());
        assertOpenEventually(EntryListener.latch);
    }

    Config createConfig(String cacheName) {
        Config config = new Config();
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName(cacheName);
        CacheSimpleEntryListenerConfig cacheSimpleEntryListenerConfig = new CacheSimpleEntryListenerConfig();
        cacheSimpleEntryListenerConfig.setCacheEntryListenerFactory(EntryListenerFactory.class.getName());
        cacheSimpleConfig.addEntryListenerConfig(cacheSimpleEntryListenerConfig);
        config.addCacheConfig(cacheSimpleConfig);
        return config;
    }

    private ICacheService getCacheService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(ICacheService.SERVICE_NAME);
    }

    @Test
    public void testGetCacheConfigsAtJoin() {
        final String cacheName = randomString();
        final String managerPrefix = "hz:";
        final String fullCacheName = managerPrefix + cacheName;
        final Config config = new Config();
        final CacheConfig cacheConfig = new CacheConfig()
                .setName(cacheName)
                .setManagerPrefix(managerPrefix);

        final TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = instanceFactory.newHazelcastInstance(config);
        final ICacheService cacheService1 = getCacheService(instance1);

        assertNull(cacheService1.getCacheConfig(fullCacheName));

        cacheService1.putCacheConfigIfAbsent(cacheConfig);

        assertNotNull(cacheService1.getCacheConfig(fullCacheName));

        final HazelcastInstance instance2 = instanceFactory.newHazelcastInstance(config);
        final ICacheService cacheService2 = getCacheService(instance2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(cacheService2.getCacheConfig(fullCacheName));
            }
        });
    }

    public static class EntryListenerFactory implements Factory<EntryListener> {
        @Override
        public EntryListener create() {
            return new EntryListener();
        }
    }

    public static class EntryListener implements CacheEntryCreatedListener {

        static volatile CountDownLatch latch;

        @Override
        public void onCreated(Iterable iterable) throws CacheEntryListenerException {
            latch.countDown();
        }
    }

    public static class MyCacheLoaderFactory implements Factory {
        @Override
        public Object create() {
            return null;
        }
    }

    public static class MyCacheWriterFactory implements Factory {
        @Override
        public Object create() {
            return null;
        }
    }

    public static class MyExpirePolicyFactory implements Factory {
        @Override
        public Object create() {
            return null;
        }
    }

    public static class MyEntryListenerFactory implements Factory {
        @Override
        public Object create() {
            return null;
        }
    }

    public static class MyEntryEventFilterFactory implements Factory {
        @Override
        public Object create() {
            return null;
        }
    }

    public static class MySyncEntryListenerFactory implements Factory {
        @Override
        public Object create() {
            return null;
        }
    }

    public static class MySyncEntryEventFilterFactory implements Factory {
        @Override
        public Object create() {
            return null;
        }
    }

    public static class MyCacheLoader<K> implements CacheLoader<K, String> {
        @Override
        public String load(K key) throws CacheLoaderException {
            return String.valueOf(key);
        }

        @Override
        public Map<K, String> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
            Map<K, String> result = new HashMap<K, String>();
            for (K key : keys) {
                result.put(key, String.valueOf(key));
            }
            return result;
        }
    }

    public static class EmptyCacheWriter<K> implements CacheWriter<K, String> {

        @Override
        public void write(Cache.Entry<? extends K, ? extends String> entry) throws CacheWriterException {

        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends K, ? extends String>> entries) throws CacheWriterException {

        }

        @Override
        public void delete(Object key) throws CacheWriterException {

        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {

        }
    }
}
