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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.AbstractHazelcastCachingProvider;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceItself;
import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceName;
import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByLocation;
import static com.hazelcast.cache.jsr.JsrTestUtil.cleanup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CachingProviderTest extends HazelcastTestSupport {

    protected static final int INSTANCE_COUNT = 3;
    protected static final String INSTANCE_1_NAME = randomString();
    protected static final String INSTANCE_2_NAME = randomString();
    protected static final String CONFIG_CLASSPATH_LOCATION = "test-hazelcast-jcache.xml";

    protected TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(INSTANCE_COUNT);
    protected HazelcastInstance instance1;
    protected HazelcastInstance instance2;
    protected HazelcastInstance instance3;
    protected CachingProvider cachingProvider;

    @Before
    public void setup() {
        instance1 = createHazelcastInstance(INSTANCE_1_NAME);
        instance2 = createHazelcastInstance(INSTANCE_2_NAME);
        cachingProvider = createCachingProvider(instance1);
        // also start a hazelcast instance off a well-known config location
        Config config = new ClasspathXmlConfig(CONFIG_CLASSPATH_LOCATION);
        instance3 = instanceFactory.newHazelcastInstance(config);
    }

    protected HazelcastInstance createHazelcastInstance(String instanceName) {
        Config config = new Config();
        config.setInstanceName(instanceName);
        config.getNetworkConfig().getJoin().getMulticastConfig().setLoopbackModeEnabled(true);
        config.getGroupConfig().setName("test-group1");
        config.getGroupConfig().setPassword("test-pass1");
        return instanceFactory.newHazelcastInstance(config);
    }

    protected CachingProvider createCachingProvider(HazelcastInstance defaultInstance) {
        return HazelcastServerCachingProvider.createCachingProvider(defaultInstance);
    }

    @Test
    public void whenDefaultURI_instanceNameAsProperty_thenThatInstanceIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                null, null, propertiesByInstanceName(INSTANCE_2_NAME));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenOtherURI_instanceNameAsProperty_thenThatInstanceIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                new URI("other-uri"), null, propertiesByInstanceName(INSTANCE_2_NAME));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenDefaultURI_inexistentInstanceNameAsProperty_thenStartsOtherInstance() throws URISyntaxException {
        cachingProvider.getCacheManager(null, null, propertiesByInstanceName("instance-does-not-exist"));
        assertInstanceStarted("instance-does-not-exist");
    }

    @Test
    public void whenOtherURI_inexistentInstanceNameAsProperty_thenStartsNewInstance() throws URISyntaxException {
        cachingProvider.getCacheManager(new URI("other-uri"), null, propertiesByInstanceName("instance-does-not-exist"));
        assertInstanceStarted("instance-does-not-exist");
    }

    @Test
    public void whenDefaultURI_noInstanceName_thenUseDefaultHazelcastInstance() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager();
        assertCacheManagerInstance(cacheManager, instance1);
    }

    @Test
    public void whenInstanceNameAsUri_thenThatInstanceIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                new URI(INSTANCE_2_NAME), null);
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenInexistentInstanceNameAsUri_thenOtherInstanceIsStarted() throws URISyntaxException {
        cachingProvider.getCacheManager(new URI("does-not-exist"), null);
        assertInstanceStarted("does-not-exist");
    }

    @Test
    public void whenConfigLocationAsUri_thenThatInstanceIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:" + CONFIG_CLASSPATH_LOCATION), null);
        assertCacheManagerInstance(cacheManager, instance3);
    }

    @Test
    public void whenConfigLocationAsUriViaProperty_thenThatInstanceIsUsed() throws URISyntaxException {
        System.setProperty("PROPERTY_PLACEHOLDER", "classpath:" + CONFIG_CLASSPATH_LOCATION);
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                new URI("PROPERTY_PLACEHOLDER"), null);
        assertCacheManagerInstance(cacheManager, instance3);
    }

    @Test(expected = CacheException.class)
    public void whenInvalidConfigLocationAsUri_thenFails() throws URISyntaxException {
        cachingProvider.getCacheManager(new URI("classpath:this-config-does-not-exist"), null);
    }

    // test that config location property has priority over attempting URI interpretation:
    // CacheManager's URI points to invalid config location, however a valid config location is
    // specified in properties
    @Test
    public void whenConfigLocationAsProperty_thenThatInstanceIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:this-config-does-not-exist"), null,
                propertiesByLocation("classpath:" + CONFIG_CLASSPATH_LOCATION));
        assertCacheManagerInstance(cacheManager, instance3);
    }

    // test that instance property has priority over attempting URI interpretation:
    // CacheManager's URI points to invalid config location, however a valid instance name is
    // specified in properties, so this instance is picked up.
    @Test
    public void whenInstanceNameAsProperty_thenThatInstanceIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:this-config-does-not-exist"), null, propertiesByInstanceName(INSTANCE_2_NAME));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenInstanceItselfAsProperty_andInvalidConfigURI_thenInstanceItselfIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:this-config-does-not-exist"), null, propertiesByInstanceItself(instance2));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenInstanceItselfAsProperty_andValidConfigURI_thenInstanceItselfIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:" + CONFIG_CLASSPATH_LOCATION), null, propertiesByInstanceItself(instance2));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenInstanceItselfAsProperty_andValidInstanceNameURI_thenInstanceItselfIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:" + CONFIG_CLASSPATH_LOCATION), null, propertiesByInstanceItself(instance2));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenInstanceItselfAsProperty_andDefaultURI_thenInstanceItselfIsUsed() throws URISyntaxException {
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) cachingProvider.getCacheManager(
                null, null, propertiesByInstanceItself(instance3));
        assertCacheManagerInstance(cacheManager, instance3);
    }

    @Test
    public void whenDefaultCacheManager_withUnnamedDefaultInstance_thenNoSharedNameHazelcastInstanceExists() {
        cleanupForDefaultCacheManagerTest();
        try {
            System.setProperty(AbstractHazelcastCachingProvider.NAMED_JCACHE_HZ_INSTANCE, "false");
            CachingProvider defaultCachingProvider = Caching.getCachingProvider();
            CacheManager defaultCacheManager = defaultCachingProvider.getCacheManager();
            Collection<HazelcastInstance> instances = getStartedInstances();
            for (HazelcastInstance instance : instances) {
                if (AbstractHazelcastCachingProvider.SHARED_JCACHE_INSTANCE_NAME.equals(instance.getName())) {
                    fail("The default named HazelcastInstance shouldn't have been started");
                }
            }
            defaultCachingProvider.close();
        } finally {
            cleanup();
        }
    }

    @Test
    public void whenDefaultCacheManager_thenSharedNameHazelcastInstanceExists() {
        cleanupForDefaultCacheManagerTest();
        try {
            CachingProvider defaultCachingProvider = Caching.getCachingProvider();
            CacheManager defaultCacheManager = defaultCachingProvider.getCacheManager();
            Collection<HazelcastInstance> instances = getStartedInstances();
            boolean sharedInstanceStarted = false;
            for (HazelcastInstance instance : instances) {
                if (instance.getName().equals(AbstractHazelcastCachingProvider.SHARED_JCACHE_INSTANCE_NAME)) {
                    sharedInstanceStarted = true;
                }
            }
            assertTrue("The default named HazelcastInstance should have been started", sharedInstanceStarted);
            defaultCachingProvider.close();
        } finally {
            cleanup();
        }
    }

    protected void assertCacheManagerInstance(HazelcastCacheManager cacheManager, HazelcastInstance instance) {
        assertEquals(instance, cacheManager.getHazelcastInstance());
    }

    protected void assertInstanceStarted(String instanceName) {
        HazelcastInstance otherInstance = Hazelcast.getHazelcastInstanceByName(instanceName);
        assertNotNull(otherInstance);
        otherInstance.getLifecycleService().terminate();
    }

    protected Collection<HazelcastInstance> getStartedInstances() {
        return Hazelcast.getAllHazelcastInstances();
    }

    // tests for the default cache manager require cleanup before running which must be overridden for client-side tests
    protected void cleanupForDefaultCacheManagerTest() {
        cleanup();
    }
}
