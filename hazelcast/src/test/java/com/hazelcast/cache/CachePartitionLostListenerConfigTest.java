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

package com.hazelcast.cache;

import com.hazelcast.cache.CachePartitionLostListenerTest.EventCollectingCachePartitionLostListener;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.config.CachePartitionLostListenerConfigReadOnly;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.EventListener;
import java.util.List;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CachePartitionLostListenerConfigTest extends HazelcastTestSupport {

    private final URL configUrl = getClass().getClassLoader().getResource("test-hazelcast-jcache-partition-lost-listener.xml");

    @Test
    public void testCachePartitionLostListener_registeredViaImplementationInConfigObject() {
        final String cacheName = "myCache";

        Config config = new Config();
        CacheSimpleConfig cacheConfig = config.getCacheConfig(cacheName);
        CachePartitionLostListener listener = mock(CachePartitionLostListener.class);
        cacheConfig.addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig(listener));
        cacheConfig.setBackupCount(0);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.getCache(cacheName);

        final EventService eventService = getNode(instance).getNodeEngine().getEventService();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<EventRegistration> registrations = eventService.getRegistrations(CacheService.SERVICE_NAME, cacheName);
                assertFalse(registrations.isEmpty());
            }
        });
    }

    @Test
    public void cacheConfigXmlTest() throws IOException {
        String cacheName = "cacheWithPartitionLostListener";
        Config config = new XmlConfigBuilder(configUrl).build();
        CacheSimpleConfig cacheConfig = config.getCacheConfig(cacheName);

        List<CachePartitionLostListenerConfig> configs = cacheConfig.getPartitionLostListenerConfigs();
        assertEquals(1, configs.size());
        assertEquals("DummyCachePartitionLostListenerImpl", configs.get(0).getClassName());
    }

    @Test
    public void testCachePartitionLostListenerConfig_setImplementation() {
        CachePartitionLostListener listener = mock(CachePartitionLostListener.class);
        CachePartitionLostListenerConfig listenerConfig = new CachePartitionLostListenerConfig();
        listenerConfig.setImplementation(listener);

        assertEquals(listener, listenerConfig.getImplementation());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCachePartitionLostListenerReadOnlyConfig_withClassName() {
        CachePartitionLostListenerConfigReadOnly readOnly
                = new CachePartitionLostListenerConfigReadOnly(new CachePartitionLostListenerConfig());
        readOnly.setClassName("com.hz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCachePartitionLostListenerReadOnlyConfig_withImplementation() {
        CachePartitionLostListener listener = mock(CachePartitionLostListener.class);
        CachePartitionLostListenerConfig listenerConfig = new CachePartitionLostListenerConfig(listener);
        CachePartitionLostListenerConfigReadOnly readOnly = new CachePartitionLostListenerConfigReadOnly(listenerConfig);
        assertEquals(listener, readOnly.getImplementation());
        readOnly.setImplementation(mock(CachePartitionLostListener.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCachePartitionLostListenerReadOnlyConfig_withEventListenerImplementation() {
        CachePartitionLostListenerConfigReadOnly readOnly
                = new CachePartitionLostListenerConfigReadOnly(new CachePartitionLostListenerConfig());
        readOnly.setImplementation(mock(EventListener.class));
    }

    @Test
    public void testGetImplementation() {
        CachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(0);
        CachePartitionLostListenerConfig listenerConfig = new CachePartitionLostListenerConfig(listener);
        assertEquals(listener, listenerConfig.getImplementation());
    }

    @Test
    public void testGetClassName() {
        String className = "EventCollectingCachePartitionLostListener";
        CachePartitionLostListenerConfig listenerConfig = new CachePartitionLostListenerConfig(className);
        assertEquals(className, listenerConfig.getClassName());
    }

    @Test
    public void testEqualsAndHashCode() {
        CachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(0);

        CachePartitionLostListenerConfig listenerConfig1 = new CachePartitionLostListenerConfig();
        CachePartitionLostListenerConfig listenerConfig2 = new CachePartitionLostListenerConfig();

        assertEquals(listenerConfig1, listenerConfig1);
        assertEquals(listenerConfig1, new CachePartitionLostListenerConfig());
        assertNotEquals(listenerConfig1, null);
        assertNotEquals(listenerConfig1, new Object());

        listenerConfig1.setImplementation(listener);
        assertNotEquals(listenerConfig1, listenerConfig2);
        assertNotEquals(listenerConfig1.hashCode(), listenerConfig2.hashCode());

        listenerConfig2.setImplementation(listener);
        assertEquals(listenerConfig1, listenerConfig2);
        assertEquals(listenerConfig1.hashCode(), listenerConfig2.hashCode());

        listenerConfig1.setClassName("EventCollectingCachePartitionLostListener");
        listenerConfig2.setClassName("CachePartitionLostListenerConfig");
        assertNotEquals(listenerConfig1, listenerConfig2);
        assertNotEquals(listenerConfig1.hashCode(), listenerConfig2.hashCode());

        listenerConfig2.setClassName("EventCollectingCachePartitionLostListener");
        assertEquals(listenerConfig1, listenerConfig2);
        assertEquals(listenerConfig1.hashCode(), listenerConfig2.hashCode());
    }
}
