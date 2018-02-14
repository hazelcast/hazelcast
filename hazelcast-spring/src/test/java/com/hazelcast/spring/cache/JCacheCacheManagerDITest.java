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

package com.hazelcast.spring.cache;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import javax.cache.Cache;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"jCacheCacheManager-applicationContext-DI.xml"})
@Category(QuickTest.class)
public class JCacheCacheManagerDITest {

    @Resource(name = "cacheManager")
    private JCacheCacheManager springCacheManager;

    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testCacheWithCacheLoaderFactory_dependenciesInjected() {
        Cache cacheWithLoader = springCacheManager.getCacheManager().getCache("cacheWithLoader");
        // use cacheloader at least once to ensure it's instantiated
        cacheWithLoader.get(1);

        // ensure CacheLoaderFactory & CacheLoader dependencies have been injected
        assertTrue("HazelcastInstance not injected to HazelcastInstanceAware object",
                JCacheCacheLoaderFactory.HAZELCAST_INSTANCE_INJECTED.get());
        assertTrue("Node not injected to NodeAware object",
                JCacheCacheLoaderFactory.NODE_INJECTED.get());
        assertNotNull("Spring bean not injected to @SpringAware object",
                JCacheCacheLoaderFactory.INSTANCE.getDummyBean());
        assertTrue("HazelcastInstance not injected to HazelcastInstanceAware object",
                JCacheCacheLoader.HAZELCAST_INSTANCE_INJECTED.get());
        assertTrue("Node not injected to NodeAware object",
                JCacheCacheLoader.NODE_INJECTED.get());
        assertNotNull("Spring bean not injected to @SpringAware object",
                JCacheCacheLoader.INSTANCE.getDummyBean());
    }

    @Test
    public void testCacheWithCacheWriterFactory_dependenciesInjected() {
        Cache cacheWithWriter = springCacheManager.getCacheManager().getCache("cacheWithWriter");
        // use cacheloader at least once to ensure it's instantiated
        cacheWithWriter.put(1, "1");

        // ensure CacheLoaderFactory & CacheLoader dependencies have been injected
        assertTrue("HazelcastInstance not injected to HazelcastInstanceAware object",
                JCacheCacheWriterFactory.HAZELCAST_INSTANCE_INJECTED.get());
        assertTrue("Node not injected to NodeAware object",
                JCacheCacheWriterFactory.NODE_INJECTED.get());
        assertNotNull("Spring bean not injected to @SpringAware object",
                JCacheCacheWriterFactory.INSTANCE.getDummyBean());
        assertTrue("HazelcastInstance not injected to HazelcastInstanceAware object",
                JCacheCacheWriter.HAZELCAST_INSTANCE_INJECTED.get());
        assertTrue("Node not injected to NodeAware object",
                JCacheCacheWriter.NODE_INJECTED.get());
        assertNotNull("Spring bean not injected to @SpringAware object",
                JCacheCacheWriter.INSTANCE.getDummyBean());
    }

    @Test
    public void testCacheWithExpiryPolicyFactory_dependenciesInjected() {
        Cache cacheWithExpiryPolicy = springCacheManager.getCacheManager().getCache("cacheWithExpiryPolicy");
        // use cacheloader at least once to ensure it's instantiated
        cacheWithExpiryPolicy.put(1, "1");

        // ensure CacheLoaderFactory & CacheLoader dependencies have been injected
        assertTrue("HazelcastInstance not injected to HazelcastInstanceAware object",
                JCacheExpiryPolicyFactory.HAZELCAST_INSTANCE_INJECTED.get());
        assertTrue("Node not injected to NodeAware object",
                JCacheExpiryPolicyFactory.NODE_INJECTED.get());
        assertNotNull("Spring bean not injected to @SpringAware object",
                JCacheExpiryPolicyFactory.INSTANCE.getDummyBean());
        assertTrue("HazelcastInstance not injected to HazelcastInstanceAware object",
                JCacheExpiryPolicy.HAZELCAST_INSTANCE_INJECTED.get());
        assertTrue("Node not injected to NodeAware object",
                JCacheExpiryPolicy.NODE_INJECTED.get());
        assertNotNull("Spring bean not injected to @SpringAware object",
                JCacheExpiryPolicy.INSTANCE.getDummyBean());
    }

    @Test
    public void testCacheWithListeners_dependenciesInjected() {
        Cache cacheWithPartitionLostListener = springCacheManager.getCacheManager()
                .getCache("cacheWithListeners");

        cacheWithPartitionLostListener.get(1);

        // ensure partition lost listener got its dependencies injected
        assertTrue("HazelcastInstance not injected to HazelcastInstanceAware object",
                JCachePartitionLostListener.HAZELCAST_INSTANCE_INJECTED.get());
        assertTrue("Node not injected to NodeAware object",
                JCachePartitionLostListener.NODE_INJECTED.get());
        assertNotNull("Spring bean not injected to @SpringAware object",
                JCachePartitionLostListener.INSTANCE.getDummyBean());
        // ensure CacheEntryListenerFactory & CacheEntryListener dependencies have been injected
        assertTrue("HazelcastInstance not injected to HazelcastInstanceAware object",
                JCacheCacheEntryListenerFactory.HAZELCAST_INSTANCE_INJECTED.get());
        assertTrue("Node not injected to NodeAware object",
                JCacheCacheEntryListenerFactory.NODE_INJECTED.get());
        assertNotNull("Spring bean not injected to @SpringAware object",
                JCacheCacheEntryListenerFactory.INSTANCE.getDummyBean());
        assertTrue("HazelcastInstance not injected to HazelcastInstanceAware object",
                JCacheCacheEntryListener.HAZELCAST_INSTANCE_INJECTED.get());
        assertTrue("Node not injected to NodeAware object",
                JCacheCacheEntryListener.NODE_INJECTED.get());
        assertNotNull("Spring bean not injected to @SpringAware object",
                JCacheCacheEntryListener.INSTANCE.getDummyBean());
    }

}
