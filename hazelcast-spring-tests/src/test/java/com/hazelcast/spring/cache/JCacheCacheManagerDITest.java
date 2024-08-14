/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spring.CustomSpringExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.cache.Cache;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"jCacheCacheManager-applicationContext-DI.xml"})
class JCacheCacheManagerDITest {

    @Autowired
    private JCacheCacheManager springCacheManager;

    @BeforeAll
    @AfterAll
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    void testCacheWithCacheLoaderFactory_dependenciesInjected() {
        Cache<Integer, ?> cacheWithLoader = springCacheManager.getCacheManager().getCache("cacheWithLoader");
        // use cacheloader at least once to ensure it's instantiated
        cacheWithLoader.get(1);

        // ensure CacheLoaderFactory & CacheLoader dependencies have been injected
        assertTrue(JCacheCacheLoaderFactory.HAZELCAST_INSTANCE_INJECTED.get(), "HazelcastInstance not injected to HazelcastInstanceAware object");
        assertTrue(JCacheCacheLoaderFactory.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheLoaderFactory.INJECTED_DUMMY_BEAN.get(), "Spring bean not injected to @SpringAware object");
        assertTrue(JCacheCacheLoader.HAZELCAST_INSTANCE_INJECTED.get(), "HazelcastInstance not injected to HazelcastInstanceAware object");
        assertTrue(JCacheCacheLoader.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheLoader.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
    }

    @Test
    void testCacheWithCacheWriterFactory_dependenciesInjected() {
        Cache<Integer, String> cacheWithWriter = springCacheManager.getCacheManager().getCache("cacheWithWriter");
        // use cacheloader at least once to ensure it's instantiated
        cacheWithWriter.put(1, "1");

        // ensure CacheLoaderFactory & CacheLoader dependencies have been injected
        assertTrue(JCacheCacheWriterFactory.HAZELCAST_INSTANCE_INJECTED.get(), "HazelcastInstance not injected to HazelcastInstanceAware object");
        assertTrue(JCacheCacheWriterFactory.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheWriterFactory.INJECTED_DUMMY_BEAN.get(), "Spring bean not injected to @SpringAware object");
        assertTrue(JCacheCacheWriter.HAZELCAST_INSTANCE_INJECTED.get(), "HazelcastInstance not injected to HazelcastInstanceAware object");
        assertTrue(JCacheCacheWriter.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheWriter.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
    }

    @Test
    void testCacheWithExpiryPolicyFactory_dependenciesInjected() {
        Cache<Integer, String> cacheWithExpiryPolicy = springCacheManager.getCacheManager().getCache("cacheWithExpiryPolicy");
        // use cacheloader at least once to ensure it's instantiated
        cacheWithExpiryPolicy.put(1, "1");

        // ensure CacheLoaderFactory & CacheLoader dependencies have been injected
        assertTrue(JCacheExpiryPolicyFactory.HAZELCAST_INSTANCE_INJECTED.get(), "HazelcastInstance not injected to HazelcastInstanceAware object");
        assertTrue(JCacheExpiryPolicyFactory.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheExpiryPolicyFactory.INJECTED_DUMMY_BEAN.get(), "Spring bean not injected to @SpringAware object");
        assertTrue(JCacheExpiryPolicy.HAZELCAST_INSTANCE_INJECTED.get(), "HazelcastInstance not injected to HazelcastInstanceAware object");
        assertTrue(JCacheExpiryPolicy.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheExpiryPolicy.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
    }

    @Test
    void testCacheWithListeners_dependenciesInjected() {
        Cache<Integer, ?> cacheWithPartitionLostListener = springCacheManager.getCacheManager()
                .getCache("cacheWithListeners");

        cacheWithPartitionLostListener.get(1);

        // ensure partition lost listener got its dependencies injected
        assertTrue(JCachePartitionLostListener.HAZELCAST_INSTANCE_INJECTED.get(), "HazelcastInstance not injected to HazelcastInstanceAware object");
        assertTrue(JCachePartitionLostListener.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCachePartitionLostListener.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
        // ensure CacheEntryListenerFactory & CacheEntryListener dependencies have been injected
        assertTrue(JCacheCacheEntryListenerFactory.HAZELCAST_INSTANCE_INJECTED.get(), "HazelcastInstance not injected to HazelcastInstanceAware object");
        assertTrue(JCacheCacheEntryListenerFactory.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheEntryListenerFactory.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
        assertTrue(JCacheCacheEntryListener.HAZELCAST_INSTANCE_INJECTED.get(), "HazelcastInstance not injected to HazelcastInstanceAware object");
        assertTrue(JCacheCacheEntryListener.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheEntryListener.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
    }

}
