/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.cache.CacheManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"jCacheCacheManager-applicationContext-hazelcast.xml"})
public class JCacheCacheManagerTest {

    @Autowired
    private HazelcastInstance instance;

    @Autowired
    private IJCacheDummyBean bean;

    @Autowired
    private JCacheCacheManager springCacheManager;

    @Autowired
    private CacheManager cacheManager2;

    @BeforeAll
    @AfterAll
    public static void start() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @BeforeEach
    public void reset() {
        Iterable<String> cacheNames = springCacheManager.getCacheManager().getCacheNames();
        for (String cacheName : cacheNames) {
            springCacheManager.getCacheManager().getCache(cacheName).removeAll();
        }
        bean.reset();
    }

    @Test
    public void testBean() {
        for (int i = 0; i < 100; i++) {
            bean.reset();
            bean.getName(i);
            bean.getCity(i);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("name:" + i, bean.getName(i));
            assertEquals("city:" + i, bean.getCity(i));
        }
    }

    @Test
    void testURI() {
        assertEquals("hazelcast", springCacheManager.getCacheManager().getURI().toString());
        assertEquals("testURI", cacheManager2.getURI().toString());
    }

    @Test
    void testProperties() {
        assertEquals("testValue", cacheManager2.getProperties().getProperty("testProperty"));
        assertEquals("named-spring-hz-instance", cacheManager2.getProperties().getProperty("hazelcast.instance.name"));
    }

    @Test
    void testCacheNames() {
        assertNotNull(springCacheManager.getCacheManager().getCache("name"));
        assertNotNull(springCacheManager.getCacheManager().getCache("city"));
    }

    public static class DummyBean implements IJCacheDummyBean {

        private boolean alreadyCalledName;
        private boolean alreadyCalledCity;

        @Override
        @Cacheable("name")
        public String getName(int index) {
            if (!alreadyCalledName) {
                alreadyCalledName = true;
                return "name:" + index;
            }
            fail("value is not retrieved from cache on second call!");
            return null;
        }

        @Override
        @Cacheable("city")
        public String getCity(int k) {
            if (!alreadyCalledCity) {
                alreadyCalledCity = true;
                return "city:" + k;
            }
            fail("value is not retrieved from cache on second call!");
            return null;
        }

        @Override
        public void reset() {
            alreadyCalledCity = false;
            alreadyCalledName = false;
        }
    }
}
