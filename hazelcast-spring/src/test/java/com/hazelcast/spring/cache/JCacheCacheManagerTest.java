/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import javax.cache.CacheManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"jCacheCacheManager-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class JCacheCacheManagerTest {

    @Resource(name = "instance")
    private HazelcastInstance instance;

    @Autowired
    private IJCacheDummyBean bean;

    @Resource(name = "cacheManager")
    private JCacheCacheManager springCacheManager;

    @Resource(name = "cacheManager2")
    private CacheManager cacheManager2;

    @BeforeClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void reset() {
        Iterable<String> cacheNames = springCacheManager.getCacheManager().getCacheNames();
        for (String cacheName :  cacheNames) {
            springCacheManager.getCacheManager().getCache(cacheName).removeAll();
        }
        bean.reset();
    }

    @Test
    public void testURI() {
        assertEquals("hazelcast", springCacheManager.getCacheManager().getURI().toString());
        assertEquals("testURI", cacheManager2.getURI().toString());
    }

    @Test
    public void testProperties() {
        assertEquals("testValue", cacheManager2.getProperties().getProperty("testProperty"));
        assertEquals("named-spring-hz-instance", cacheManager2.getProperties().getProperty("hazelcast.instance.name"));
    }

    @Test
    public void test() {
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
    public void testCacheNames() throws InterruptedException {
        assertNotNull(springCacheManager.getCacheManager().getCache("name"));
        assertNotNull(springCacheManager.getCacheManager().getCache("city"));
    }

    public static class DummyBean implements IJCacheDummyBean {

        private boolean alreadyCalledName;
        private boolean alreadyCalledCity;

        @Cacheable("name")
        public String getName(int k) {
            if (!alreadyCalledName) {
                alreadyCalledName = true;
                return "name:" + k;
            }
            Assert.fail("value is not retireved from cache on second call!");
            return null;
        }

        @Cacheable("city")
        public String getCity(int k) {
            if (!alreadyCalledCity) {
                alreadyCalledCity = true;
                return "city:" + k;
            }
            Assert.fail("value is not retrieved from cache on second call!");
            return null;
        }

        @Override
        public void reset() {
            alreadyCalledCity = false;
            alreadyCalledName = false;
        }
    }
}
