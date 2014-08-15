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

package com.hazelcast.spring.cache;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"cacheManager-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestCacheManager {

    @Resource(name = "instance")
    private HazelcastInstance instance;

    @Autowired
    private IDummyBean bean;

    @Autowired
    private CacheManager cacheManager;

    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        for (int i = 0; i < 100; i++) {
            assertEquals("name:" + i, bean.getName(i));
            assertEquals("city:" + i, bean.getCity(i));
        }
    }

    @Test
    public void testNull() {
        for (int i = 0; i < 100; i++) {
            assertNull(bean.getNull());
        }
    }

    @Test
    public void testTTL() throws InterruptedException {
        final String name = bean.getNameWithTTL();
        assertEquals("ali", name);
        final String nameFromCache = bean.getNameWithTTL();
        assertEquals("ali", nameFromCache);

        Thread.sleep(3000);

        final String nameFromCacheAfterTTL = bean.getNameWithTTL();
        assertNull(nameFromCacheAfterTTL);

    }

    @Test
    public void testCacheNames() throws InterruptedException {

        //Create a test instance, to reproduce the behaviour stated in the
        //github issue: https://github.com/hazelcast/hazelcast/issues/492
        String testMap = "test-map";
        HazelcastInstance testInstance = Hazelcast.newHazelcastInstance();
        testInstance.getMap(testMap);

        //Be sure that test-map is distrubuted
        Thread.sleep(3000);

        Collection<String> test = cacheManager.getCacheNames();
        Assert.assertTrue(test.contains(testMap));
        testInstance.shutdown();
    }


    public static class DummyBean implements IDummyBean {

        @Cacheable("name")
        public String getName(int k) {
            Assert.fail("should not call this method!");
            return null;
        }

        @Cacheable("city")
        public String getCity(int k) {
            Assert.fail("should not call this method!");
            return null;
        }

        final AtomicBoolean nullCall = new AtomicBoolean(false);

        @Cacheable("null-map")
        public Object getNull() {
            if (nullCall.compareAndSet(false, true)) {
                return null;
            }
            Assert.fail("should not call this method!");
            return null;
        }

        final AtomicBoolean firstCall = new AtomicBoolean(false);

        @Cacheable("map-with-ttl")
        public String getNameWithTTL(){
            if (firstCall.compareAndSet(false, true)) {
                return "ali";
            }
            return null;
        }
    }
}
