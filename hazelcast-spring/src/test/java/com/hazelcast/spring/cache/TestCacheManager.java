/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.spring.HzSpringJUnit4ClassRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(HzSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"cacheManager-applicationContext-hazelcast.xml"})
public class TestCacheManager {

    static {
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
    }

    @Resource(name = "instance")
    private HazelcastInstance instance;

    @Autowired
    private IDummyBean bean;

    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals("name:" + i, bean.getName(i));
            Assert.assertEquals("city:" + i, bean.getCity(i));
        }
    }

    @Test
    public void testNull() {
        for (int i = 0; i < 100; i++) {
            Assert.assertNull(bean.getNull());
        }
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
    }
}
