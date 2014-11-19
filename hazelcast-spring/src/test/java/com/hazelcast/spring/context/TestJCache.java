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

package com.hazelcast.spring.context;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for jcache parser
 */
@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"test-jcache-application-context.xml"})
@Category(QuickTest.class)
public class TestJCache {

    @Autowired
    private ApplicationContext context;

    @Resource(name = "instance1")
    private HazelcastInstance instance1;

    @BeforeClass
    @AfterClass
    public static void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testContextInitiazedSuccessfully() {
        assertNotNull(context);
    }

    @Test
    public void testConfig() {
        assertNotNull(instance1);

        CacheSimpleConfig simpleConfig = instance1.getConfig().getCacheConfigs().get("cache1");

        assertNotNull(simpleConfig);

        assertEquals(1, simpleConfig.getAsyncBackupCount());
        assertEquals(2, simpleConfig.getBackupCount());
        assertEquals("java.lang.Integer", simpleConfig.getKeyType());
        assertEquals("java.lang.String", simpleConfig.getValueType());
        assertTrue(simpleConfig.isStatisticsEnabled());
        assertTrue(simpleConfig.isManagementEnabled());
        assertTrue(simpleConfig.isReadThrough());
        assertTrue(simpleConfig.isWriteThrough());
        assertEquals("com.hazelcast.cache.MyCacheLoaderFactory", simpleConfig.getCacheLoaderFactory());
        assertEquals("com.hazelcast.cache.MyCacheWriterFactory", simpleConfig.getCacheWriterFactory());
        assertEquals("com.hazelcast.cache.MyExpiryPolicyFactory", simpleConfig.getExpiryPolicyFactory());
        assertEquals(InMemoryFormat.OBJECT, simpleConfig.getInMemoryFormat());
        assertNotNull(simpleConfig.getMaxSizeConfig());
        assertEquals(50, simpleConfig.getMaxSizeConfig().getSize());
        assertEquals(MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE,
                simpleConfig.getMaxSizeConfig().getMaxSizePolicy());
        assertEquals(20, simpleConfig.getEvictionPercentage());
        assertEquals(EvictionPolicy.LRU, simpleConfig.getEvictionPolicy());
    }

}
