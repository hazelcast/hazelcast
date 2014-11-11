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
        CacheSimpleConfig simpleConfig1 = instance1.getConfig().getCacheConfigs().get("cache1");
        assertNotNull(simpleConfig1);

        assertEquals(20, simpleConfig1.getEvictionPercentage());
    }

}
