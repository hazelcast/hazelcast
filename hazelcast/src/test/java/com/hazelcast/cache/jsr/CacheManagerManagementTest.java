/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.jsr;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.MutableConfiguration;

import static com.hazelcast.cache.jsr.JsrTestUtil.assertNoMBeanLeftovers;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheManagerManagementTest extends org.jsr107.tck.management.CacheManagerManagementTest {

    @BeforeClass
    public static void init() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanup() {
        JsrTestUtil.cleanup();
    }

    @Test
    public void testMBeansLeftoversAreDetected() throws Exception {
        CacheManager cacheManager = getCacheManager();
        MutableConfiguration<Integer, Integer> cacheConfig = new MutableConfiguration<>();
        cacheConfig.setManagementEnabled(true);
        Cache<Integer, Integer> cache = cacheManager.createCache("int-cache", cacheConfig);
        try {
            assertNoMBeanLeftovers();
            fail("Should have failed due to mbean leftovers");
        } catch (AssertionError error) {
            assertThat(error.getMessage()).contains("Leftover MBeans are still registered with the platform MBeanServer");
            cache.close();
        }
    }
}
