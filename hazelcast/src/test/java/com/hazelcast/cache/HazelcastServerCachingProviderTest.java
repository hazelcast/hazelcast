/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByLocation;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastServerCachingProviderTest
        extends org.jsr107.tck.spi.CachingProviderTest {

    @BeforeClass
    public static void init() {
        JsrTestUtil.setup();
        System.setProperty("javax.cache.spi.CachingProvider",
                HazelcastCachingProvider.SERVER_CACHING_PROVIDER);
    }

    @AfterClass
    public static void cleanup() {
        JsrTestUtil.cleanup();
        System.clearProperty("javax.cache.spi.CachingProvider");
    }

    @Test
    public void testMemberCachingProviderYamlConfig() {
        testCachingProvider("classpath:test-hazelcast.yaml");
    }

    @Test
    public void testMemberCachingProviderXmlConfig() {
        testCachingProvider("classpath:test-hazelcast.xml");
    }

    private void testCachingProvider(String config) {
        CachingProvider cachingProvider = Caching.getCachingProvider(HazelcastCachingProvider.MEMBER_CACHING_PROVIDER);
        CacheManager cacheManager = cachingProvider.getCacheManager(null, null, propertiesByLocation(config));
        assertNotNull(cacheManager);
    }
}
