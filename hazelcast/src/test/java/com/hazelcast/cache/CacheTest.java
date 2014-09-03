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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheTest
        extends HazelcastTestSupport {

    private CacheManager cacheManager;

    @Before
    public void init() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
        cacheManager = new HazelcastServerCacheManager(hcp, hazelcastInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
    }

    @After
    public void onTearDown() {
        cacheManager.close();
    }

    @Test
    public void testSimple(){
        assertNotNull(cacheManager);

        MutableConfiguration<Integer,String> mc = new MutableConfiguration<Integer, String>();


        CacheConfig<Integer,String> config =  new CacheConfig<Integer, String>(mc);
        config.setTypes(Integer.class,String.class);


        final Cache<Integer, String> cache = cacheManager.createCache("default", config);
        assertNotNull(cache);

        cache.put(1,"test");

        assertEquals("test", cache.get(1));


        final Cache<Integer,String> cache2 = cacheManager.getCache("default",Integer.class, String.class);
        assertNotNull(cache2);

        final Cache<Integer,String> cache3 = cacheManager.getCache("non-exist-cache",Integer.class, String.class);
        assertNull(cache3);
    }

}
