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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;


@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheCreationTest {

    private final URL configUrl1 = getClass().getClassLoader().getResource("test-hazelcast-real-jcache.xml");
    Config hzConfig;

    @Before
    public void init() throws Exception{
        Hazelcast.shutdownAll();
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(configUrl1.getFile());
        hzConfig = configBuilder.build();
    }

    @After
    public void cleanup() throws Exception{
        Hazelcast.shutdownAll();
    }

    @Test
    public void creatingASingleCache() throws URISyntaxException {

        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(hzConfig);
        HazelcastServerCachingProvider cachingProvider = HazelcastServerCachingProvider
                .createCachingProvider(hazelcastInstance);

        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache("xmlCache" + 1);
        cache.get(1);
    }


    @Test
    public void creatingASingleCacheFromMultiProviders() throws URISyntaxException {

        int threadCount=4;

        final CountDownLatch latch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int finalI = i;
            new Thread() {
                public void run() {
                    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(hzConfig);
                    HazelcastServerCachingProvider cachingProvider = HazelcastServerCachingProvider
                            .createCachingProvider(hazelcastInstance);

                    Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache("xmlCache" );
                    cache.get(1);
                    latch.countDown();
                }
            }.start();
        }
        HazelcastTestSupport.assertOpenEventually(latch);
    }

    @Test
    public void creatingMultiCacheFromMultiProviders() throws URISyntaxException {

        int threadCount=4;

        final CountDownLatch latch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int finalI = i;
            new Thread() {
                public void run() {
                    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(hzConfig);
                    HazelcastServerCachingProvider cachingProvider = HazelcastServerCachingProvider
                            .createCachingProvider(hazelcastInstance);

                    Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache("xmlCache"+finalI);
                    cache.get(1);
                    latch.countDown();
                }
            }.start();
        }
        HazelcastTestSupport.assertOpenEventually(latch);
    }
}
