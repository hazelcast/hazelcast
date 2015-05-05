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
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import javax.cache.Cache;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheCreationTest {

    static Config hzConfig;
    private static final int THREAD_COUNT = 4;

    @BeforeClass
    public static void init() throws Exception{
        final URL configUrl1 = CacheCreationTest.class.getClassLoader().getResource("test-hazelcast-real-jcache.xml");
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(configUrl1.getFile());
        hzConfig = configBuilder.build();
    }

    @Before
    @After
    public void killAllHazelcastInstances() throws Exception {
        HazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void creatingASingleCache() throws URISyntaxException {
        HazelcastServerCachingProvider cachingProvider = createCachingProvider(hzConfig);
        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache("xmlCache" + 1);
        cache.get(1);
    }

    @Test
    public void creatingASingleCacheFromMultiProviders() throws URISyntaxException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        final CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    HazelcastServerCachingProvider cachingProvider = createCachingProvider(hzConfig);
                    Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache("xmlCache");
                    cache.get(1);
                    latch.countDown();
                }
            });
        }
        HazelcastTestSupport.assertOpenEventually(latch);
        executorService.shutdown();
    }

    @Test
    public void creatingMultiCacheFromMultiProviders() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        final CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int finalI = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    HazelcastServerCachingProvider cachingProvider = createCachingProvider(hzConfig);
                    Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache("xmlCache"+finalI);
                    cache.get(1);
                    latch.countDown();
                }
            });
        }
        HazelcastTestSupport.assertOpenEventually(latch);
        executorService.shutdown();
    }

    private HazelcastServerCachingProvider createCachingProvider(Config hzConfig){
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(hzConfig);
        HazelcastServerCachingProvider cachingProvider =
                HazelcastServerCachingProvider.createCachingProvider(hazelcastInstance);
        return cachingProvider;
    }
}
