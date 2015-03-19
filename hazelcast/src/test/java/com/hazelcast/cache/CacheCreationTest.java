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
import com.hazelcast.config.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;

import static junit.framework.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheCreationTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hz1;
    private HazelcastInstance hz2;

    private HazelcastServerCachingProvider cachingProvider1;
    private HazelcastServerCachingProvider cachingProvider2;

    private CacheManager cacheManager1;
    private CacheManager cacheManager2;

    private final URL configUrl1 = getClass().getClassLoader().getResource("test-hazelcast-jcache.xml");

    @Before
    public void init() throws Exception{
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(configUrl1.getFile());
        Config hzConfig = configBuilder.build();

        factory = new TestHazelcastInstanceFactory(2);
        hz1 = factory.newHazelcastInstance(hzConfig);
        hz2 = factory.newHazelcastInstance(hzConfig);

        cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(hz1);
        cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(hz2);

        cacheManager1 = cachingProvider1.getCacheManager();
        cacheManager2 = cachingProvider2.getCacheManager();
    }

    @After
    public void cleanup() {
        cachingProvider1.close();
        factory.shutdownAll();
        HazelcastInstanceFactory.terminateAll();
        Caching.getCachingProvider().close();
    }

    @Test
    public void play1() throws URISyntaxException {
        Cache c = cacheManager1.getCache("j");
        c.get(1);
    }

    @Test
    public void play1a() throws URISyntaxException {
        int i=1;
        Cache c = cacheManager1.getCache("c"+i);
        c.get(1);
    }

    @Test
    public void play2() throws URISyntaxException {
        for(int i=0; i<10; i++){
            Cache c = cacheManager1.getCache("c"+i);
            c.get(1);
        }
    }

    @Test
    public void play3() throws URISyntaxException {

        int threadCount=5;

        final CountDownLatch latch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            new Thread() {
                public void run() {
                    for(int i=0; i<10; i++){
                        Cache c = cacheManager1.getCache("c"+i);
                        c.get(1);
                    }
                    latch.countDown();
                }
            }.start();
        }
        HazelcastTestSupport.assertOpenEventually(latch);
    }


    @Test
    public void play4() throws URISyntaxException {

        int threadCount=5;

        final CountDownLatch latch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            new Thread() {
                public void run() {

                    for(int i=0; i<10; i++){
                        Cache c;
                        if(i%2==0){
                            c = cacheManager1.getCache("c"+i);
                            c.get(1);
                        }else{
                            c = cacheManager2.getCache("c"+i);
                            c.get(1);
                        }
                        assertNotNull(c);
                    }

                    latch.countDown();
                }
            }.start();
        }
        HazelcastTestSupport.assertOpenEventually(latch);
    }

}
