/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link HazelcastCache} for timeout.
 *
 * @author Gokhan Oner
 */
@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"readtimeout-config.xml"})
@Category(QuickTest.class)
public class HazelcastCacheReadTimeoutTest extends HazelcastTestSupport{

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private IDummyTimeoutBean dummyTimeoutBean;

    private Cache delay150;
    private Cache delay50;
    private Cache delayNo;
    private Cache delay100;


    @BeforeClass
    @AfterClass
    public static void start() {
        System.setProperty(HazelcastCacheManager.CACHE_PROP, "defaultReadTimeout=100,delay150=150,delay50=50,delayNo=0");
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() {
        this.delay150 = cacheManager.getCache("delay150");
        this.delay50 = cacheManager.getCache("delay50");
        this.delayNo = cacheManager.getCache("delayNo");
        this.delay100 = cacheManager.getCache("delay100");

        //delay > readTimeout, throws exception
        ((IMap<?, ?>)this.delay150.getNativeCache()).addInterceptor(new DelayIMapGetInterceptor(200));
        //delay < readTimeout, get returns before timeout
        ((IMap<?, ?>)this.delay50.getNativeCache()).addInterceptor(new DelayIMapGetInterceptor(2));
        //cache block get operations, readTimeout 0.
        ((IMap<?, ?>)this.delayNo.getNativeCache()).addInterceptor(new DelayIMapGetInterceptor(300));
    }

    @Test
    public void testCache_TimeoutConfig() {
        assertEquals(150, ((HazelcastCache) delay150).getReadTimeout());
        assertEquals(50, ((HazelcastCache) delay50).getReadTimeout());
        assertEquals(0, ((HazelcastCache) delayNo).getReadTimeout());
        assertEquals(100, ((HazelcastCache) delay100).getReadTimeout());
    }

    @Test(expected = OperationTimeoutException.class)
    public void testCache_delay150() {
        delay150.get(createRandomKey());
    }

    @Test
    public void testCache_delay50() {
        String key = createRandomKey();
        long start = System.nanoTime();
        delay50.get(key);
        long time =  TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(time >= 2L);
    }

    @Test
    public void testCache_delayNo() {
        String key = createRandomKey();
        long start = System.nanoTime();
        delayNo.get(key);
        long time =  TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(time >= 300L);
    }

    @Test(expected = OperationTimeoutException.class)
    public void testBean_delay150() {
        dummyTimeoutBean.getDelay150(createRandomKey());
    }

    @Test
    public void testBean_delay50() {
        String key = createRandomKey();
        long start = System.nanoTime();
        dummyTimeoutBean.getDelay50(key);
        long time =  TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(time >= 2L);
    }

    @Test
    public void testBean_delayNo() {
        String key = createRandomKey();
        long start = System.nanoTime();
        dummyTimeoutBean.getDelayNo(key);
        long time =  TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(time >= 300L);
    }

    public static class DelayIMapGetInterceptor implements MapInterceptor {

        private final int delay;

        public DelayIMapGetInterceptor(int delay) {
            this.delay = delay;
        }

        @Override
        public Object interceptGet(Object value) {
            sleepMillis(delay);
            return null;
        }

        @Override
        public void afterGet(Object value) {

        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return null;
        }

        @Override
        public void afterPut(Object value) {

        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object value) {

        }
    }

    public static class DummyTimeoutBean implements IDummyTimeoutBean {

        @Override
        public Object getDelay150(String key) {
            return null;
        }

        @Override
        public Object getDelay50(String key) {
            return null;
        }

        @Override
        public Object getDelayNo(String key) {
            return null;
        }

        @Override
        public String getDelay100(String key) {
            return null;
        }
    }

    private String createRandomKey() {
        return UUID.randomUUID().toString();
    }
}
