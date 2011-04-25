/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IMapAsyncTest {
    private final String key = "key";
    private final String value1 = "value1";
    private final String value2 = "value2";

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testGetAsync() throws Exception {
        IMap<String, String> map = Hazelcast.getMap("map:test:getAsync");
        map.put(key, value1);
        Future<String> f1 = map.getAsync(key);
        TestCase.assertEquals(value1, f1.get());
    }

    @Test
    public void testPutAsync() throws Exception {
        IMap<String, String> map = Hazelcast.getMap("map:test:putAsync");
        Future<String> f1 = map.putAsync(key, value1);
        String f1Val = f1.get();
        TestCase.assertNull(f1Val);
        Future<String> f2 = map.putAsync(key, value2);
        String f2Val = f2.get();
        TestCase.assertEquals(value1, f2Val);
    }

    @Test
    public void testRemoveAsync() throws Exception {
        IMap<String, String> map = Hazelcast.getMap("map:test:removeAsync");
        // populate map
        map.put(key, value1);
        Future<String> f1 = map.removeAsync(key);
        TestCase.assertEquals(value1, f1.get());
    }

    @Test
    public void testRemoveAsyncWithImmediateTimeout() throws Exception {
        final IMap<String, String> map = Hazelcast.getMap("map:test:removeAsync:timeout");
        // populate map
        map.put(key, value1);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                map.lock(key);
                latch.countDown();
            }
        }).start();
        latch.await();
        Future<String> f1 = map.removeAsync(key);
        try {
            TestCase.assertEquals(value1, f1.get(0L, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            // expected
            return;
        }
        TestCase.fail("Failed to throw TimeoutException with zero timeout");
    }

    @Test
    public void testRemoveAsyncWithNonExistantKey() throws Exception {
        IMap<String, String> map = Hazelcast.getMap("map:test:removeAsync:nonexistant");
        Future<String> f1 = map.removeAsync(key);
        TestCase.assertNull(f1.get());
    }
}
