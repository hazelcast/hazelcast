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

package com.hazelcast.map;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AsyncTest extends HazelcastTestSupport {

    private final String key = "key";
    private final String value1 = "value1";
    private final String value2 = "value2";

    @Test
    public void testGetAsync() throws Exception {
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        IMap<String, String> map = factory.newHazelcastInstance().getMap("testGetAsync");
        map.put(key, value1);
        Future<String> f1 = map.getAsync(key);
        TestCase.assertEquals(value1, f1.get());
    }

    @Test
    public void testPutAsync() throws Exception {
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        IMap<String, String> map = factory.newHazelcastInstance().getMap("map:test:putAsync");
        Future<String> f1 = map.putAsync(key, value1);
        String f1Val = f1.get();
        TestCase.assertNull(f1Val);
        Future<String> f2 = map.putAsync(key, value2);
        String f2Val = f2.get();
        TestCase.assertEquals(value1, f2Val);
    }

    @Test
    public void testPutAsyncWithTtl() throws Exception {
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        IMap<String, String> map = factory.newHazelcastInstance().getMap("map:test:putAsyncWithTtl");

        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, true);

        Future<String> f1 = map.putAsync(key, value1, 3, TimeUnit.SECONDS);
        String f1Val = f1.get();
        TestCase.assertNull(f1Val);
        assertEquals(value1, map.get(key));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(map.get(key));
    }

    @Test
    public void testRemoveAsync() throws Exception {
        IMap<String, String> map = createHazelcastInstance().getMap("map:test:removeAsync");
        // populate map
        map.put(key, value1);
        Future<String> f1 = map.removeAsync(key);
        TestCase.assertEquals(value1, f1.get());
    }

    @Test
    public void testRemoveAsyncWithImmediateTimeout() throws Exception {
       final IMap<String, String> map = createHazelcastInstance().getMap("map:test:removeAsync:timeout");
        // populate map
        map.put(key, value1);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                map.lock(key);
                latch.countDown();
            }
        }).start();
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        Future<String> f1 = map.removeAsync(key);
        try {
            assertEquals(value1, f1.get(0L, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            // expected
            return;
        }
        TestCase.fail("Failed to throw TimeoutException with zero timeout");
    }

    @Test
    public void testRemoveAsyncWithNonExistentKey() throws Exception {
       IMap<String, String> map = createHazelcastInstance().getMap("map:test:removeAsync:nonexistant");
        Future<String> f1 = map.removeAsync(key);
        TestCase.assertNull(f1.get());
    }
}
