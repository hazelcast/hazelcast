/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AsyncTest extends HazelcastTestSupport {

    private final String key = "key";
    private final String value1 = "value1";
    private final String value2 = "value2";

    protected HazelcastInstance instance;

    @Before
    public void setup() {
        instance = createHazelcastInstance(getConfig());
    }

    @Test
    public void testGetAsync() throws Exception {
        IMap<String, String> map = instance.getMap(randomString());
        map.put(key, value1);
        Future<String> f1 = map.getAsync(key).toCompletableFuture();
        assertEquals(value1, f1.get());
    }

    @Test
    public void testPutAsync() throws Exception {
        IMap<String, String> map = instance.getMap(randomString());
        Future<String> f1 = map.putAsync(key, value1).toCompletableFuture();
        String f1Val = f1.get();
        assertNull(f1Val);
        Future<String> f2 = map.putAsync(key, value2).toCompletableFuture();
        String f2Val = f2.get();
        assertEquals(value1, f2Val);
    }

    @Test
    public void testPutAsyncWithTtl() throws Exception {
        IMap<String, String> map = instance.getMap(randomString());

        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener((EntryExpiredListener<String, String>) event -> latch.countDown(), true);

        Future<String> f1 = map.putAsync(key, value1, 3, TimeUnit.SECONDS).toCompletableFuture();
        String f1Val = f1.get();
        assertNull(f1Val);
        assertEquals(value1, map.get(key));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(map.get(key));
    }

    @Test
    public void testPutIfAbsentAsync() throws Exception {
        MapProxyImpl<Object, Object> map = (MapProxyImpl<Object, Object>) instance.getMap(randomString());
        assertNull(map.putIfAbsentAsync(key, value1).toCompletableFuture().get());
        assertEquals(value1, map.putIfAbsentAsync(key, value2).toCompletableFuture().get());
        assertEquals(value1, map.putIfAbsentAsync(key, value2).toCompletableFuture().get());
    }

    @Test
    public void testPutIfAbsentAsyncWithTtl() throws Exception {
        MapProxyImpl<Object, Object> map = (MapProxyImpl<Object, Object>) instance.getMap(randomString());

        CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener((EntryExpiredListener<String, String>) event -> latch.countDown(), true);

        assertNull(map.putIfAbsentAsync(key, value1, 3, TimeUnit.SECONDS).toCompletableFuture().get());
        assertEquals(value1, map.get(key));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(map.get(key));
    }

    @Test
    public void testSetAsync() throws Exception {
        IMap<String, String> map = instance.getMap(randomString());
        Future<Void> f1 = map.setAsync(key, value1).toCompletableFuture();
        f1.get();
        assertEquals(value1, map.get(key));
        Future<Void> f2 = map.setAsync(key, value2).toCompletableFuture();
        f2.get();
        assertEquals(value2, map.get(key));
    }

    @Test
    public void testSetAsync_issue9599() throws Exception {
        IMap<String, String> map = instance.getMap(randomString());
        Future<Void> f = map.setAsync(key, value1).toCompletableFuture();

        // the return value was not of type Void, but Boolean. So assignment to Void would fail.
        Void v = f.get();
        assertNull(v);
    }

    @Test
    public void testSetAsyncWithTtl() throws Exception {
        IMap<String, String> map = instance.getMap(randomString());

        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener((EntryExpiredListener<String, String>) event -> latch.countDown(), true);

        Future<Void> f1 = map.setAsync(key, value1, 3, TimeUnit.SECONDS).toCompletableFuture();
        f1.get();
        assertEquals(value1, map.get(key));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(map.get(key));
    }

    @Test
    public void testRemoveAsync() throws Exception {
        IMap<String, String> map = instance.getMap(randomString());
        // populate map
        map.put(key, value1);
        Future<String> f1 = map.removeAsync(key).toCompletableFuture();
        assertEquals(value1, f1.get());
    }

    @Test
    public void testRemoveAsyncWithImmediateTimeout() throws Exception {
        final IMap<String, String> map = instance.getMap(randomString());
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
        Future<String> f1 = map.removeAsync(key).toCompletableFuture();
        try {
            assertEquals(value1, f1.get(0L, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            // expected
            return;
        }
        fail("Failed to throw TimeoutException with zero timeout");
    }

    @Test
    public void testRemoveAsyncWithNonExistentKey() throws Exception {
        IMap<String, String> map = instance.getMap(randomString());
        Future<String> f1 = map.removeAsync(key).toCompletableFuture();
        assertNull(f1.get());
    }
}
