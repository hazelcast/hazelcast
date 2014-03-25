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

package com.hazelcast.client.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMultiMapLockTest {

    static HazelcastInstance server;
    static HazelcastInstance client;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testIsLocked_whenNotLocked() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "KeyNotLocked";
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testLock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        assertTrue(mm.isLocked(key));
    }

    @Test(expected = NullPointerException.class)
    public void testLock_whenKeyNull() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.lock(null);
    }

    @Test
    public void testUnLock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "key";

        mm.lock(key);
        mm.unlock(key);
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testMultiLockCalls() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        mm.lock(key);
        assertTrue(mm.isLocked(key));
    }

    @Test
    public void testLockAndTryLock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        assertTrue(mm.tryLock(key));
    }

    @Test
    public void testLockTTL() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";

        mm.lock(key, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testLock_WithTryLock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key1";
        mm.lock(key);
        final CountDownLatch tryLockFailed = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (mm.tryLock(key) == false) {
                    tryLockFailed.countDown();
                }
            }
        }.start();
        assertTrue(tryLockFailed.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testLockTTTL_threaded() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";

        mm.lock(key, 2, TimeUnit.SECONDS);
        final CountDownLatch tryLockSuccess = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (mm.tryLock(key, 4, TimeUnit.SECONDS)) {
                        tryLockSuccess.countDown();
                    }
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        }.start();
        assertTrue(tryLockSuccess.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testUnLockThreaded() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "keyZ";

        mm.lock(key);

        final CountDownLatch tryLockReturnsTrue = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(mm.tryLock(key, 10, TimeUnit.SECONDS)){
                        tryLockReturnsTrue.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        sleepSeconds(1);
        mm.unlock(key);

        assertTrue(tryLockReturnsTrue.await(20, TimeUnit.SECONDS));
        assertTrue(mm.isLocked(key));
    }

    @Test
    public void testForceUnlock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "key";
        mm.lock(key);
        final CountDownLatch forceUnlock = new CountDownLatch(1);
        new Thread(){
            public void run() {
                mm.forceUnlock(key);
                forceUnlock.countDown();
            }
        }.start();
        assertTrue(forceUnlock.await(30, TimeUnit.SECONDS));
        assertFalse(mm.isLocked(key));
    }
}