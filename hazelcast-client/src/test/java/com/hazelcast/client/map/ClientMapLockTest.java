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

package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.*;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.*;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMapLockTest {

    static HazelcastInstance client;
    static HazelcastInstance server;

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

    @Test(expected = NullPointerException.class)
    public void testisLocked_whenKeyNull_fromSameThread() {
        final IMap map = client.getMap(randomString());
        map.isLocked(null);
    }

    @Test
    public void testisLocked_whenKeyAbsent_fromSameThread() {
        final IMap map = client.getMap(randomString());
        boolean isLocked = map.isLocked("NOT_THERE");
        assertFalse(isLocked);
    }

    @Test
    public void testisLocked_whenKeyPresent_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final Object key ="key";
        map.put(key, "value");
        final boolean isLocked = map.isLocked(key);
        assertFalse(isLocked);
    }

    @Test(expected = NullPointerException.class)
    public void testLock_whenKeyNull_fromSameThread() {
        final IMap map = client.getMap(randomString());
        map.lock(null);
    }

    @Test
    public void testLock_whenKeyAbsent_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final Object key = "key";
        map.lock(key);
        assertTrue(map.isLocked(key));
    }

    @Test
    public void testLock_whenKeyPresent_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final Object key ="key";
        map.put(key, "value");
        map.lock(key);
        assertTrue(map.isLocked(key));
    }

    @Test
    public void testLock_whenLockedRepeatedly_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final Object key ="key";

        map.lock(key);
        map.lock(key);
        assertTrue(map.isLocked(key));
    }

    @Test(expected = NullPointerException.class)
    public void testUnLock_whenKeyNull_fromSameThread() {
        final IMap map = client.getMap(randomString());
        map.unlock(null);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnLock_whenKeyNotPresentAndNotLocked_fromSameThread() {
        final IMap map = client.getMap(randomString());
        map.unlock("NOT_THERE_OR_LOCKED");
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnLock_whenKeyPresentAndNotLocked_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        map.put(key, "value");
        map.unlock(key);
    }

    @Test
    public void testUnLock_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        map.lock(key);
        map.unlock(key);
        assertFalse(map.isLocked(key));
    }

    @Test
    public void testUnLock_whenKeyLockedRepeatedly_fromSameThread()  {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        map.lock(key);
        map.lock(key);
        map.unlock(key);
        assertTrue(map.isLocked(key));
    }

    @Test(expected = NullPointerException.class)
    public void testForceUnlock_whenKeyNull_fromSameThread()  {
        final IMap map = client.getMap(randomString());
        map.forceUnlock(null);
    }

    @Test
    public void testForceUnlock_whenKeyNotPresentAndNotLocked_fromSameThread() {
        final IMap map = client.getMap(randomString());
        map.forceUnlock("NOT_THERE_OR_LOCKED");
    }

    @Test
    public void testForceUnlock_whenKeyPresentAndNotLocked_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        map.put(key, "value");
        map.forceUnlock(key);
    }

    @Test
    public void testForceUnlock_fromSameThread(){
        final IMap map = client.getMap(randomString());
        final String key = "key";
        map.lock(key);
        map.forceUnlock(key);
        assertFalse(map.isLocked(key));
    }

    @Test
    public void testForceUnLock_whenKeyLockedRepeatedly_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        map.lock(key);
        map.lock(key);
        map.unlock(key);
        assertTrue(map.isLocked(key));
    }

    @Test
    public void testLockAbsentKey_thenPutKey_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String value = "value";
        map.lock(key);
        map.put(key, value);
        map.unlock(key);

        assertEquals(value, map.get(key));
    }

    @Test
    public void testLockAbsentKey_thenPutKeyIfAbsent_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String value = "value";
        map.lock(key);
        map.putIfAbsent(key, value);
        map.unlock(key);

        assertEquals(value, map.get(key));
    }

    @Test
    public void testLockPresentKey_thenPutKey_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String oldValue = "oldValue";
        final String newValue = "newValue";

        map.put(key, oldValue);
        map.lock(key);
        map.put(key, newValue);
        map.unlock(key);

        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testLockPresentKey_thenSetKey_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String oldValue = "oldValue";
        final String newValue = "newValue";

        map.put(key, oldValue);
        map.lock(key);
        map.set(key, newValue);
        map.unlock(key);

        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testLockPresentKey_thenReplace_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String oldValue = "oldValue";
        final String newValue = "newValue";

        map.put(key, oldValue);
        map.lock(key);
        map.replace(key, newValue);
        map.unlock(key);

        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testLockPresentKey_thenRemoveKey_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String oldValue = "oldValue";

        map.put(key, oldValue);
        map.lock(key);
        map.remove(key);
        map.unlock(key);

        assertFalse(map.isLocked(key));
        assertEquals(null, map.get(key));
    }

    @Test
    public void testLockPresentKey_thenDeleteKey_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String oldValue = "oldValue";

        map.put(key, oldValue);
        map.lock(key);
        map.delete(key);
        map.unlock(key);

        assertFalse(map.isLocked(key));
        assertEquals(null, map.get(key));
    }

    @Test
    public void testLockPresentKey_thenEvictKey_fromSameThread() {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String oldValue = "oldValue";

        map.put(key, oldValue);
        map.lock(key);
        map.evict(key);
        map.unlock(key);

        assertFalse(map.isLocked(key));
        assertEquals(null, map.get(key));
    }

    @Test
    public void testLockKey_thenPutAndCheckKeySet_fromOtherThread() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String value = "oldValue";

        final CountDownLatch putWhileLocked = new CountDownLatch(1);
        final CountDownLatch checkingKeySet = new CountDownLatch(1);

        new Thread() {
            public void run() {
                try {
                    map.lock(key);
                    map.put(key, value);
                    putWhileLocked.countDown();
                    checkingKeySet.await();
                    map.unlock(key);
                }catch(Exception e){}
            }
        }.start();

        putWhileLocked.await();
        Set keySet = map.keySet();
        assertFalse(keySet.isEmpty());
        checkingKeySet.countDown();
    }

    @Test
    public void testLockKey_thenPutAndGet_fromOtherThread() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String value = "oldValue";

        final CountDownLatch putWhileLocked = new CountDownLatch(1);
        final CountDownLatch checkingKeySet = new CountDownLatch(1);

        new Thread() {
            public void run() {
                try {
                    map.lock(key);
                    map.put(key, value);
                    putWhileLocked.countDown();
                    checkingKeySet.await();
                    map.unlock(key);
                }catch(Exception e){}
            }
        }.start();

        putWhileLocked.await();
        assertEquals(value, map.get(key));
        checkingKeySet.countDown();
    }

    @Test
    public void testLockKey_thenRemoveAndGet_fromOtherThread() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String value = "oldValue";

        final CountDownLatch removeWhileLocked = new CountDownLatch(1);
        final CountDownLatch checkingKey = new CountDownLatch(1);
        map.put(key, value);

        new Thread() {
            public void run() {
                try {
                    map.lock(key);
                    map.remove(key);
                    removeWhileLocked.countDown();
                    checkingKey.await();
                    map.unlock(key);
                }catch(Exception e){}
            }
        }.start();

        removeWhileLocked.await();
        assertEquals(null, map.get(key));
        checkingKey.countDown();
    }

    @Test
    public void testLockKey_thenTryPutOnKey() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String value = "value";

        map.put(key, value);
        map.lock(key);

        final CountDownLatch tryPutReturned = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.tryPut(key, "NEW_VALUE", 1, TimeUnit.SECONDS);
                tryPutReturned.countDown();
            }
        }.start();

        assertOpenEventually(tryPutReturned);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testLockTTLExpires_usingIsLocked() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        map.lock(key, 3, TimeUnit.SECONDS);

        final boolean isLockedBeforeSleep = map.isLocked(key);
        sleepSeconds(4);
        final boolean isLockedAfterSleep = map.isLocked(key);

        assertTrue(isLockedBeforeSleep);
        assertFalse(isLockedAfterSleep);
    }

    @Test
    public void testLockTTLExpires() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String oldValue = "value";
        final String newValue = "NEW_VALUE";

        map.put(key, oldValue);
        map.lock(key, 4, TimeUnit.SECONDS);

        final CountDownLatch tryPutReturned = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.tryPut(key, newValue, 8, TimeUnit.SECONDS);
                tryPutReturned.countDown();
            }
        }.start();

        assertOpenEventually(tryPutReturned);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testLockTTLExpires_onAbsentKey() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";
        final String value = "value";

        map.lock(key, 4, TimeUnit.SECONDS);

        final CountDownLatch tryPutReturned = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.tryPut(key, value, 8, TimeUnit.SECONDS);
                tryPutReturned.countDown();
            }
        }.start();

        assertOpenEventually(tryPutReturned);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testisLocked_whenLockedFromOtherThread() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";

        final CountDownLatch lockedLatch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.lock(key);
                lockedLatch.countDown();
            }
        }.start();

        assertOpenEventually(lockedLatch);
        assertTrue(map.isLocked(key));
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnLocked_whenLockedFromOtherThread() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";

        final CountDownLatch lockedLatch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.lock(key);
                lockedLatch.countDown();
            }
        }.start();

        assertOpenEventually(lockedLatch);
        map.unlock(key);
    }

    @Test
    public void testForceUnLocked_whenLockedFromOtherThread() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";

        final CountDownLatch lockedLatch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.lock(key);
                map.lock(key);
                lockedLatch.countDown();
            }
        }.start();

        lockedLatch.await(10, TimeUnit.SECONDS);
        map.forceUnlock(key);
        assertFalse(map.isLocked(key));
    }

    @Test
    public void testTryPut_whenLockedFromOtherThread() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";

        final CountDownLatch lockedLatch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.lock(key);
                lockedLatch.countDown();
            }
        }.start();

        assertOpenEventually(lockedLatch);
        assertFalse(map.tryPut(key, "value", 1, TimeUnit.SECONDS));
    }

    @Test
    public void testTryRemove_whenLockedFromOtherThread() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";

        final CountDownLatch lockedLatch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.lock(key);
                lockedLatch.countDown();
            }
        }.start();

        assertOpenEventually(lockedLatch);
        assertFalse(map.tryRemove(key, 1, TimeUnit.SECONDS));
    }

    @Test
    public void testTryLock_whenLockedFromOtherThread() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";

        final CountDownLatch lockedLatch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.lock(key);
                lockedLatch.countDown();
            }
        }.start();

        assertOpenEventually(lockedLatch);
        assertFalse(map.tryLock(key));
    }

    @Test
    public void testLock_whenUnLockedFromOtherThread() throws Exception {
        final IMap map = client.getMap(randomString());
        final String key = "key";

        map.lock(key);

        final CountDownLatch beforeLock = new CountDownLatch(1);
        final CountDownLatch afterLock = new CountDownLatch(1);
        new Thread() {
            public void run() {
                beforeLock.countDown();
                map.lock(key);
                afterLock.countDown();
            }
        }.start();

        beforeLock.await();
        map.unlock(key);
        assertOpenEventually(afterLock);
    }
}
