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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMultiMapLockTest {

    private static HazelcastInstance client;

    private Object key;
    private MultiMap<Object, Object> multiMap;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() {
        key = randomString();
        multiMap = client.getMultiMap(randomString());
    }

    @Test
    public void testIsLocked_whenNotLocked() throws Exception {
        boolean result = multiMap.isLocked(key);
        
        assertFalse(result);
    }

    @Test
    public void testLock() throws Exception {
        multiMap.lock(key);
        
        assertTrue(multiMap.isLocked(key));
    }

    @Test(expected = NullPointerException.class)
    public void testLock_whenKeyNull() throws Exception {
        multiMap.lock(null);
    }

    @Test(expected = NullPointerException.class)
    public void testUnLock_whenNullKey() throws Exception {
        multiMap.unlock(null);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlock_whenNotLocked() throws Exception {
        multiMap.unlock("NOT_LOCKED");
    }

    @Test
    public void testUnLock() throws Exception {
        multiMap.lock(key);
        multiMap.unlock(key);

        assertFalse(multiMap.isLocked(key));
    }

    @Test
    public void testUnlock_whenRentrantlyLockedBySelf() throws Exception {
        multiMap.lock(key);
        multiMap.lock(key);
        multiMap.unlock(key);

        assertTrue(multiMap.isLocked(key));
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlock_whenLockedByOther() throws Exception {
        multiMap.lock(key);

        UnLockThread t = new UnLockThread(key, multiMap);
        t.start();

        assertJoinable(t);
        throw t.exception;
    }

    @Test
    public void testLock_whenAlreadyLockedBySelf() throws Exception {
        multiMap.lock(key);
        multiMap.lock(key);

        assertTrue(multiMap.isLocked(key));
    }

    @Test
    public void testTryLock() throws Exception {
        assertTrue(multiMap.tryLock(key));
    }

    @Test(expected = NullPointerException.class)
    public void testTryLock_whenNullKey() throws Exception {
        multiMap.tryLock(null);
    }

    @Test
    public void testTryLock_whenLockedBySelf() throws Exception {
        multiMap.lock(key);

        assertTrue(multiMap.tryLock(key));
    }

    @Test
    public void testTryLock_whenLockedByOther() throws Exception {
        multiMap.lock(key);

        final CountDownLatch tryLockFailed = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (!multiMap.tryLock(key)) {
                    tryLockFailed.countDown();
                }
            }
        }.start();
        assertOpenEventually(tryLockFailed);
    }

    @Test
    public void testTryLockWaitingOnLockedKey_thenKeyUnlockedByOtherThread() throws Exception {
        multiMap.lock(key);

        final CountDownLatch tryLockReturnsTrue = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (multiMap.tryLock(key, 10, TimeUnit.SECONDS)) {
                        tryLockReturnsTrue.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        multiMap.unlock(key);

        assertOpenEventually(tryLockReturnsTrue);
        assertTrue(multiMap.isLocked(key));
    }

    @Test(expected = NullPointerException.class)
    public void testForceUnlock_whenKeyNull() throws Exception {
        multiMap.forceUnlock(null);
    }

    @Test
    public void testForceUnlock_whenKeyLocked() throws Exception {
        multiMap.lock(key);
        multiMap.forceUnlock(key);

        assertFalse(multiMap.isLocked(key));
    }

    @Test
    public void testForceUnlock_whenKeyLockedTwice() throws Exception {
        multiMap.lock(key);
        multiMap.lock(key);
        multiMap.forceUnlock(key);

        assertFalse(multiMap.isLocked(key));
    }

    @Test
    public void testForceUnlock_whenKeyLockedByOther() throws Exception {
        multiMap.lock(key);

        final CountDownLatch forceUnlock = new CountDownLatch(1);
        new Thread() {
            public void run() {
                multiMap.forceUnlock(key);
                forceUnlock.countDown();
            }
        }.start();
        assertOpenEventually(forceUnlock);
        assertFalse(multiMap.isLocked(key));
    }

    @Test
    public void testForceUnlock_whenKeyLockedTwiceByOther() throws Exception {
        multiMap.lock(key);
        multiMap.lock(key);

        final CountDownLatch forceUnlock = new CountDownLatch(1);
        new Thread() {
            public void run() {
                multiMap.forceUnlock(key);
                forceUnlock.countDown();
            }
        }.start();
        assertOpenEventually(forceUnlock);
        assertFalse(multiMap.isLocked(key));
    }

    @Test
    public void testLockTTL() throws Exception {
        multiMap.lock(key, 30, TimeUnit.SECONDS);

        assertTrue(multiMap.isLocked(key));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLockTTL_whenZeroTimeout() throws Exception {
        multiMap.lock(key, 0, TimeUnit.SECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLockTTL_whenNegativeTimeout() throws Exception {
        multiMap.lock(key, -1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testLockTTL_whenLockedBySelf() throws Exception {
        multiMap.lock(key);
        multiMap.lock(key, 30, TimeUnit.SECONDS);

        assertTrue(multiMap.isLocked(key));
    }

    @Test
    public void testLockTTLExpired() throws Exception {
        multiMap.lock(key, 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        assertFalse(multiMap.isLocked(key));
    }

    @Test
    public void testLockTTLExpired_whenLockedBySelf() throws Exception {
        multiMap.lock(key);
        multiMap.lock(key, 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        assertFalse(multiMap.isLocked(key));
    }

    @Test
    public void testLockTTLExpires_thenOtherThreadCanObtain() throws Exception {
        multiMap.lock(key, 2, TimeUnit.SECONDS);

        final CountDownLatch tryLockSuccess = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (multiMap.tryLock(key, 4, TimeUnit.SECONDS)) {
                        tryLockSuccess.countDown();
                    }
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        }.start();

        assertOpenEventually(tryLockSuccess);
    }

    private static class UnLockThread extends Thread {
        private final Object key;
        private final MultiMap<Object, Object> multiMap;

        private Exception exception = null;

        public UnLockThread(Object key, MultiMap<Object, Object> multiMap) {
            this.key = key;
            this.multiMap = multiMap;
        }

        public void run() {
            try {
                multiMap.unlock(key);
            } catch (Exception e) {
                exception = e;
            }
        }
    }
}