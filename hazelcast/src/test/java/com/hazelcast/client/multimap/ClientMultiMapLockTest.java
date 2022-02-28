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

package com.hazelcast.client.multimap;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMultiMapLockTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @Test
    public void testIsLocked_whenNotLocked() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "KeyNotLocked";
        final boolean result = mm.isLocked(key);
        assertFalse(result);
    }

    @Test
    public void testLock() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        assertTrue(mm.isLocked(key));
    }

    @Test(expected = NullPointerException.class)
    public void testLock_whenKeyNull() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.lock(null);
    }

    @Test(expected = NullPointerException.class)
    public void testUnLock_whenNullKey() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.unlock(null);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlock_whenNotLocked() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.unlock("NOT_LOCKED");
    }

    @Test
    public void testUnLock() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "key";

        mm.lock(key);
        mm.unlock(key);
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testUnlock_whenRentrantlyLockedBySelf() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "key";

        mm.lock(key);
        mm.lock(key);
        mm.unlock(key);
        assertTrue(mm.isLocked(key));
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlock_whenLockedByOther() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "key";
        mm.lock(key);

        UnLockThread t = new UnLockThread(mm, key);
        t.start();
        assertJoinable(t);
        throw t.exception;
    }

    static class UnLockThread extends Thread {

        public Exception exception = null;
        public MultiMap mm = null;
        public Object key = null;

        UnLockThread(MultiMap mm, Object key) {
            this.mm = mm;
            this.key = key;
        }

        public void run() {
            try {
                mm.unlock(key);
            } catch (Exception e) {
                exception = e;
            }
        }
    }

    @Test
    public void testLock_whenAlreadyLockedBySelf() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        mm.lock(key);
        assertTrue(mm.isLocked(key));
    }

    @Test
    public void testTryLock() {
        final MultiMap mm = client.getMultiMap(randomString());
        Object key = "key";
        assertTrue(mm.tryLock(key));
    }

    @Test(expected = NullPointerException.class)
    public void testTryLock_whenNullKey() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.tryLock(null);
    }

    @Test
    public void testTryLock_whenLockedBySelf() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        assertTrue(mm.tryLock(key));
    }

    @Test
    public void testTryLock_whenLockedByOther() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key1";
        mm.lock(key);
        final CountDownLatch tryLockFailed = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (!mm.tryLock(key)) {
                    tryLockFailed.countDown();
                }
            }
        }.start();
        assertOpenEventually(tryLockFailed);
    }

    @Test
    public void testTryLockWaitingOnLockedKey_thenKeyUnlockedByOtherThread() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "keyZ";

        mm.lock(key);

        final CountDownLatch tryLockReturnsTrue = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (mm.tryLock(key, 10, TimeUnit.SECONDS)) {
                        tryLockReturnsTrue.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        mm.unlock(key);

        assertOpenEventually(tryLockReturnsTrue);
        assertTrue(mm.isLocked(key));
    }

    @Test(expected = NullPointerException.class)
    public void testForceUnlock_whenKeyNull() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.forceUnlock(null);
    }

    @Test
    public void testForceUnlock_whenKeyLocked() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        mm.forceUnlock(key);
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testForceUnlock_whenKeyLockedTwice() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        mm.lock(key);
        mm.forceUnlock(key);
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testForceUnlock_whenKeyLockedByOther() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "key";
        mm.lock(key);
        final CountDownLatch forceUnlock = new CountDownLatch(1);
        new Thread() {
            public void run() {
                mm.forceUnlock(key);
                forceUnlock.countDown();
            }
        }.start();
        assertOpenEventually(forceUnlock);
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testForceUnlock_whenKeyLockedTwiceByOther() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "key";
        mm.lock(key);
        mm.lock(key);
        final CountDownLatch forceUnlock = new CountDownLatch(1);
        new Thread() {
            public void run() {
                mm.forceUnlock(key);
                forceUnlock.countDown();
            }
        }.start();
        assertOpenEventually(forceUnlock);
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testLockTTL() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";

        mm.lock(key, 30, TimeUnit.SECONDS);
        assertTrue(mm.isLocked(key));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLockTTL_whenZeroTimeout() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key, 0, TimeUnit.SECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLockTTL_whenNegativeTimeout() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key, -1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testLockTTL_whenLockedBySelf() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        mm.lock(key, 30, TimeUnit.SECONDS);
        assertTrue(mm.isLocked(key));
    }

    @Test
    public void testLockTTLExpired() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";

        mm.lock(key, 1, TimeUnit.SECONDS);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(mm.isLocked(key));
            }
        });
    }

    @Test
    public void testLockTTLExpired_whenLockedBySelf() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";

        mm.lock(key);
        mm.lock(key, 1, TimeUnit.SECONDS);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(mm.isLocked(key));
            }
        });
    }

    @Test
    public void testLockTTLExpires_thenOtherThreadCanObtain() {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";

        mm.lock(key, 2, TimeUnit.SECONDS);
        final CountDownLatch tryLockSuccess = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (mm.tryLock(key, 10, TimeUnit.SECONDS)) {
                        tryLockSuccess.countDown();
                    }
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        }.start();
        assertOpenEventually(tryLockSuccess);
    }

    @Test(timeout = 60000)
    public void testTryLockLeaseTime_whenLockFree() throws InterruptedException {
        MultiMap multiMap = getMultiMapForLock();
        String key = randomString();
        boolean isLocked = multiMap.tryLock(key, 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);
    }

    @Test(timeout = 60000)
    public void testTryLockLeaseTime_whenLockAcquiredByOther() throws InterruptedException {
        final MultiMap multiMap = getMultiMapForLock();
        final String key = randomString();
        Thread thread = new Thread() {
            public void run() {
                multiMap.lock(key);
            }
        };
        thread.start();
        thread.join();

        boolean isLocked = multiMap.tryLock(key, 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        Assert.assertFalse(isLocked);
    }

    @Test
    public void testTryLockLeaseTime_lockIsReleasedEventually() throws InterruptedException {
        final MultiMap multiMap = getMultiMapForLock();
        final String key = randomString();
        multiMap.tryLock(key, 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Assert.assertFalse(multiMap.isLocked(key));
            }
        }, 30);
    }

    private MultiMap getMultiMapForLock() {
        return client.getMultiMap(randomString());
    }
}
