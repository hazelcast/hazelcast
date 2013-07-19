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

package com.hazelcast.client;

import com.hazelcast.core.ILock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HazelcastClientLockTest extends HazelcastClientTestBase {

    @BeforeClass
    @AfterClass
    public static void before() {
        single.destroy();
    }

    @Test(expected = NullPointerException.class)
    public void testLockNull() {
        HazelcastClient hClient = getHazelcastClient();
        final ILock lock = hClient.getLock(null);
        lock.lock();
    }

    @Test
    public void testDestroy() {
        HazelcastClient hClient = getHazelcastClient();
        ILock lock = hClient.getLock("testDestroy");
        lock.destroy();
    }

    @Test
    public void testLockUnlock() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final ILock lock = hClient.getLock("testLockUnlock");
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch unlockLatch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                assertFalse(lock.tryLock());
                unlockLatch.countDown();
                lock.lock();
                latch.countDown();
            }
        }).start();
        assertTrue(unlockLatch.await(10, TimeUnit.SECONDS));
        lock.unlock();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testTryLock() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final ILock lock = hClient.getLock("testTryLock");
        assertTrue(lock.tryLock());
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch unlockLatch = new CountDownLatch(1);
        new Thread(new Runnable() {

            public void run() {
                assertFalse(lock.tryLock());
                unlockLatch.countDown();
                try {
                    assertTrue(lock.tryLock(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            }
        }).start();
        assertTrue(unlockLatch.await(10, TimeUnit.SECONDS));
        lock.unlock();
        lock.unlock();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test for issue #39
     */
    @Test
    public void testIsLocked() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final ILock lock = hClient.getLock("testIsLocked");
        assertFalse(lock.isLocked());
        lock.lock();
        assertTrue(lock.isLocked());

        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                assertTrue(lock.isLocked());
                try {
                    while (lock.isLocked()) {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            }
        });
        thread.start();
        Thread.sleep(100);
        lock.unlock();
        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

}
