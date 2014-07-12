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

package com.hazelcast.client.lock;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ali 5/28/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientLockTest {

    private static final String name = "test";

    private static ILock lock;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        lock = client.getLock(name);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void reset() throws IOException {
        lock.forceUnlock();
    }

    @Test
    public void testLock() throws Exception {
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (!lock.tryLock()) {
                    latch.countDown();
                }
            }
        }.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        lock.forceUnlock();
    }

    @Test
    public void testLockTtl() throws Exception {
        lock.lock(3, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread() {
            public void run() {
                if (!lock.tryLock()) {
                    latch.countDown();
                }
                try {
                    if (lock.tryLock(5, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        lock.forceUnlock();
    }

    @Test
    public void testTryLock() throws Exception {

        assertTrue(lock.tryLock(2, TimeUnit.SECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (!lock.tryLock(2, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        assertTrue(lock.isLocked());

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (lock.tryLock(20, TimeUnit.SECONDS)) {
                        latch2.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);
        lock.unlock();
        assertTrue(latch2.await(100, TimeUnit.SECONDS));
        assertTrue(lock.isLocked());
        lock.forceUnlock();
    }

    @Test
    public void testForceUnlock() throws Exception {
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                lock.forceUnlock();
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertFalse(lock.isLocked());
    }

    @Test
    public void testStats() throws InterruptedException {
        lock.lock();
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());

        lock.unlock();
        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
        assertEquals(-1L, lock.getRemainingLeaseTime());

        lock.lock(1, TimeUnit.MINUTES);
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
        assertTrue(lock.getRemainingLeaseTime() > 1000 * 30);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                assertTrue(lock.isLocked());
                assertFalse(lock.isLockedByCurrentThread());
                assertEquals(1, lock.getLockCount());
                assertTrue(lock.getRemainingLeaseTime() > 1000 * 30);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }

    @Test
    public void testObtainLock_FromDiffClients() throws InterruptedException {

        HazelcastInstance clientA = HazelcastClient.newHazelcastClient();
        ILock lockA = clientA.getLock(name);
        lockA.lock();

        HazelcastInstance clientB = HazelcastClient.newHazelcastClient();
        ILock lockB = clientB.getLock(name);
        boolean lockObtained = lockB.tryLock();

        assertFalse("Lock obtained by 2 client ", lockObtained);
    }
}
