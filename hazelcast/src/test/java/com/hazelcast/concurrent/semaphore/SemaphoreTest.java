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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SemaphoreTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Before
    public void setUp() {
        hz = createHazelcastInstance();
    }

    @Test(timeout = 30000)
    public void testAcquire() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore(randomString());

        int numberOfPermits = 20;
        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.acquire();
        }

        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test(timeout = 30000)
    public void testAcquire_whenNoPermits() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore("testAcquire_whenNoPermits");
        semaphore.init(0);
        final AcquireThread acquireThread = new AcquireThread(semaphore);
        acquireThread.start();
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(acquireThread.isAlive());
                assertEquals(0, semaphore.availablePermits());
            }
        }, 5);
    }

    @Test(timeout = 30000)
    public void testAcquire_whenNoPermits_andSemaphoreDestroyed() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore("testRelease_whenBlockedAcquireThread");
        AcquireThread thread = new AcquireThread(semaphore);
        thread.start();

        semaphore.destroy();
        assertEquals(0, semaphore.availablePermits());
    }

    @Test(expected = IllegalStateException.class, timeout = 30000)
    public void testAcquire_whenInstanceShutdown() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore(randomString());
        hz.shutdown();
        semaphore.acquire();
    }

    @Test(timeout = 30000)
    public void testRelease() {
        final ISemaphore semaphore = hz.getSemaphore(randomString());

        int numberOfPermits = 20;
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(i, semaphore.availablePermits());
            semaphore.release();
        }

        assertEquals(semaphore.availablePermits(), numberOfPermits);
    }

    @Test(timeout = 30000)
    public void testRelease_whenArgumentNegative() {
        final ISemaphore semaphore = hz.getSemaphore("testRelease_whenArgumentNegative");
        try {
            semaphore.release(-5);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test(timeout = 30000)
    public void testRelease_whenBlockedAcquireThread() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore("testRelease_whenBlockedAcquireThread");
        semaphore.init(0);

        new Thread() {
            @Override
            public void run() {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        semaphore.release();
        assertEquals(0, semaphore.availablePermits());
    }

    @Test(timeout = 30000)
    public void testMultipleAcquire() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore(randomString());
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.acquire(5);
        }
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test(timeout = 30000)
    public void testMultipleAcquire_whenNegative() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore("testRelease_whenArgumentNegative");
        int numberOfPermits = 10;
        semaphore.init(numberOfPermits);
        try {
            for (int i = 0; i < numberOfPermits; i += 5) {
                semaphore.acquire(-5);
                fail();
            }
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(10, semaphore.availablePermits());

    }

    @Test(timeout = 30000)
    public void testMultipleAcquire_whenNotEnoughPermits() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore("testMultipleAcquire_whenNotEnoughPermits");
        int numberOfPermits = 5;
        semaphore.init(numberOfPermits);
        final Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    semaphore.acquire(6);
                    assertEquals(5, semaphore.availablePermits());
                    semaphore.acquire(6);
                    assertEquals(5, semaphore.availablePermits());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(thread.isAlive());
                assertEquals(5, semaphore.availablePermits());
            }
        }, 5);
    }

    @Test(timeout = 30000)
    public void testMultipleRelease() {
        final ISemaphore semaphore = hz.getSemaphore(randomString());
        int numberOfPermits = 20;

        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(i, semaphore.availablePermits());
            semaphore.release(5);
        }
        assertEquals(semaphore.availablePermits(), numberOfPermits);
    }

    @Test(timeout = 30000)
    public void testMultipleRelease_whenNegative() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore("testRelease_whenArgumentNegative");
        semaphore.init(0);

        try {
            semaphore.release(-5);
            fail();

        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Repeat(10)
    @Test(timeout = 30000)
    public void testMultipleRelease_whenBlockedAcquireThreads() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore("testRelease_whenBlockedAcquireThread");
        int numberOfPermits = 10;
        semaphore.init(numberOfPermits);
        semaphore.acquire(numberOfPermits);
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);
        Thread thread1 = new BlockAcquireThread(semaphore, latch1);
        thread1.start();
        Thread thread2 = new BlockAcquireThread(semaphore, latch2);
        thread2.start();
        Thread thread3 = new BlockAcquireThread(semaphore, latch3);
        thread3.start();

        semaphore.release();
        assertOpenEventually(latch1);
        semaphore.release();
        assertOpenEventually(latch2);
        semaphore.release();
        assertOpenEventually(latch3);
    }

    @Test(timeout = 30000)
    public void testDrain() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore(randomString());
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        semaphore.acquire(5);
        int drainedPermits = semaphore.drainPermits();
        assertEquals(drainedPermits, numberOfPermits - 5);
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test(timeout = 30000)
    public void testDrain_whenNoPermits() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore("testDrain_whenNoPermits");
        semaphore.init(0);
        assertEquals(0, semaphore.drainPermits());
    }

    @Test(timeout = 30000)
    public void testReduce() {
        final ISemaphore semaphore = hz.getSemaphore(randomString());
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.reducePermits(5);
        }

        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test(timeout = 30000)
    public void testReduce_whenArgumentNegative() {
        final ISemaphore semaphore = hz.getSemaphore("testReduce_whenArgumentNegative");
        try {
            semaphore.reducePermits(-5);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }


    @Test(timeout = 30000)
    public void testTryAcquire() {
        final ISemaphore semaphore = hz.getSemaphore(randomMapName());
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            assertEquals(semaphore.tryAcquire(), true);
        }
        assertFalse(semaphore.tryAcquire());
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test(timeout = 30000)
    public void testTryAcquireMultiple() {
        final ISemaphore semaphore = hz.getSemaphore(randomString());
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            assertEquals(semaphore.tryAcquire(5), true);
        }

        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test(timeout = 30000)
    public void testTryAcquireMultiple_whenArgumentNegative() {
        final ISemaphore semaphore = hz.getSemaphore(randomString());
        int negativePermits = -5;
        semaphore.init(0);
        try {
            semaphore.tryAcquire(negativePermits);
            fail();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test(timeout = 30000)
    public void testTryAcquire_whenNotEnoughPermits() throws InterruptedException {
        final ISemaphore semaphore = hz.getSemaphore(randomString());
        int numberOfPermits = 10;
        semaphore.init(numberOfPermits);
        semaphore.acquire(10);
        boolean result = semaphore.tryAcquire(1);

        assertFalse(result);
        assertEquals(0, semaphore.availablePermits());
    }

    @Test(timeout = 30000)
    public void testInit_whenNotIntialized() {
        ISemaphore semaphore = hz.getSemaphore(randomString());

        boolean result = semaphore.init(2);

        assertTrue(result);
        assertEquals(2, semaphore.availablePermits());
    }

    @Test(timeout = 30000)
    public void testInit_whenAlreadyIntialized() {
        ISemaphore semaphore = hz.getSemaphore(randomString());
        semaphore.init(2);

        boolean result = semaphore.init(4);

        assertFalse(result);
        assertEquals(2, semaphore.availablePermits());
    }


    private class AcquireThread extends Thread {
        ISemaphore semaphore;

        AcquireThread(ISemaphore semaphore) {
            this.semaphore = semaphore;
        }

        @Override
        public void run() {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private class BlockAcquireThread extends Thread {
        ISemaphore semaphore;
        CountDownLatch latch;

        BlockAcquireThread(ISemaphore semaphore, CountDownLatch latch) {
            this.semaphore = semaphore;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                semaphore.acquire();
                latch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
