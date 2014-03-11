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
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;
import static org.junit.Assert.*;

/**
 * @author ali 5/28/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientLockTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static ILock l;

    @BeforeClass
    public static void init() {
        Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient();
        l = hz.getLock(name);
    }

    @AfterClass
    public static void destroy() {
        hz.shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        l.forceUnlock();
    }

    @Test
    public void testLock() throws Exception {
        l.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (!l.tryLock()) {
                    latch.countDown();
                }
            }
        }.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        l.forceUnlock();
    }

    @Test
    public void testLockTtl() throws Exception {
        l.lock(3, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread() {
            public void run() {
                if (!l.tryLock()) {
                    latch.countDown();
                }
                try {
                    if (l.tryLock(5, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        l.forceUnlock();
    }

    @Test
    public void testTryLock() throws Exception {

        assertTrue(l.tryLock(2, TimeUnit.SECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (!l.tryLock(2, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        assertTrue(l.isLocked());

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (l.tryLock(20, TimeUnit.SECONDS)) {
                        latch2.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);
        l.unlock();
        assertTrue(latch2.await(100, TimeUnit.SECONDS));
        assertTrue(l.isLocked());
        l.forceUnlock();
    }

    @Test
    public void testForceUnlock() throws Exception {
        l.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                l.forceUnlock();
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertFalse(l.isLocked());
    }

    @Test
    public void testStats() throws InterruptedException {
        l.lock();
        assertTrue(l.isLocked());
        assertTrue(l.isLockedByCurrentThread());
        assertEquals(1, l.getLockCount());

        l.unlock();
        assertFalse(l.isLocked());
        assertEquals(0, l.getLockCount());
        assertEquals(-1L, l.getRemainingLeaseTime());

        l.lock(1, TimeUnit.MINUTES);
        assertTrue(l.isLocked());
        assertTrue(l.isLockedByCurrentThread());
        assertEquals(1, l.getLockCount());
        assertTrue(l.getRemainingLeaseTime() > 1000 * 30);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                assertTrue(l.isLocked());
                assertFalse(l.isLockedByCurrentThread());
                assertEquals(1, l.getLockCount());
                assertTrue(l.getRemainingLeaseTime() > 1000 * 30);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }


    @Test
    public void concurrent_TryLockTest() throws InterruptedException {
        AtomicInteger upTotal = new AtomicInteger(0);
        AtomicInteger downTotal = new AtomicInteger(0);

        final ILock lock = hz.getLock("concurrent_TryLockTest");
        Thread threads[] = new Thread[8];
        for ( int i=0; i<threads.length; i++ ) {
            Thread t = new TryLockThread(lock, upTotal, downTotal);
            t.start();
            threads[i] = t;
        }

        assertJoinable(threads);
        assertTrue("concurrent access to locked code caused wrong total", upTotal.get() + downTotal.get() == 0);
    }

    static class TryLockThread extends Thread{
        static protected final Random random = new Random();
        protected ILock lock;
        protected AtomicInteger upTotal;
        protected AtomicInteger downTotal;

        public TryLockThread(ILock lock, AtomicInteger upTotal, AtomicInteger downTotal){
            this.lock = lock;
            this.upTotal = upTotal;
            this.downTotal = downTotal;
        }

        public void run()  {
            for ( int i=0; i<1000*10; i++ ) {
                if(lock.tryLock()){
                    work();
                    lock.unlock();
                }
            }
        }

        protected void work(){
            int delta = random.nextInt(1000);
            upTotal.addAndGet(delta);
            downTotal.addAndGet(-delta);
        }
    }


    @Test
    public void concurrent_TryLock_WithTimeOutTest() throws InterruptedException {
        AtomicInteger upTotal = new AtomicInteger(0);
        AtomicInteger downTotal = new AtomicInteger(0);

        final ILock lock = hz.getLock("concurrent_TryLockWithTimeOutTest");
        Thread threads[] = new Thread[8];
        for ( int i=0; i<threads.length; i++ ) {
            Thread t = new TryLockWithTimeOutThread(lock, upTotal, downTotal);
            t.start();
            threads[i] = t;
        }

        assertJoinable(threads);
        assertTrue("concurrent access to locked code caused wrong total", upTotal.get() + downTotal.get() == 0);
    }

    static class TryLockWithTimeOutThread extends TryLockThread{

        public TryLockWithTimeOutThread(ILock lock, AtomicInteger upTotal, AtomicInteger downTotal){
            super(lock, upTotal, downTotal);
        }

        public void run()  {
            for ( int i=0; i<1000*10; i++ ) {
                try {
                    if(lock.tryLock(10, TimeUnit.MILLISECONDS)){
                        work();
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
