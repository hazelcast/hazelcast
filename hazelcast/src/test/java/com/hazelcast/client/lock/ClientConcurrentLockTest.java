/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientConcurrentLockTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void concurrent_TryLockTest() throws InterruptedException {
        concurrent_LockTest(false);
    }

    @Test
    public void concurrent_TryLock_WithTimeOutTest() throws InterruptedException {
        concurrent_LockTest(true);
    }

    private void concurrent_LockTest(boolean tryLockWithTimeOut) {
        final ILock lock = client.getLock(randomString());
        final AtomicInteger upTotal = new AtomicInteger(0);
        final AtomicInteger downTotal = new AtomicInteger(0);

        LockTestThread[] threads = new LockTestThread[8];
        for (int i = 0; i < threads.length; i++) {
            LockTestThread t;

            if (tryLockWithTimeOut) {
                t = new TryLockWithTimeOutThread(lock, upTotal, downTotal);
            } else {
                t = new TryLockThread(lock, upTotal, downTotal);
            }
            t.start();
            threads[i] = t;
        }

        assertJoinable(threads);
        assertEquals("concurrent access to locked code caused wrong total", 0, upTotal.get() + downTotal.get());
    }

    static class TryLockThread extends LockTestThread {
        TryLockThread(ILock lock, AtomicInteger upTotal, AtomicInteger downTotal) {
            super(lock, upTotal, downTotal);
        }

        public void doRun() {
            if (lock.tryLock()) {
                work();
                lock.unlock();
            }
        }
    }

    static class TryLockWithTimeOutThread extends LockTestThread {
        TryLockWithTimeOutThread(ILock lock, AtomicInteger upTotal, AtomicInteger downTotal) {
            super(lock, upTotal, downTotal);
        }

        public void doRun() throws Exception {
            if (lock.tryLock(1, TimeUnit.MILLISECONDS)) {
                work();
                lock.unlock();
            }
        }
    }

    abstract static class LockTestThread extends Thread {

        private static final int ITERATIONS = 1000 * 10;

        private final Random random = new Random();

        protected final ILock lock;
        protected final AtomicInteger upTotal;
        protected final AtomicInteger downTotal;

        LockTestThread(ILock lock, AtomicInteger upTotal, AtomicInteger downTotal) {
            this.lock = lock;
            this.upTotal = upTotal;
            this.downTotal = downTotal;
        }

        public void run() {
            try {
                for (int i = 0; i < ITERATIONS; i++) {
                    doRun();
                }
            } catch (Exception e) {
                throw new RuntimeException("LockTestThread throws: ", e);
            }
        }

        abstract void doRun() throws Exception;

        protected void work() {
            int delta = random.nextInt(1000);
            upTotal.addAndGet(delta);
            downTotal.addAndGet(-delta);
        }
    }
}
