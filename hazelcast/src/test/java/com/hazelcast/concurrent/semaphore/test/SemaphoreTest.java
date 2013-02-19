/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.semaphore.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * User: sancar
 * Date: 2/18/13
 * Time: 5:12 PM
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SemaphoreTest {

    @Test
    public void testSingleNode() {
        final int k = 1;
        final Config config = new Config();
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, k);

        ISemaphore semaphore = instances[0].getSemaphore("test");
        int numberOfPermits = 20;
        semaphore.init(numberOfPermits);
        try {
            for (int i = 0; i < numberOfPermits; i++) {
                Assert.assertEquals(numberOfPermits - i, semaphore.availablePermits());
                semaphore.acquire();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(semaphore.availablePermits(), 0);

        for (int i = 0; i < numberOfPermits; i++) {
            Assert.assertEquals(i, semaphore.availablePermits());
            semaphore.release();
        }

        semaphore.init(numberOfPermits);
        try {
            for (int i = 0; i < numberOfPermits; i += 5) {
                Assert.assertEquals(numberOfPermits - i, semaphore.availablePermits());
                semaphore.acquire(5);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(semaphore.availablePermits(), 0);

        for (int i = 0; i < numberOfPermits; i += 5) {
            Assert.assertEquals(i, semaphore.availablePermits());
            semaphore.release(5);
        }

        semaphore.init(numberOfPermits);
        try {
            semaphore.acquire(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int drainedPermits = semaphore.drainPermits();
        Assert.assertEquals(drainedPermits, numberOfPermits - 5);
        Assert.assertEquals(semaphore.availablePermits(), 0);

        semaphore.init(numberOfPermits);
        for (int i = 0; i < numberOfPermits; i += 5) {
            Assert.assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.reducePermits(5);
        }

        Assert.assertEquals(semaphore.availablePermits(), 0);


        semaphore.init(numberOfPermits);
        for (int i = 0; i < numberOfPermits; i++) {
            Assert.assertEquals(numberOfPermits - i, semaphore.availablePermits());
            Assert.assertEquals(semaphore.tryAcquire(), true);
        }

        Assert.assertEquals(semaphore.availablePermits(), 0);

        semaphore.init(numberOfPermits);
        for (int i = 0; i < numberOfPermits; i += 5) {
            Assert.assertEquals(numberOfPermits - i, semaphore.availablePermits());
            Assert.assertEquals(semaphore.tryAcquire(5), true);
        }

        Assert.assertEquals(semaphore.availablePermits(), 0);

    }

    @Test
    public void testMutex() {
        //Same test for java.util.concurrent.locks.Lock is right below this test (implemented to justify correctness of this test)
        final int k = 5;
        final Config config = new Config();
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, k);
        final Object sync = new Object();
        final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(k);

        instances[0].getSemaphore("test").init(1);

        for (int i = 0; i < k; i++) {
            final ISemaphore semaphore = instances[i].getSemaphore("test");
//            semaphore.init(1);
            new Thread() {
                public void run() {
                    for (int j = 0; j < 10000; j++) {
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            System.out.println("Acquire : " + e.getMessage());
                        }
                        synchronized (sync) {
                            Assert.assertEquals(atomicBoolean.compareAndSet(false, true), true);
                        }
                         //CRITICAL SECTION
                        synchronized (sync) {
                            semaphore.release();
                            atomicBoolean.set(false);
                        }
                    }
                    latch.countDown();
                }
            }.start();
        }
        try {
            Assert.assertEquals(latch.await(15, TimeUnit.SECONDS), true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMutexLock() {
        int k = 5;
        final Object sync = new Object();
        final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        final Lock lock = new ReentrantLock();
        final CountDownLatch latch = new CountDownLatch(k);
        for (int i = 0; i < k; i++) {
            new Thread() {
                public void run() {
                    for (int j = 0; j < 10000; j++) {
                        lock.lock();
                        synchronized (sync) {
                            Assert.assertEquals(atomicBoolean.compareAndSet(false, true), true);
                        }
                        //CRITICAL SECTION
                        synchronized (sync) {
                            lock.unlock();
                            atomicBoolean.set(false);
                        }
                    }
                    latch.countDown();
                }
            }.start();
        }
        try {
            Assert.assertEquals(latch.await(15, TimeUnit.SECONDS), true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


}
