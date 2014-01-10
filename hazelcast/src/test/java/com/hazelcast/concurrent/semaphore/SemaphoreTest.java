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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


/**
 * User: sancar
 * Date: 2/18/13
 * Time: 5:12 PM
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SemaphoreTest extends HazelcastTestSupport {

    @Test
    public void testAcquire(){
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore = instance1.getSemaphore("testAcquire");

        int numberOfPermits = 20;
        assertTrue(semaphore.init(numberOfPermits));
        try {
            for (int i = 0; i < numberOfPermits; i++) {
                Assert.assertEquals(numberOfPermits - i, semaphore.availablePermits());
                semaphore.acquire();
            }
        } catch (InterruptedException e) {
            fail();
        }
        Assert.assertEquals(semaphore.availablePermits(), 0);

    }

    @Test
    public void testRelease(){
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore = instance1.getSemaphore("testRelease");

        int numberOfPermits = 20;
        for (int i = 0; i < numberOfPermits; i++) {
            Assert.assertEquals(i, semaphore.availablePermits());
            semaphore.release();
        }

        Assert.assertEquals(semaphore.availablePermits(), numberOfPermits);
    }

    @Test
    public void testMultipleAcquire(){
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore = instance1.getSemaphore("testMultipleAcquire");
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        try {
            for (int i = 0; i < numberOfPermits; i += 5) {
                Assert.assertEquals(numberOfPermits - i, semaphore.availablePermits());
                semaphore.acquire(5);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testMultipleRelease(){
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore = instance1.getSemaphore("testMultipleRelease");
        int numberOfPermits = 20;

        for (int i = 0; i < numberOfPermits; i += 5) {
            Assert.assertEquals(i, semaphore.availablePermits());
            semaphore.release(5);
        }
        Assert.assertEquals(semaphore.availablePermits(), numberOfPermits);
    }

    @Test
    public void testDrain(){
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore = instance1.getSemaphore("testDrain");
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        try {
            semaphore.acquire(5);
        } catch (InterruptedException e) {
            fail();
        }
        int drainedPermits = semaphore.drainPermits();
        Assert.assertEquals(drainedPermits, numberOfPermits - 5);
        Assert.assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testReduce(){
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore = instance1.getSemaphore("testReduce");
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            Assert.assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.reducePermits(5);
        }

        Assert.assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testTryAcquire(){
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore = instance1.getSemaphore("testTryAcquire");
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            assertEquals(semaphore.tryAcquire(), true);
        }
        assertFalse(semaphore.tryAcquire());
        Assert.assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testTryAcquireMultiple(){
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore = instance1.getSemaphore("testTryAcquireMultiple");
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            Assert.assertEquals(numberOfPermits - i, semaphore.availablePermits());
            Assert.assertEquals(semaphore.tryAcquire(5), true);
        }

        Assert.assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testMutex() {
        final int k = 5;
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final CountDownLatch latch = new CountDownLatch(k);
        final int loopCount = 1000;

        class Counter {
            int count = 0;
            void inc() {count++;}
            int get() {return count;}
        }
        final Counter counter = new Counter();

        assertTrue(instances[0].getSemaphore("test").init(1));

        for (int i = 0; i < k; i++) {
            final ISemaphore semaphore = instances[i].getSemaphore("test");
            new Thread() {
                public void run() {
                    for (int j = 0; j < loopCount; j++) {
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            return;
                        }
                        try {
                            sleep((int) (Math.random() * 3));
                            counter.inc();
                        } catch (InterruptedException e) {
                            return;
                        } finally {
                            semaphore.release();
                        }
                    }
                    latch.countDown();
                }
            }.start();
        }
        try {
            assertTrue(latch.await(60, TimeUnit.SECONDS));
            Assert.assertEquals(loopCount * k, counter.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSemaphoreWithFailures() throws InterruptedException {
        final int k = 4;
        final Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k + 1);
        final HazelcastInstance[] instances = factory.newInstances(config);

        final ISemaphore semaphore = instances[k].getSemaphore("test");
        int initialPermits = 20;
        semaphore.init(initialPermits);
        for (int i = 0; i < k; i++) {

            int rand = (int) (Math.random() * 5) + 1;
            semaphore.acquire(rand);
            initialPermits -= rand;
            Assert.assertEquals(initialPermits, semaphore.availablePermits());
            semaphore.release(rand);
            initialPermits += rand;
            Assert.assertEquals(initialPermits, semaphore.availablePermits());

            instances[i].getLifecycleService().shutdown();

            semaphore.acquire(rand);
            initialPermits -= rand;
            Assert.assertEquals(initialPermits, semaphore.availablePermits());
            semaphore.release(rand);
            initialPermits += rand;
            Assert.assertEquals(initialPermits, semaphore.availablePermits());
        }
    }

    @Test
    public void testSemaphoreWithFailuresAndJoin() {
        final Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final ISemaphore semaphore = instance1.getSemaphore("test");
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        assertTrue(semaphore.init(0));

        final Thread thread = new Thread() {
            public void run() {
                for (int i = 0; i < 2; i++) {
                    try {
                        semaphore.acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                countDownLatch.countDown();
            }
        };
        thread.start();

        instance2.getLifecycleService().shutdown();
        semaphore.release();
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        ISemaphore semaphore1 = instance3.getSemaphore("test");
        semaphore1.release();
        try {
            assertTrue(countDownLatch.await(15, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            thread.interrupt();
        }
    }

    @Test
    public void testSemaphoreInit(){
        final Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        final ISemaphore sm1 = instance1.getSemaphore("sm");
        assertTrue(sm1.init(5));
        assertFalse(sm1.init(10));
        assertEquals(5, sm1.drainPermits());
        assertFalse(sm1.init(10));

        final ISemaphore sm2 = instance2.getSemaphore("sm");
        assertFalse(sm2.init(10));
        assertEquals(0, sm2.availablePermits());


        assertTrue(instance1.getSemaphore("test").init(2));
        assertFalse(instance2.getSemaphore("test").init(4));
        assertEquals(2, instance2.getSemaphore("test").availablePermits());

    }
}
