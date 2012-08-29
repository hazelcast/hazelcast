/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.impl.GroupProperties;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SemaphoreTest {

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSemaphoreWithTimeout() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore = instance.getSemaphore("test");
        //Test acquire and timeout.
        try {
            assertEquals(10, semaphore.availablePermits());
            semaphore.tryAcquire();
            assertEquals(9, semaphore.availablePermits());
            assertEquals(false, semaphore.tryAcquire(10, 10, TimeUnit.MILLISECONDS));
            assertEquals(9, semaphore.availablePermits());
            semaphore.release();
            //Test acquire and timeout and check for partial acquisitions.
            assertEquals(10, semaphore.availablePermits());
            assertEquals(false, semaphore.tryAcquire(20, 10, TimeUnit.MILLISECONDS));
            assertEquals(10, semaphore.availablePermits());
        } catch (Throwable e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSimpleSemaphore() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 1);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore = instance.getSemaphore("test");
        assertEquals(1, semaphore.availablePermits());
        semaphore.tryAcquire();
        assertEquals(0, semaphore.availablePermits());
        semaphore.release();
        assertEquals(1, semaphore.availablePermits());
        semaphore.tryAcquire();
        assertEquals(0, semaphore.availablePermits());
        semaphore.release();
        assertEquals(1, semaphore.availablePermits());
        semaphore.tryAcquire();
        assertEquals(0, semaphore.availablePermits());
        semaphore.release();
        assertEquals(1, semaphore.availablePermits());
        semaphore.tryAcquire();
        assertEquals(0, semaphore.availablePermits());
        semaphore.release();
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testSemaphoreReducePermits() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore = instance.getSemaphore("test");
        assertEquals(10, semaphore.availablePermits());
        semaphore.reducePermits(1);
        assertEquals(9, semaphore.availablePermits());
        semaphore.tryAcquire(9);
        assertEquals(0, semaphore.availablePermits());
        semaphore.reducePermits(8);
        assertEquals(-8, semaphore.availablePermits());
        semaphore.release();
        assertEquals(-7, semaphore.availablePermits());
        semaphore.release(8);
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testSemaphoreDisconnect() throws InterruptedException {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 10);
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_CONNECTION_MONITOR_INTERVAL, "1");
        config.setProperty(GroupProperties.PROP_CONNECTION_MONITOR_MAX_FAULTS, "1");
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore1 = instance1.getSemaphore("test");
        ISemaphore semaphore2 = instance2.getSemaphore("test");
        assertEquals(10, semaphore1.availablePermits());
        semaphore1.tryAcquireAttach(5);
        semaphore1.reducePermits(1);
        instance1.getLifecycleService().kill();
        Thread.sleep(500);
        assertEquals(9, semaphore2.availablePermits());
    }

    @Test
    public void testSemaphorePeerDisconnect() throws InterruptedException {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore1 = instance1.getSemaphore("test");
        ISemaphore semaphore2 = instance2.getSemaphore("test");
        semaphore2.tryAcquireAttach(5);
        int result = semaphore1.availablePermits();
        int expectedResult = 5;
        assertEquals(expectedResult, result);
        final CountDownLatch latch = new CountDownLatch(1);
        instance1.getCluster().addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                latch.countDown();
            }
        });
        instance2.getLifecycleService().shutdown();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        expectedResult = 10;
        result = semaphore1.availablePermits();
        assertEquals(expectedResult, result);
    }

    @Test
    public void testAsyncAcquire() throws Exception {
        final ISemaphore semaphore = Hazelcast.getSemaphore("test");
        assertEquals(0, semaphore.availablePermits());
        final Future f1 = semaphore.acquireAsync();
        Thread thread = new Thread() {
            @Override
            public void run() {
                for (; ; ) {
                    try {
                        f1.get(1, TimeUnit.SECONDS);
                        break;
                    } catch (InterruptedException e) {
                        //expected
                        semaphore.release();
                        assertFalse(f1.cancel(false));
                        break;
                    } catch (ExecutionException e) {
                        // not gonna happen
                    } catch (TimeoutException e) {
                        // keep trying
                    }
                }
            }
        };
        assertEquals(0, semaphore.availablePermits());
        thread.start();
        Thread.sleep(3500);
        thread.interrupt();
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testSemaphoreIncreasePermits() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 1);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore = instance.getSemaphore("test");
        assertEquals(1, semaphore.availablePermits());
        semaphore.release();
        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    public void testSemaphoreTryAcquireTimeout() throws InterruptedException {
        ISemaphore semaphore = Hazelcast.getSemaphore("test");
        assertEquals(0, semaphore.availablePermits());
        try {
            semaphore.tryAcquire(5000, TimeUnit.MILLISECONDS);
        } catch (InstanceDestroyedException e) {
            e.printStackTrace();
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testMultiInstanceSemaphore() {
        final Random random = new Random();
        final int rndTimeMax = 20;
        int initialPermits = 2;
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(null);
        final ISemaphore semaphore1 = instance1.getSemaphore("test");
        final ISemaphore semaphore2 = instance2.getSemaphore("test");
        final ISemaphore semaphore3 = instance3.getSemaphore("test");
        semaphore1.release(initialPermits);
        assertEquals(initialPermits, semaphore1.availablePermits());
        assertEquals(initialPermits, semaphore2.availablePermits());
        assertEquals(initialPermits, semaphore3.availablePermits());
        Thread thread1 = new Thread() {
            public void run() {
                for (int i = 0; i < 100; i++) {
                    try {
                        semaphore1.acquire(2);
                        Thread.sleep(random.nextInt(rndTimeMax));
                        semaphore1.release(2);
                        Thread.sleep(random.nextInt(rndTimeMax));
                    } catch (InterruptedException e) {
                        fail(e.getMessage());
                    } catch (InstanceDestroyedException e) {
                        fail(e.getMessage());
                    }
                }
            }
        };
        Thread thread2 = new Thread() {
            public void run() {
                for (int i = 0; i < 200; i++) {
                    try {
                        semaphore2.acquire();
                        Thread.sleep(random.nextInt(rndTimeMax));
                        semaphore2.release();
                        Thread.sleep(random.nextInt(rndTimeMax));
                    } catch (InterruptedException e) {
                        fail(e.getMessage());
                    } catch (InstanceDestroyedException e) {
                        fail(e.getMessage());
                    }
                }
            }
        };
        Thread thread3 = new Thread() {
            public void run() {
                for (int i = 0; i < 300; i++) {
                    try {
                        semaphore3.acquire();
                        Thread.sleep(random.nextInt(rndTimeMax));
                        semaphore3.release();
                        Thread.sleep(random.nextInt(rndTimeMax));
                    } catch (InterruptedException e) {
                        fail(e.getMessage());
                    } catch (InstanceDestroyedException e) {
                        fail(e.getMessage());
                    }
                }
            }
        };
        thread1.start();
        thread2.start();
        thread3.start();
        try {
            thread1.join();
            thread2.join();
            thread3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        assertEquals(initialPermits, semaphore1.availablePermits());
        assertEquals(initialPermits, semaphore2.availablePermits());
        assertEquals(initialPermits, semaphore3.availablePermits());
    }
}
