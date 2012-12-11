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

package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.InstanceDestroyedException;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.TestUtility.newHazelcastClient;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HazelcastClientSemaphoreTest {

    @Before
    @After
    public void after() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
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
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("test", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HazelcastClient client = newHazelcastClient(instance);
        ISemaphore semaphore = client.getSemaphore("test");
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
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } catch (com.hazelcast.core.InstanceDestroyedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSimpleSemaphore() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("test", 1);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HazelcastClient client = newHazelcastClient(instance);
        ISemaphore semaphore = client.getSemaphore("test");
        assertEquals(1, semaphore.availablePermits());
        semaphore.tryAcquire();
        assertEquals(0, semaphore.availablePermits());
        semaphore.release();
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testSemaphoreReducePermits() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("test", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HazelcastClient client = newHazelcastClient(instance);
        ISemaphore semaphore = client.getSemaphore("test");
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
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HazelcastClient client1 = newHazelcastClient(instance);
        HazelcastClient client2 = newHazelcastClient(instance);
        ISemaphore semaphore1 = client1.getSemaphore("test");
        ISemaphore semaphore2 = client2.getSemaphore("test");
        assertEquals(10, semaphore1.availablePermits());
        semaphore1.tryAcquireAttach(5);
        semaphore2.tryAcquire(3);
        semaphore1.reducePermits(1);
        assertEquals(1, semaphore2.availablePermits());
        client1.shutdown();
        assertEquals(6, semaphore2.availablePermits());
    }

    @Test
    public void testSemaphorePeerDisconnect() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HazelcastClient client1 = newHazelcastClient(instance);
        HazelcastClient client2 = newHazelcastClient(instance);
        ISemaphore semaphore1 = client1.getSemaphore("test");
        ISemaphore semaphore2 = client2.getSemaphore("test");
        semaphore2.tryAcquireAttach(5);
        semaphore2.tryAcquire(3);
        semaphore2.detach(2);
        assertEquals(2, semaphore1.availablePermits());
        client2.shutdown();
        assertEquals(5, semaphore1.availablePermits());
    }

    @Test
    public void testSemaphoreIncreasePermits() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 1);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HazelcastClient client = newHazelcastClient(instance);
        ISemaphore semaphore = client.getSemaphore("test");
        assertEquals(1, semaphore.availablePermits());
        semaphore.release();
        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    public void testAsyncAcquire() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = newHazelcastClient(instance);
        final ISemaphore semaphore = client.getSemaphore("test");
        final Future f1 = semaphore.acquireAsync();
        Thread thread = new Thread() {
            @Override
            public void run() {
                for (; ; ) {
                    try {
                        f1.get();
                        break;
                    } catch (InterruptedException e) {
                        // thread is interrupted but still trying to acquire
                        assertTrue(f1.cancel(false));
                        semaphore.release();
                        break;
                    } catch (ExecutionException e) {
                    }
                }
            }
        };
        thread.start();
        Thread.sleep(1000);
        thread.interrupt();
        thread.join();
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testMultiInstanceSemaphore() {
        // three threads, two permits
        // thread 1: client acquire 2 permits 100 times
        // thread 2: client acquire 1 permits 200 times
        // thread 3: instance acquire 1 permits 300 times
        // test permit count never goes below zero and end count is same as start count
        final Random random = new Random();
        final int rndTimeMax = 20;
        int initialPermits = 2;
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client1 = newHazelcastClient(instance);
        HazelcastClient client2 = newHazelcastClient(instance);
        final ISemaphore semaphore1 = client1.getSemaphore("test");
        final ISemaphore semaphore2 = client2.getSemaphore("test");
        final ISemaphore semaphore3 = instance.getSemaphore("test");
        semaphore1.release(initialPermits);
        assertEquals(initialPermits, semaphore1.availablePermits());
        assertEquals(initialPermits, semaphore2.availablePermits());
        assertEquals(initialPermits, semaphore3.availablePermits());
        Thread thread1 = new Thread() {
            public void run() {
                for (int i = 0; i < 100; i++) {
                    try {
                        semaphore1.acquire(2);
                        if (semaphore3.availablePermits() < 0)
                            fail();
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
                        if (semaphore1.availablePermits() < 0)
                            fail();
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
                        if (semaphore2.availablePermits() < 0)
                            fail();
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
