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

package com.hazelcast.client.semaphore;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ali 5/24/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientSemaphoreTest {
    static HazelcastInstance client;
    static HazelcastInstance server;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSemaphoreInit() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        assertTrue(semaphore.init(10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSemaphoreNegInit() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(-1);
    }

    @Test
    public void testRelease() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(0);
        semaphore.release();
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testdrainPermits() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(10);
        assertEquals(10, semaphore.drainPermits());
    }

    @Test
    public void testAvailablePermits_AfterDrainPermits() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(10);
        semaphore.drainPermits();
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testTryAcquire_whenDrainPermits() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(10);
        semaphore.drainPermits();
        assertFalse(semaphore.tryAcquire());
    }

    @Test
    public void testAvailablePermits() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(10);
        assertEquals(10, semaphore.availablePermits());
    }

    @Test
    public void testAvailableReducePermits() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(10);
        semaphore.reducePermits(5);
        assertEquals(5, semaphore.availablePermits());
    }

    @Test
    public void testAvailableReducePermits_WhenZero() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(0);
        semaphore.reducePermits(1);
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testTryAcquire_whenAvailable() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(1);
        assertTrue(semaphore.tryAcquire());
    }

    @Test
    public void testTryAcquire_whenUnAvailable() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(0);
        assertFalse(semaphore.tryAcquire());
    }

    @Test
    public void testTryAcquire_whenAvailableWithTimeOut() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(1);
        assertTrue(semaphore.tryAcquire(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testTryAcquire_whenUnAvailableWithTimeOut() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(0);
        assertFalse(semaphore.tryAcquire(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testTryAcquireMultiPermits_whenAvailable() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(10);
        assertTrue(semaphore.tryAcquire(5));
    }

    @Test
    public void testTryAcquireMultiPermits_whenUnAvailable() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(5);
        assertFalse(semaphore.tryAcquire(10));
    }

    @Test
    public void testTryAcquireMultiPermits_whenAvailableWithTimeOut() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(10);
        assertTrue(semaphore.tryAcquire(5, 1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testTryAcquireMultiPermits_whenUnAvailableWithTimeOut() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(5);
        assertFalse(semaphore.tryAcquire(10, 1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testTryAcquire_afterRelease() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(0);
        semaphore.release();
        assertTrue(semaphore.tryAcquire());
    }

    @Test
    public void testMulitReleaseTryAcquire() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(0);
        semaphore.release(5);
        assertTrue(semaphore.tryAcquire(5));
    }

    @Test
    public void testAcquire_Threaded() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(0);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    semaphore.acquire();
                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);
        semaphore.release(2);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void tryAcquire_Threaded() throws Exception {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(0);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(semaphore.tryAcquire(1, 5, TimeUnit.SECONDS)){
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        semaphore.release(2);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, semaphore.availablePermits());
    }
}
