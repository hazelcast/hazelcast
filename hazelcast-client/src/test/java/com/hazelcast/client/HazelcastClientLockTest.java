/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */
package com.hazelcast.client;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import static com.hazelcast.client.TestUtility.getHazelcastClient;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class HazelcastClientLockTest {

     private HazelcastClient hClient;



    @After
    public void shutdownAll() throws InterruptedException{
    	Hazelcast.shutdownAll();
    	if(hClient!=null){	hClient.shutdown(); }
    	Thread.sleep(500);
    }

    @Test

    public void testLockUnlock() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        final ILock lock = hClient.getLock("Fuad");
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable(){

            public void run() {
                assertFalse(lock.tryLock());
                lock.lock();
                latch.countDown();

            }
        }).start();

        Thread.sleep(100);
        assertEquals(1, latch.getCount());

        lock.unlock();
        Thread.sleep(100);

        assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
    }


    @Test
    public void testTryLock() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        final ILock lock = hClient.getLock("Fuad");
        assertTrue(lock.tryLock());
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable(){

            public void run() {
                assertFalse(lock.tryLock());
                try {
                    assertTrue(lock.tryLock(200, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();

            }
        }).start();

        Thread.sleep(100);
        assertEquals(1, latch.getCount());

        lock.unlock();
        lock.unlock();

        Thread.sleep(100);
        assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
    }

}
