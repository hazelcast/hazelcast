/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import static com.hazelcast.client.TestUtility.getHazelcastClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.hazelcast.core.ILock;

public class HazelcastClientLockTest {

    @Test (expected = NullPointerException.class)
    public void testLockNull(){
        HazelcastClient hClient = getHazelcastClient();
        final ILock lock = hClient.getLock(null);
        lock.lock();
    }

    @Test
    public void testLockUnlock() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();

        final ILock lock = hClient.getLock("testLockUnlock");
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
        HazelcastClient hClient = getHazelcastClient();

        final ILock lock = hClient.getLock("testTryLock");
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
