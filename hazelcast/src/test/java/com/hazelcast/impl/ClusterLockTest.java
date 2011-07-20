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

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Transaction;
import junit.framework.Assert;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.impl.TestUtil.getCMap;
import static junit.framework.Assert.*;
import static org.junit.Assert.assertFalse;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ClusterLockTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 100000)
    public void testScheduledLockActionForDeadMember() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map1 = h1.getMap("default");
        map1.put(1, 1);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map2 = h2.getMap("default");
        Assert.assertTrue(map1.tryLock(1));
        new Thread(new Runnable() {
            public void run() {
                try {
                    map2.lock(1);
                    fail("Shouldn't be able to lock!");
                } catch (Throwable e) {
                }
            }
        }).start();
        Thread.sleep(2000);
        h2.getLifecycleService().shutdown();
        Thread.sleep(2000);
        map1.unlock(1);
        Assert.assertTrue(map1.tryLock(1));
    }

    @Test(timeout = 100000)
    public void testLockOwnerDiesWaitingMemberObtains() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map1 = h1.getMap("default");
        map1.put(1, 1);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map2 = h2.getMap("default");
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map3 = h3.getMap("default");
        final CountDownLatch latchShutdown = new CountDownLatch(1);
        final CountDownLatch latchLock = new CountDownLatch(1);
        Assert.assertTrue(map2.tryLock(1));
        new Thread(new Runnable() {
            public void run() {
                try {
                    map3.lock(1);
                    latchShutdown.countDown();
                    assertTrue(latchLock.await(10, TimeUnit.SECONDS));
                    map3.unlock(1);
                } catch (Throwable e) {
                    fail(e.getMessage());
                }
            }
        }).start();
        Thread.sleep(2000);
        h2.getLifecycleService().shutdown();
        assertTrue(latchShutdown.await(10, TimeUnit.SECONDS));
        Assert.assertFalse(map1.tryLock(1));
        latchLock.countDown();
        Assert.assertTrue(map1.tryLock(1, 10, TimeUnit.SECONDS));
    }

    @Test(timeout = 100000)
    public void testKeyOwnerDies() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map1 = h1.getMap("default");
        map1.put(1, 1);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map2 = h2.getMap("default");
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map3 = h3.getMap("default");
        assertTrue(map2.tryLock(1));
        final CountDownLatch latchLock = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                try {
                    map3.lock(1);
                    assertTrue(latchLock.await(10, TimeUnit.SECONDS));
                } catch (Throwable e) {
                    fail();
                }
            }
        }).start();
        Thread.sleep(2000);
        h1.getLifecycleService().shutdown();
        Thread.sleep(2000);
        assertEquals(1, map2.put(1, 2));
        map2.unlock(1);
        latchLock.countDown();
        assertFalse(map2.tryLock(1));
    }

    @Test
    public void testUnusedLocks() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        IMap map2 = h2.getMap("default");
        for (int i = 0; i < 1000; i++) {
            map2.lock(i);
            map2.unlock(i);
        }
        CMap cmap1 = getCMap(h1, "default");
        CMap cmap2 = getCMap(h2, "default");
        Thread.sleep(15000);
        assertEquals(0, cmap1.mapRecords.size());
        assertEquals(0, cmap2.mapRecords.size());
    }

    @Test(timeout = 100000)
    public void testTransactionCommitRespectLockCount() throws InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map1 = h1.getMap("default");
        Transaction tx = h1.getTransaction();
        map1.lock(1);
        tx.begin();
        map1.put(1, 1);
        tx.commit();
        final AtomicBoolean locked = new AtomicBoolean(false);
        new Thread(new Runnable() {

            public void run() {
                try {
                    map1.lock(1);
                    locked.set(true);
                } catch (Throwable e) {
                }
            }
        }).start();
        Thread.sleep(2000);
        Assert.assertFalse("should not acquire lock", locked.get());
        map1.unlock(1);
    }

    @Test(timeout = 100000)
    public void testTransactionRollbackRespectLockCount() throws InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map1 = h1.getMap("default");
        Transaction tx = h1.getTransaction();
        map1.lock(1);
        tx.begin();
        map1.put(1, 1);
        tx.rollback();
        final AtomicBoolean locked = new AtomicBoolean(false);
        new Thread(new Runnable() {

            public void run() {
                try {
                    map1.lock(1);
                    locked.set(true);
                } catch (Throwable e) {
                }
            }
        }).start();
        Thread.sleep(2000);
        Assert.assertFalse("should not acquire lock", locked.get());
        map1.unlock(1);
    }

    @Test(timeout = 100000)
    public void testPutAndUnlockRespectLockCount() throws InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map1 = h1.getMap("default");
        map1.lock(1);
        map1.lock(1);
        map1.putAndUnlock(1, 1);
        final AtomicBoolean locked = new AtomicBoolean(false);
        new Thread(new Runnable() {

            public void run() {
                try {
                    map1.lock(1);
                    locked.set(true);
                } catch (Throwable e) {
                }
            }
        }).start();
        Thread.sleep(2000);
        Assert.assertFalse("should not acquire lock", locked.get());
        map1.unlock(1);
    }

    @Test(timeout = 100000)
    public void testUnlockInsideTransaction() throws InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map1 = h1.getMap("default");
        Transaction tx = h1.getTransaction();
        tx.begin();
        map1.put(1, 1);
        map1.lock(1);
        map1.unlock(1);
        final AtomicBoolean locked = new AtomicBoolean(false);
        new Thread(new Runnable() {

            public void run() {
                try {
                    map1.lock(1);
                    locked.set(true);
                } catch (Throwable e) {
                }
            }
        }).start();
        Thread.sleep(2000);
        Assert.assertFalse("should not acquire lock", locked.get());
        tx.commit();
    }
}
