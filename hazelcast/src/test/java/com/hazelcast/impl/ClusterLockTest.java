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

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.impl.MapStoreTest.MapStoreAdaptor;
import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.PartitionService;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.impl.TestUtil.getCMap;
import static com.hazelcast.impl.TestUtil.migrateKey;
import static com.hazelcast.nio.IOUtil.toData;
import static org.junit.Assert.*;

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
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map2 = h2.getMap("default");
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map3 = h3.getMap("default");
        map1.put(1, 1);
        migrateKey(1, h1, h2, 0);
        migrateKey(1, h1, h3, 1);
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
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map2 = h2.getMap("default");
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map3 = h3.getMap("default");
        CMap cmap1 = getCMap(h1, "default");
        CMap cmap2 = getCMap(h2, "default");
        CMap cmap3 = getCMap(h3, "default");
        Data dKey = toData(1);
        map1.put(1, 1);
        migrateKey(1, h1, h2, 0);
        migrateKey(1, h1, h3, 1);
        cmap1.startCleanup(true);
        assertTrue(h1.getPartitionService().getPartition(1).getOwner().equals(h2.getCluster().getLocalMember()));
        assertTrue(h3.getPartitionService().getPartition(1).getOwner().equals(h2.getCluster().getLocalMember()));
        assertTrue(h2.getPartitionService().getPartition(1).getOwner().localMember());
        assertTrue(map1.tryLock(1));

        final CountDownLatch latchLock = new CountDownLatch(1);
        final CountDownLatch latchUnlock = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                try {
                    map3.lock(1);
                    latchLock.countDown();
                    Thread.sleep(1000);
                    map3.unlock(1);
                    latchUnlock.countDown();
                } catch (Throwable e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            }
        }).start();
        Thread.sleep(2000);

        Record rec1 = cmap1.getRecord(dKey);
        Record rec2 = cmap2.getRecord(dKey);
        Record rec3 = cmap3.getRecord(dKey);
        assertNull(rec1);
        assertNotNull(rec2);
        assertNotNull(rec3);

        assertEquals(1, rec2.getScheduledActionCount());
        assertEquals(0, rec3.getScheduledActionCount());
        assertTrue(rec2.getScheduledActions().iterator().next().isValid());

        DistributedLock lock2 = rec2.getLock();
        DistributedLock lock3 = rec3.getLock();
        Assert.assertNotNull(lock2);
        Assert.assertNotNull(lock3);

        h2.getLifecycleService().shutdown();
        Thread.sleep(3000);

        assertEquals(h3.getCluster().getLocalMember(), h1.getPartitionService().getPartition(1).getOwner());
        assertEquals(h3.getCluster().getLocalMember(), h3.getPartitionService().getPartition(1).getOwner());

        assertEquals(1, map1.put(1, 2));
        Thread.sleep(5000); // scheduled action may be invalid because of backup copy, wait a little.
        rec3 = cmap3.getRecord(dKey);
        assertEquals(1, rec3.getScheduledActionCount());
        assertTrue(rec3.getScheduledActions().iterator().next().isValid());
        map1.unlock(1);
        assertTrue(latchLock.await(3, TimeUnit.SECONDS));
        lock3 = rec3.getLock();
        assertNotNull(lock3);
        assertEquals(lock3.getLockAddress(), ((MemberImpl) h3.getCluster().getLocalMember()).getAddress());
        assertEquals(1, lock3.getLockCount());
        assertFalse(map1.tryLock(1));
        assertTrue(latchUnlock.await(3, TimeUnit.SECONDS));
        assertTrue(map1.tryLock(1));
    }

    @Test
    public void testUnusedLocksOneNode() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_REMOVE_DELAY_SECONDS, "0");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        h1.getMap("default");
        IMap map1 = h1.getMap("default");
        CMap cmap1 = getCMap(h1, "default");
        for (int i = 0; i < 1000; i++) {
            map1.lock(i);
            map1.unlock(i);
        }
        Thread.sleep(cmap1.removeDelayMillis + 100);
        assertTrue(cmap1.startCleanup(true));
        Thread.sleep(1000);
        assertEquals(0, cmap1.mapRecords.size());
        for (int i = 0; i < 1000; i++) {
            map1.lock(i);
        }
        Thread.sleep(cmap1.removeDelayMillis + 100);
        assertTrue(cmap1.startCleanup(true));
        Thread.sleep(1000);
        assertEquals(1000, cmap1.mapRecords.size());
    }

    @Test
    public void testUnusedLocks() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_REMOVE_DELAY_SECONDS, "0");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        h1.getMap("default");
        IMap map2 = h2.getMap("default");
        for (int i = 0; i < 1000; i++) {
            map2.lock(i);
            map2.unlock(i);
        }
        CMap cmap1 = getCMap(h1, "default");
        CMap cmap2 = getCMap(h2, "default");
        Thread.sleep(cmap1.removeDelayMillis + 100);
        assertTrue(cmap1.startCleanup(true));
        assertTrue(cmap2.startCleanup(true));
        Thread.sleep(1000);
        assertEquals(0, cmap1.mapRecords.size());
        assertEquals(0, cmap2.mapRecords.size());
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
        final BlockingQueue<Boolean> q = new LinkedBlockingQueue<Boolean>();
        new Thread(new Runnable() {

            public void run() {
                try {
                    q.put(map1.tryLock(1));
                } catch (Throwable e) {
                }
            }
        }).start();
        Boolean locked = q.poll(5, TimeUnit.SECONDS);
        assertNotNull(locked);
        Assert.assertFalse("should not acquire lock", locked);
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
        final BlockingQueue<Boolean> q = new LinkedBlockingQueue<Boolean>();
        new Thread(new Runnable() {

            public void run() {
                try {
                    q.put(map1.tryLock(1));
                } catch (Throwable e) {
                }
            }
        }).start();
        Boolean locked = q.poll(5, TimeUnit.SECONDS);
        assertNotNull(locked);
        Assert.assertFalse("should not acquire lock", locked);
        tx.commit();
    }

    /**
     * Test for Issue 710
     */
    @Test
    public void testEvictedEntryNotNullAfterLockAndGet() throws Exception {
        String mapName = "testLock";
        Config config = new XmlConfigBuilder().build();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setTimeToLiveSeconds(3);
        config.addMapConfig(mapConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> m1 = h1.getMap(mapName);
        m1.put(1, 1);
        assertEquals(1, m1.get(1));
        Thread.sleep(5000);
        assertEquals(null, m1.get(1));
        m1.lock(1);
        assertEquals(null, m1.get(1));
        m1.put(1, 1);
        assertEquals(1, m1.get(1));
    }

    /**
     * Test for issue #166.
     */
    @Test
    public void testAbstractRecordLockConcurrentAccess() throws InterruptedException {
        final Record record = new AbstractRecord(null, 0, null, 0, 0, 1) {
            public Record copy() {
                return null;
            }

            public Object getValue() {
                return null;
            }

            public Data getValueData() {
                return null;
            }

            public Object setValue(final Object value) {
                return null;
            }

            public void setValueData(final Data value) {
            }

            public int valueCount() {
                return 0;
            }

            public long getCost() {
                return 0;
            }

            public boolean hasValueData() {
                return false;
            }

            public void invalidate() {
            }

            protected void invalidateValueCache() {
            }
        };
        final AtomicBoolean run = new AtomicBoolean(true);
        final Thread serviceThread = new Thread() {
            public void run() {
                try {
                    final Address address = new Address("localhost", 5000);
                    while (run.get()) {
                        record.lock(1, address);
                        record.unlock(1, address);
                        record.clearLock();
                    }
                } catch (Exception e) {
                    run.set(false);
                    e.printStackTrace();
                }
            }
        };
        final int loop = 100000;
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicReference<Exception> error = new AtomicReference<Exception>();
        final Thread executorThread = new Thread() {
            public void run() {
                for (int i = 0; i < loop && run.get(); i++) {
                    try {
                        record.isEvictable();
                        record.isLocked();
                        record.isRemovable();
                        count.incrementAndGet();
                    } catch (Exception e) {
                        error.set(e);
                        e.printStackTrace();
                        break;
                    }
                }
            }
        };
        serviceThread.start();
        executorThread.start();
        executorThread.join();
        run.set(false);
        serviceThread.join();
        Assert.assertEquals("Error: " + error.get(), loop, count.get());
    }

    @Test
    public void testLockWhenMemberDiesAfterPutAndUnlock() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
        final Object key = createKeyOwnedByInstance(hz.getPartitionService(),
                hz2.getCluster().getLocalMember());
        final IMap map = hz.getMap("testLockWhenMemberDiesAfterPutAndUnlock");
        map.lock(key);
        map.putAndUnlock(key, "value");
        hz2.getLifecycleService().shutdown();
        Thread.sleep(1000);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (map.tryLock(key)) {
                    latch.countDown();
                } else {
                    fail("Could not acquire lock!");
                }
            }
        }.start();
        assertTrue("Backup of putAndUnlock is wrong!", latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testLockWhenMemberDiesAfterTxPut() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
        final Object key = createKeyOwnedByInstance(hz.getPartitionService(),
                hz2.getCluster().getLocalMember());
        final IMap map = hz.getMap("testLockWhenMemberDiesAfterTxPut");
        Transaction tx = hz.getTransaction();
        tx.begin();
        map.put(key, "value");
        tx.commit();
        hz2.getLifecycleService().shutdown();
        Thread.sleep(1000);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (map.tryLock(key)) {
                    latch.countDown();
                } else {
                    fail("Could not acquire lock!");
                }
            }
        }.start();
        assertTrue("Backup of tx put is wrong!", latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testLockWhenMemberDiesAfterTxRemove() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
        final Object key = createKeyOwnedByInstance(hz.getPartitionService(),
                hz2.getCluster().getLocalMember());
        final IMap map = hz.getMap("testLockWhenMemberDiesAfterTxRemove");
        map.put(key, "value");
        Transaction tx = hz.getTransaction();
        tx.begin();
        map.remove(key, "value");
        tx.commit();
        hz2.getLifecycleService().shutdown();
        Thread.sleep(1000);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (map.tryLock(key)) {
                    latch.countDown();
                } else {
                    fail("Could not acquire lock!");
                }
            }
        }.start();
        assertTrue("Backup of tx remove is wrong!", latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testLockWhenMemberDiesAfterTxRemoveAndPut() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
        final Object key = createKeyOwnedByInstance(hz.getPartitionService(),
                hz2.getCluster().getLocalMember());
        final IMap map = hz.getMap("testLockWhenMemberDiesAfterTxRemove");
        map.put(key, "value");
        Transaction tx = hz.getTransaction();
        tx.begin();
        map.remove(key, "value");
        map.put(key, "value2");
        tx.commit();
        hz2.getLifecycleService().shutdown();
        Thread.sleep(1000);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (map.tryLock(key)) {
                    latch.countDown();
                } else {
                    fail("Could not acquire lock!");
                }
            }
        }.start();
        assertTrue("Backup of tx remove and put is wrong!", latch.await(3, TimeUnit.SECONDS));
    }

    private int createKeyOwnedByInstance(PartitionService ps, final Member member) {
        int id = 1;
        while (!member.equals(ps.getPartition(id).getOwner())) {
            id++;
        }
        return id;
    }

    @Test
    public void testLockInterruption() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_FORCE_THROW_INTERRUPTED_EXCEPTION, "true");
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        final Lock lock = hz.getLock("test");
        Random rand = new Random();
        for (int i = 0; i < 30; i++) {
            Thread t = new Thread() {
                public void run() {
                    try {
                        lock.lock();
                        sleep(1);
                    } catch (InterruptedException e) {
                        System.err.println(e.getMessage());
                    } finally {
                        lock.unlock();
                    }
                }
            };
            t.start();
            Thread.sleep(rand.nextInt(3));
            t.interrupt();
            t.join();
            if (!lock.tryLock(3, TimeUnit.SECONDS)) {
                fail("Could not acquire lock!");
            } else {
                lock.unlock();
            }
            Thread.sleep(100);
        }
    }

    @Test
    public void testLockInterruption2() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_FORCE_THROW_INTERRUPTED_EXCEPTION, "true");
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        final Lock lock = hz.getLock("test");
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    lock.tryLock(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    System.err.println(e);
                } finally {
                    lock.unlock();
                }
            }
        });
        lock.lock();
        t.start();
        Thread.sleep(250);
        t.interrupt();
        Thread.sleep(1000);
        lock.unlock();
        Thread.sleep(500);
        assertTrue("Could not acquire lock!", lock.tryLock());
    }

    /**
     * Test for issue #39
     */
    @Test
    public void testIsMapKeyLocked() throws InterruptedException {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        final IMap map = h1.getMap("testIsMapKeyLocked");
        final IMap map2 = h2.getMap("testIsMapKeyLocked");
        assertFalse(map.isLocked("key"));
        assertFalse(map2.isLocked("key"));
        map.lock("key");
        assertTrue(map.isLocked("key"));
        assertTrue(map2.isLocked("key"));
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                IMap map3 = h3.getMap("testIsMapKeyLocked");
                assertTrue(map3.isLocked("key"));
                try {
                    while (map3.isLocked("key")) {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            }
        });
        thread.start();
        Thread.sleep(100);
        map.unlock("key");
        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    /**
     * Test for issue #39
     */
    @Test
    public void testLockIsLocked() throws InterruptedException {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        final ILock lock = h1.getLock("testLockIsLocked");
        final ILock lock2 = h2.getLock("testLockIsLocked");
        assertFalse(lock.isLocked());
        assertFalse(lock2.isLocked());
        lock.lock();
        assertTrue(lock.isLocked());
        assertTrue(lock2.isLocked());
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                ILock lock3 = h3.getLock("testLockIsLocked");
                assertTrue(lock3.isLocked());
                try {
                    while (lock3.isLocked()) {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            }
        });
        thread.start();
        Thread.sleep(100);
        lock.unlock();
        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    /**
     * Test for issues #223, #228, #256
     */
    public void testMapPutLockAndRemove() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_FORCE_THROW_INTERRUPTED_EXCEPTION, "true");
        HazelcastInstance[] nodes = new HazelcastInstance[3];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = Hazelcast.newHazelcastInstance(config);
            nodes[i].getPartitionService().getPartition(0).getOwner();
        }
        final int loop = 1000;
        final int key = 1;
        final Thread[] threads = new Thread[nodes.length * 3];
        final CountDownLatch latch = new CountDownLatch(loop * threads.length);
        abstract class TestThread extends Thread {
            MultiMap<Integer, Object> map;

            protected TestThread(String name, final HazelcastInstance hazelcast) {
                super(name);
                map = hazelcast.getMultiMap("test");
            }

            public final void run() {
                Random random = new Random();
                for (int i = 0; i < loop; i++) {
                    doRun();
                    latch.countDown();
                    try {
                        Thread.sleep(random.nextInt(10));
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }

            abstract void doRun();
        }
        int k = 0;
        for (final HazelcastInstance node : nodes) {
            threads[k++] = new TestThread("Putter-" + k, node) {
                void doRun() {
                    UUID uuid = UUID.randomUUID();
                    map.lock(key);
                    try {
                        map.put(key, uuid);
                    } finally {
                        map.unlock(key);
                    }
                }
            };
            threads[k++] = new TestThread("Remover.A-" + k, node) {
                void doRun() {
                    map.lock(key);
                    try {
                        map.remove(key);
                    } finally {
                        map.unlock(key);
                    }
                }
            };
            threads[k++] = new TestThread("Remover.B-" + k, node) {
                void doRun() {
                    map.lock(key);
                    try {
                        Collection values = map.get(key);
                        for (Object value : values) {
                            map.remove(key, value);
                        }
                    } finally {
                        map.unlock(key);
                    }
                }
            };
        }
        for (Thread thread : threads) {
            thread.start();
        }
        assertTrue("Remaining operations: " + latch.getCount(),
                latch.await(60, TimeUnit.SECONDS));
        for (Thread thread : threads) {
            thread.interrupt();
        }
    }

    @Test(timeout = 1000 * 100)
    /**
     * Test for issue 267
     */
    public void testHighConcurrentLockAndUnlock() {
        Config config = new Config();
        // increases chance of reproduce issue
        config.setProperty(GroupProperties.PROP_CLEANUP_DELAY_SECONDS, "1");
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        final String key = "key";
        final int threadCount = 100;
        final int lockCountPerThread = 5000;
        final int locks = 50;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final AtomicInteger totalCount = new AtomicInteger();
        class LockTest implements Runnable {
            public void run() {
                boolean live = true;
                Random rand = new Random();
                try {
                    for (int j = 0; j < lockCountPerThread && live; j++) {
                        final Lock lock = hz.getLock(key + rand.nextInt(locks));
                        lock.lock();
                        try {
                            totalCount.incrementAndGet();
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            break;
                        } finally {
                            try {
                                lock.unlock();
                            } catch (Exception e) {
                                e.printStackTrace();
                                live = false;
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            }
        }
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(new LockTest());
        }
        try {
            assertTrue("Lock tasks stuck!", latch.await(60, TimeUnit.SECONDS));
            assertEquals((threadCount * lockCountPerThread), totalCount.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                hz.getLifecycleService().kill();
            } catch (Throwable ignored) {
            }
            executorService.shutdownNow();
        }
    }

    @Test(timeout = 1000 * 30)
    /**
     * Test for issue #268
     */
    public void testConcurrentTryLockAndGetWithMapStore() {
        Config config = new Config();
        final String name = "test";
        config.getMapConfig(name).setMapStoreConfig(
                new MapStoreConfig().setEnabled(true).setImplementation(new MapStoreAdaptor()));
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        final IMap map = hz.getMap(name);
        final String key = "key";
        final int taskCount = 25;
        final CountDownLatch latch = new CountDownLatch(taskCount);
        class TryLockAndGetRunnable implements Runnable {
            volatile boolean gotTheLock = false;

            boolean gotTheLock() {
                return gotTheLock;
            }

            public void run() {
                try {
                    try {
                        map.tryLockAndGet(key, 50, TimeUnit.MILLISECONDS);
                        gotTheLock = true;
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {
                        }
                    } catch (TimeoutException ignored) {
                        // can not acquire lock
                    } finally {
                        if (gotTheLock) {
                            map.unlock(key);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            }
        }
        TryLockAndGetRunnable[] tasks = new TryLockAndGetRunnable[taskCount];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new TryLockAndGetRunnable();
        }
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (TryLockAndGetRunnable task : tasks) {
            executorService.execute(task);
        }
        try {
            assertTrue("TryLockAndGetRunnable tasks stuck: " + latch.getCount(),
                    latch.await(10, TimeUnit.SECONDS));
            int lockCount = 0;
            for (TryLockAndGetRunnable task : tasks) {
                lockCount += (task.gotTheLock() ? 1 : 0);
            }
            assertEquals("Multiple threads got the lock!", 1, lockCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void testLockDestroyAndTryLock() throws InterruptedException {
        final String lockObject = "testLockDestroyAndTryLock";
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        ILock lock = h.getLock(lockObject);
        Thread t = new Thread(new Runnable() {
            public void run() {
                ILock lock = h2.getLock(lockObject);
                lock.lock();
                lock.unlock();
                lock.destroy();
            }
        });
        t.start();
        t.join();
        assertTrue(lock.tryLock());
    }

    @Test
    public void testForceUnlock() throws InterruptedException {
        final String lockObject = "testLockDestroyAndTryLock";
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        ILock lock = h.getLock(lockObject);
        lock.lock();
        Thread t = new Thread(new Runnable() {
            public void run() {
                ILock lock = h2.getLock(lockObject);
                lock.forceUnlock();
                assertFalse(lock.isLocked());
            }
        });
        t.start();
        t.join();
        //This line fails, but it shouldn't
        //The reason is ForceUnlock doesn't clean the LocalLock on the node that actually did the lock.
//        assertFalse(lock.isLocked());
        new Thread(new Runnable() {
            public void run() {
                final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
                ILock lock = h2.getLock(lockObject);
                assertFalse(lock.isLocked());
            }
        });
    }
}

