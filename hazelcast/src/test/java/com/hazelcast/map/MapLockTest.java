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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


/**
 * User: sancar
 * Date: 2/18/13
 * Time: 5:12 PM
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapLockTest {

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

        map.put(key, "value") ;
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

        map.put(key, "value") ;
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

    @Test
    /**
     * Test for issues #223, #228, #256
     */
    public void testMapPutLockAndRemove() throws InterruptedException {
        Config config = new Config() ;
        HazelcastInstance[] nodes = new HazelcastInstance[3];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = Hazelcast.newHazelcastInstance(config);
            nodes[i].getPartitionService().getPartition(0).getOwner();
        }

        final int loop = 1000;
        final int key = 1;
        final Thread[] threads = new Thread[nodes.length * 2];
        final CountDownLatch latch = new CountDownLatch(loop * threads.length);
        abstract class TestThread extends Thread {
            IMap<Integer, Object> map;

            protected TestThread(String name, final HazelcastInstance hazelcast) {
                super(name);
                map = hazelcast.getMap("test");
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

            threads[k++] = new TestThread("Remover-" + k, node) {
                void doRun() {
                    map.lock(key);
                    try {
                        map.remove(key);
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


    @Test(timeout = 1000 * 30)
    /**
     * Test for issue #268
     */
    public void testConcurrentTryLockAndGetWithMapStore() {
        Config config = new Config();
        final String name = "test";
        config.getMapConfig(name).setMapStoreConfig(
                new MapStoreConfig().setEnabled(true).setImplementation(new MapStoreTest.MapStoreAdaptor()));

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
                    gotTheLock = map.tryLock(key, 50, TimeUnit.MILLISECONDS);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
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
}
