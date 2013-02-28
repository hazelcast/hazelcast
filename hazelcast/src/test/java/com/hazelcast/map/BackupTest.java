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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.util.Clock;
import org.junit.After;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

public class BackupTest {

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    /**
     * I should NOT shutdown before I backup everything
     */
    @Test
    public void testGracefulShutdown() throws Exception {
        int size = 100000;
        StaticNodeFactory nodeFactory = new StaticNodeFactory(5);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(new Config());
        IMap m1 = h1.getMap("default");
        for (int i = 0; i < size; i++) {
            m1.put(i, i);
        }
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        IMap m2 = h2.getMap("default");
        h1.getLifecycleService().shutdown();
        Thread.sleep(1000);
        assertEquals(size, m2.size());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        IMap m3 = h3.getMap("default");
        h2.getLifecycleService().shutdown();
        assertEquals(size, m3.size());
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(new Config());
        h4.getLifecycleService().shutdown();
        assertEquals(size, m3.size());
    }

    @Test
    public void testGracefulShutdown2() throws Exception {
        int size = 10000;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        IMap m1 = h1.getMap("default");
        for (int i = 0; i < size; i++) {
            m1.put(i, i);
        }
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        IMap m2 = h2.getMap("default");
        IMap m3 = h3.getMap("default");
        h1.getLifecycleService().shutdown();
        assertEquals(size, m2.size());
        h2.getLifecycleService().shutdown();
        assertEquals(size, m3.size());
    }


    @Test
    public void issue395BackupProblemWithBCount2() throws InterruptedException {
        final int size = 1000;
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_MIGRATION_INTERVAL, "0");
        config.getMapConfig("default").setBackupCount(2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }
        assertEquals(size, getTotalOwnedEntryCount(map1, map2));
        assertEquals(size, getTotalBackupEntryCount(map1, map2));
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        IMap map3 = h3.getMap("default");
        // we should wait here a little,
        // since copying 2nd backup is not immediate.
        // we have set 'PROP_PARTITION_MIGRATION_INTERVAL' to 0.
        Thread.sleep(1000 * 3);
        assertEquals(size, getTotalOwnedEntryCount(map1, map2, map3));
        assertEquals(2 * size, getTotalBackupEntryCount(map1, map2, map3));
    }

    /**
     * Fix for the issue 275.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testDataRecovery2() throws Exception {
        final int size = 1000;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        IMap map1 = h1.getMap("default");
        for (int i = 0; i < size; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(size, map1.size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        sleep(3000);
        IMap map2 = h2.getMap("default");
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, getTotalOwnedEntryCount(map1, map2));
        assertEquals(size, getTotalBackupEntryCount(map1, map2));
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        IMap map3 = h3.getMap("default");
        sleep(3000);
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        assertEquals(size, getTotalOwnedEntryCount(map1, map2, map3));
        assertEquals(size, getTotalBackupEntryCount(map1, map2, map3));
        MemberImpl member2 = (MemberImpl) h2.getCluster().getLocalMember();
        h2.getLifecycleService().shutdown();
        sleep(5000);
        assertEquals(size, map1.size());
        assertEquals(size, map3.size());
        assertEquals(size, getTotalOwnedEntryCount(map1, map3));
        assertEquals(size, getTotalBackupEntryCount(map1, map3));
        MemberImpl member1 = (MemberImpl) h1.getCluster().getLocalMember();
        h1.getLifecycleService().shutdown();
        assertEquals(size, map3.size());
    }

    /**
     * Testing if we are losing any data when we start a node
     * or when we shutdown a node.
     * <p/>
     * Before the shutdowns we are waiting 2 seconds so that we will give
     * remaining members some time to backup their data.
     */
    @Test(timeout = 60000)
    public void testDataRecovery() throws Exception {
        final int size = 1000;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        IMap map1 = h1.getMap("default");
        for (int i = 0; i < size; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(size, map1.size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        IMap map2 = h2.getMap("default");
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        IMap map3 = h3.getMap("default");
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(new Config());
        IMap map4 = h4.getMap("default");
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        assertEquals(size, map4.size());
        Thread.sleep(4000);
        long ownedSize = getTotalOwnedEntryCount(map1, map2, map3, map4);
        long backupSize = getTotalBackupEntryCount(map1, map2, map3, map4);
        assertEquals(size, ownedSize);
        assertEquals(size, backupSize);
        h4.getLifecycleService().shutdown();
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        sleep(4000);
        ownedSize = getTotalOwnedEntryCount(map1, map2, map3);
        backupSize = getTotalBackupEntryCount(map1, map2, map3);
        assertEquals(size, ownedSize);
        assertEquals(size, backupSize);
        h1.getLifecycleService().shutdown();
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        sleep(4000);
        ownedSize = getTotalOwnedEntryCount(map2, map3);
        backupSize = getTotalBackupEntryCount(map2, map3);
        assertEquals(size, ownedSize);
        assertEquals(size, backupSize);
        h2.getLifecycleService().shutdown();
        assertEquals(size, map3.size());
    }

    @Test(timeout = 160000)
    public void testBackupCountTwo() throws Exception {
        Config config = new Config();
        config.getProperties().put(GroupProperties.PROP_PARTITION_MIGRATION_INTERVAL, "0");
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setBackupCount(2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        int size = 10000;
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }
        assertEquals(size, getTotalOwnedEntryCount(map1, map2));
        assertEquals(size, getTotalBackupEntryCount(map1, map2));
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        IMap map3 = h3.getMap("default");
        Thread.sleep(3000);
        assertEquals(size, getTotalOwnedEntryCount(map1, map2, map3));
        assertEquals(2 * size, getTotalBackupEntryCount(map1, map2, map3));
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(config);
        IMap map4 = h4.getMap("default");
        Thread.sleep(3000);
        assertEquals(size, getTotalOwnedEntryCount(map1, map2, map3, map4));
        assertEquals(2 * size, getTotalBackupEntryCount(map1, map2, map3, map4));
    }

    private long getTotalOwnedEntryCount(IMap... maps) {
        long total = 0;
        for (IMap iMap : maps) {
            total += iMap.getLocalMapStats().getOwnedEntryCount();
        }
        return total;
    }

    private long getTotalBackupEntryCount(IMap... maps) {
        long total = 0;
        for (IMap iMap : maps) {
            total += iMap.getLocalMapStats().getBackupEntryCount();
        }
        return total;
    }

    private long getOwnedAndBackupCount(IMap imap) {
        LocalMapStats localMapStats = imap.getLocalMapStats();
        return localMapStats.getOwnedEntryCount() + localMapStats.getBackupEntryCount();
    }


    /**
     * Testing correctness of the sizes during migration.
     * <p/>
     * Migration happens block by block and after completion of
     * each block, next block will start migrating after a fixed
     * time interval. While a block migrating, for the multicall
     * operations, such as size() and queries, there might be a case
     * where data is migrated but not counted/queried. To avoid this
     * hazelcast will compare the block-owners hash. If req.blockId
     * (which is holding requester's block-owners hash value) is not same
     * as the target's block-owners hash value, then request will
     * be re-done.
     */
    @Test(timeout = 3600000)
    public void testDataRecoveryAndCorrectness() throws Exception {
        final int size = 10000;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, h1.getCluster().getMembers().size());
        IMap map1 = h1.getMap("default");
        for (int i = 0; i < size; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(size, map1.size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        IMap map2 = h2.getMap("default");
        for (int i = 0; i < 100; i++) {
            assertEquals(size, map1.size());
            assertEquals(size, map2.size());
        }
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        IMap map3 = h3.getMap("default");
        for (int i = 0; i < 100; i++) {
            assertEquals(size, map1.size());
            assertEquals(size, map2.size());
            assertEquals(size, map3.size());
        }
        assertEquals(3, h3.getCluster().getMembers().size());
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(new Config());
        IMap map4 = h4.getMap("default");
        for (int i = 0; i < 100; i++) {
            assertEquals(size, map1.size());
            assertEquals(size, map2.size());
            assertEquals(size, map3.size());
            assertEquals(size, map4.size());
        }
        h4.getLifecycleService().shutdown();
        for (int i = 0; i < 100; i++) {
            assertEquals(size, map1.size());
            assertEquals(size, map2.size());
            assertEquals(size, map3.size());
        }
        h1.getLifecycleService().shutdown();
        for (int i = 0; i < 100; i++) {
            assertEquals(size, map2.size());
            assertEquals(size, map3.size());
        }
        h2.getLifecycleService().shutdown();
        for (int i = 0; i < 100; i++) {
            assertEquals(size, map3.size());
        }
    }


    @Test
    public void testIssue177BackupCount() throws InterruptedException {
        final Config config = new Config();
        config.setProperty("hazelcast.partition.migration.interval", "0");
        config.setProperty("hazelcast.wait.seconds.before.join", "1");
        final String name = "test";
        config.getMapConfig(name).setBackupCounts(1, 0);

        final Random rand = new Random(System.currentTimeMillis());
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(10);
        final int count = 10000;
        final int totalCount = count * (instances.length() - 1);
        final Thread[] threads = new Thread[instances.length()];

        for (int i = 0; i < instances.length(); i++) {
            final int finalI = i;
            Thread thread = new Thread() {
                public void run() {
                    try {
                        Thread.sleep(3000 * finalI);
                        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
                        instances.set(finalI, instance);
                        Thread.sleep(rand.nextInt(100));
                        if (finalI != 0) { // do not run on master node,
                            // let partition assignment be made during put ops.
                            for (int j = 0; j < 10000; j++) {
                                instance.getMap(name).put(getName() + "-" + j, "value");
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
            threads[i] = thread;
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        final int trials = 10;
        for (int i = 0; i < trials; i++) {
            long totalOwned = 0L;
            long totalBackup = 0L;
            for (int j = 0; j < instances.length(); j++) {
                HazelcastInstance hz = instances.get(j);
                LocalMapStats stats = hz.getMap(name).getLocalMapStats();
                totalOwned += stats.getOwnedEntryCount();
                totalBackup += stats.getBackupEntryCount();
            }
            System.out.println("totalOwned = " + totalOwned);
            System.out.println("totalBackup = " + totalBackup);
            assertEquals("Owned entry count is wrong! ", totalCount, totalOwned);
            if (i < trials - 1) {
                if (totalBackup == totalCount) {
                    break;
                }
                // check again after sometime
                Thread.sleep(1000);
            } else {
                assertEquals("Backup entry count is wrong! ", totalCount, totalBackup);
            }
        }
    }

    @Test
    /**
     * Test for issue #259.
     */
    public void testBackupPutWhenOwnerNodeDead() throws InterruptedException {
        final Config config = new Config();
        config.setProperty("hazelcast.wait.seconds.before.join", "1");
        final String name = "test";

        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        final IMap<Object, Object> map = hz2.getMap(name);

        final int size = 100000;
        final byte[] data = new byte[250];
        final int threads = 100;
        final int l = size / threads;
        final CountDownLatch latch = new CountDownLatch(threads);
        ExecutorService ex = Executors.newFixedThreadPool(threads);

        new Thread() {
            public void run() {
                while (hz.getMap(name).size() < size / 2) {
                    try {
                        sleep(5);
                    } catch (InterruptedException ignored) {
                    }
                }
                TestUtil.getNode(hz).getConnectionManager().shutdown();
            }
        }.start();

        for (int i = 0; i < threads; i++) {
            final int n = i;
            ex.execute(new Runnable() {
                public void run() {
                    for (int j = (n * l); j < (n + 1) * l; j++) {
                        map.put(j, data);
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ignored) {
                        }
                    }
                    latch.countDown();
                }
            });
        }

        latch.await();
        ex.shutdown();
        assertEquals("Data lost!", size, map.size());
    }

    @Test
    /**
     * Test for issue #259.
     */
    public void testBackupRemoveWhenOwnerNodeDead() throws InterruptedException {
        final Config config = new Config();
        config.setProperty("hazelcast.wait.seconds.before.join", "1");
        final String name = "test";

        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        final IMap<Object, Object> map = hz2.getMap(name);

        final int size = 100000;
        final int threads = 100;
        ExecutorService ex = Executors.newFixedThreadPool(threads);
        final int loadCount = 10;
        final CountDownLatch loadLatch = new CountDownLatch(loadCount);

        // initial load
        for (int i = 0; i < loadCount; i++) {
            final int n = i;
            ex.execute(new Runnable() {
                public void run() {
                    int chunk = size / loadCount;
                    for (int j = (n * chunk); j < (n + 1) * chunk; j++) {
                        map.put(j, j);
                    }
                    loadLatch.countDown();
                }
            });
        }
        loadLatch.await();

        new Thread() {
            public void run() {
                while (hz.getMap(name).size() > size / 2) {
                    try {
                        sleep(5);
                    } catch (InterruptedException ignored) {
                    }
                }
                TestUtil.getNode(hz).getConnectionManager().shutdown();
            }
        }.start();

        final int chunk = size / threads;
        final CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            final int n = i;
            ex.execute(new Runnable() {
                public void run() {
                    for (int j = (n * chunk); j < (n + 1) * chunk; j++) {
                        map.remove(j);
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ignored) {
                        }
                    }
                    latch.countDown();
                }
            });
        }

        latch.await();
        ex.shutdown();
        assertEquals("Remove failed!", 0, map.size());
    }
}
