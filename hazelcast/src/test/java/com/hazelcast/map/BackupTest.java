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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class BackupTest extends HazelcastTestSupport {

    @Test
    public void testGracefulShutdown() throws Exception {
        int size = 250000;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(5);
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1111");

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap m1 = h1.getMap("default");
        for (int i = 0; i < size; i++) {
            m1.put(i, i);
        }

        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        IMap m2 = h2.getMap("default");
        h1.getLifecycleService().shutdown();
        assertEquals(size, m2.size());

        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap m3 = h3.getMap("default");
        h2.getLifecycleService().shutdown();
        assertEquals(size, m3.size());

        HazelcastInstance h4 = nodeFactory.newHazelcastInstance(config);
        IMap m4 = h4.getMap("default");
        h3.getLifecycleService().shutdown();
        assertEquals(size, m4.size());
    }

    @Test
    // TODO: @mm - Test fails randomly!
    public void testGracefulShutdown2() throws Exception {
        Config config = new Config();
        config.getMapConfig("test").setBackupCount(2).setStatisticsEnabled(true);
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1111");

        TestHazelcastInstanceFactory f = createHazelcastInstanceFactory(6);
        final HazelcastInstance hz = f.newHazelcastInstance(config);

        final IMap<Object, Object> map = hz.getMap("test");
        final int size = 200000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        final HazelcastInstance hz2 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map2 = hz2.getMap("test");

        Assert.assertEquals(size, map2.size());

        final HazelcastInstance hz3 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map3 = hz3.getMap("test");

        final HazelcastInstance hz4 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map4 = hz4.getMap("test");

        Assert.assertEquals(size, map3.size());
        Assert.assertEquals(size, map4.size());

        final HazelcastInstance hz5 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map5 = hz5.getMap("test");

        final HazelcastInstance hz6 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map6 = hz6.getMap("test");

        Assert.assertEquals(size, map5.size());
        Assert.assertEquals(size, map6.size());

        hz.getLifecycleService().shutdown();
        hz2.getLifecycleService().shutdown();

        Assert.assertEquals(size, map3.size());
        Assert.assertEquals(size, map4.size());
        Assert.assertEquals(size, map5.size());
        Assert.assertEquals(size, map6.size());

        hz3.getLifecycleService().shutdown();
        hz4.getLifecycleService().shutdown();

        Assert.assertEquals(size, map5.size());
        Assert.assertEquals(size, map6.size());
    }

    @Test
    // TODO: @mm - Test fails randomly!
    public void issue395BackupProblemWithBCount2() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final int size = 1000;
        Config config = new Config();
        final String name = "default";
        config.getMapConfig(name).setBackupCount(2).setStatisticsEnabled(true);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        IMap map1 = h1.getMap(name);
        IMap map2 = h2.getMap(name);
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }
        assertEquals(size, getTotalOwnedEntryCount(map1, map2));
        assertEquals(size, getTotalBackupEntryCount(map1, map2));
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap map3 = h3.getMap(name);

        Thread.sleep(1000 * 3);
        assertEquals(size, getTotalOwnedEntryCount(map1, map2, map3));
        assertEquals(2 * size, getTotalBackupEntryCount(map1, map2, map3));
    }

    @Test(timeout = 60000)
    // TODO: @mm - Test fails randomly!
    public void testDataRecovery() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);

        final int size = 1000;
        final String name = "default";
        final Config config = new Config();
        config.getMapConfig(name).setStatisticsEnabled(true);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map1 = h1.getMap(name);
        for (int i = 0; i < size; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(size, map1.size());
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        IMap map2 = h2.getMap(name);
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap map3 = h3.getMap(name);
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        HazelcastInstance h4 = nodeFactory.newHazelcastInstance(config);
        IMap map4 = h4.getMap(name);
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        assertEquals(size, map4.size());
        sleep(3000);
        long ownedSize = getTotalOwnedEntryCount(map1, map2, map3, map4);
        long backupSize = getTotalBackupEntryCount(map1, map2, map3, map4);
        assertEquals(size, ownedSize);
        assertEquals(size, backupSize);
        h4.getLifecycleService().shutdown();
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        sleep(3000);
        ownedSize = getTotalOwnedEntryCount(map1, map2, map3);
        backupSize = getTotalBackupEntryCount(map1, map2, map3);
        assertEquals(size, ownedSize);
        assertEquals(size, backupSize);
        h1.getLifecycleService().shutdown();
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        sleep(3000);
        ownedSize = getTotalOwnedEntryCount(map2, map3);
        backupSize = getTotalBackupEntryCount(map2, map3);
        assertEquals(size, ownedSize);
        assertEquals(size, backupSize);
        h2.getLifecycleService().shutdown();
        assertEquals(size, map3.size());
    }

    /**
     * Fix for the issue 275.
     *
     * @throws Exception
     */
    @Test
    // TODO: @mm - Test fails randomly!
    public void testDataRecovery2() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final int size = 100000;
        final Config config = new Config();
        final String name = "default";
        config.getMapConfig(name).setStatisticsEnabled(true);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map1 = h1.getMap(name);
        for (int i = 0; i < size; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(size, map1.size());

        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        sleep(3000);
        IMap map2 = h2.getMap(name);
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, getTotalOwnedEntryCount(map1, map2));
        assertEquals(size, getTotalBackupEntryCount(map1, map2));

        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap map3 = h3.getMap(name);
        sleep(3000);
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        assertEquals(size, getTotalOwnedEntryCount(map1, map2, map3));
        assertEquals(size, getTotalBackupEntryCount(map1, map2, map3));

        h2.getLifecycleService().shutdown();
        sleep(3000);
        assertEquals(size, map1.size());
        assertEquals(size, map3.size());
        assertEquals(size, getTotalOwnedEntryCount(map1, map3));
        assertEquals(size, getTotalBackupEntryCount(map1, map3));
        h1.getLifecycleService().shutdown();
        assertEquals(size, map3.size());
    }

    @Test(timeout = 160000)
    public void testBackupCountTwo() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);

        Config config = new Config();
        final String name = "default";
        final int partitionCount = 1111;
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));
        MapConfig mapConfig = config.getMapConfig(name);
        final int backupCount = 2;
        mapConfig.setBackupCount(backupCount).setStatisticsEnabled(true);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        IMap map1 = h1.getMap(name);
        IMap map2 = h2.getMap(name);

        int size = 100000;
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }
        assertEquals(size, getTotalOwnedEntryCount(map1, map2));
        assertEquals(size, getTotalBackupEntryCount(map1, map2));

        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap map3 = h3.getMap(name);

        Thread.sleep(3000);
        assertEquals(size, getTotalOwnedEntryCount(map1, map2, map3));
        assertEquals(backupCount * size, getTotalBackupEntryCount(map1, map2, map3));

        HazelcastInstance h4 = nodeFactory.newHazelcastInstance(config);
        IMap map4 = h4.getMap(name);
        Thread.sleep(3000);
        assertEquals(size, getTotalOwnedEntryCount(map1, map2, map3, map4));
        assertEquals(backupCount * size, getTotalBackupEntryCount(map1, map2, map3, map4));
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
    // TODO: @mm - Test fails randomly!
    public void testDataRecoveryAndCorrectness() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);

        final int size = 10000;
        final Config config = new Config();
        final String name = "default";
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        assertEquals(1, h1.getCluster().getMembers().size());
        IMap map1 = h1.getMap(name);
        for (int i = 0; i < size; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(size, map1.size());
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        IMap map2 = h2.getMap(name);
        for (int i = 0; i < 100; i++) {
            assertEquals(size, map1.size());
            assertEquals(size, map2.size());
        }
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap map3 = h3.getMap(name);
        for (int i = 0; i < 100; i++) {
            assertEquals(size, map1.size());
            assertEquals(size, map2.size());
            assertEquals(size, map3.size());
        }
        assertEquals(3, h3.getCluster().getMembers().size());
        HazelcastInstance h4 = nodeFactory.newHazelcastInstance(config);
        IMap map4 = h4.getMap(name);
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
    // TODO: @mm - Test fails randomly!
    public void testIssue177BackupCount() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(10);

        final Config config = new Config();
        config.setProperty("hazelcast.partition.migration.interval", "0");
        config.setProperty("hazelcast.wait.seconds.before.join", "1");
        final String name = "test";
        config.getMapConfig(name).setBackupCounts(1, 0).setStatisticsEnabled(true);

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
                        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
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
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        final Config config = new Config();
        config.setProperty("hazelcast.wait.seconds.before.join", "1");
        final String name = "test";

        final HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance hz2 = nodeFactory.newHazelcastInstance(config);
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
                        return;
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
                            return;
                        }
                    }
                    latch.countDown();
                }
            });
        }

        try {
            assertTrue(latch.await(2, TimeUnit.MINUTES));
            assertEquals("Data lost!", size, map.size());
        } finally {
            ex.shutdownNow();
        }
    }

    @Test
    /**
     * Test for issue #259.
     */
    public void testBackupRemoveWhenOwnerNodeDead() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        final Config config = new Config();
        config.setProperty("hazelcast.wait.seconds.before.join", "1");
        final String name = "test";

        final HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance hz2 = nodeFactory.newHazelcastInstance(config);
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
                        return;
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

        try {
            assertTrue(latch.await(2, TimeUnit.MINUTES));
            assertEquals("Remove failed!", 0, map.size());
        } finally {
            ex.shutdown();
        }
    }
}
