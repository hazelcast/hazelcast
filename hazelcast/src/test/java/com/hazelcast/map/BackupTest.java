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
import com.hazelcast.core.*;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class BackupTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "default";

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Before
    public void gc() {
        Runtime.getRuntime().gc();
    }

    private void waitForTermination(HazelcastInstance instance) {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (true) {
            final Node node = TestUtil.getNode(instance);
            if (node == null || !node.isActive()) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void shutdownAndWait(HazelcastInstance instance) {
        instance.shutdown();
        waitForTermination(instance);
    }

    private void waitForMigration(HazelcastInstance instance) {
        final Node node = TestUtil.getNode(instance);
        final InternalPartitionService ps = node.getPartitionService();
        while (ps.hasOnGoingMigration()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void waitForMigrationAndCheckSize(int expectedSize,
                                              HazelcastInstance instance,
                                              IMap map) {
        waitForMigration(instance);
        assertEquals(expectedSize, map.size());
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        int size = 50000;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final Config config = new Config();

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap<Integer, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < size; i++) {
            m1.put(i, i);
        }

        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        IMap m2 = h2.getMap(MAP_NAME);
        shutdownAndWait(h1);
        waitForMigrationAndCheckSize(size, h2, m2);

        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap m3 = h3.getMap(MAP_NAME);
        shutdownAndWait(h2);
        waitForMigrationAndCheckSize(size, h3, m3);

        HazelcastInstance h4 = nodeFactory.newHazelcastInstance(config);
        IMap m4 = h4.getMap(MAP_NAME);
        shutdownAndWait(h3);
        waitForMigrationAndCheckSize(size, h4, m4);
    }

    @Test
    public void testGracefulShutdown2() throws Exception {
        Config config = new Config();

        config.getMapConfig(MAP_NAME).setBackupCount(2);

        TestHazelcastInstanceFactory f = createHazelcastInstanceFactory(6);
        final HazelcastInstance hz = f.newHazelcastInstance(config);

        final IMap<Integer, Integer> map = hz.getMap(MAP_NAME);
        final int size = 50000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        final HazelcastInstance hz2 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map2 = hz2.getMap(MAP_NAME);

        waitForMigrationAndCheckSize(size, hz2, map2);

        final HazelcastInstance hz3 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map3 = hz3.getMap(MAP_NAME);

        final HazelcastInstance hz4 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map4 = hz4.getMap(MAP_NAME);

        waitForMigrationAndCheckSize(size, hz3, map3);
        waitForMigrationAndCheckSize(size, hz4, map4);

        final HazelcastInstance hz5 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map5 = hz5.getMap(MAP_NAME);

        final HazelcastInstance hz6 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map6 = hz6.getMap(MAP_NAME);

        waitForMigrationAndCheckSize(size, hz5, map5);
        waitForMigrationAndCheckSize(size, hz6, map6);

        shutdownAndWait(hz);
        shutdownAndWait(hz2);

        waitForMigrationAndCheckSize(size, hz3, map3);
        waitForMigrationAndCheckSize(size, hz4, map4);
        waitForMigrationAndCheckSize(size, hz5, map5);
        waitForMigrationAndCheckSize(size, hz6, map6);

        shutdownAndWait(hz3);
        shutdownAndWait(hz4);

        waitForMigrationAndCheckSize(size, hz5, map5);
        waitForMigrationAndCheckSize(size, hz6, map6);
    }

    @Test
    public void testGracefulShutdown3() throws Exception {
        Config config = new Config();
        config.getMapConfig(MAP_NAME).setBackupCount(1);

        TestHazelcastInstanceFactory f = createHazelcastInstanceFactory(6);
        final HazelcastInstance hz = f.newHazelcastInstance(config);

        final IMap<Integer, Integer> map = hz.getMap(MAP_NAME);
        final int size = 50000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        final HazelcastInstance hz2 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map2 = hz2.getMap(MAP_NAME);

        final HazelcastInstance hz3 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map3 = hz3.getMap(MAP_NAME);

        final HazelcastInstance hz4 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map4 = hz4.getMap(MAP_NAME);

        final HazelcastInstance hz5 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map5 = hz5.getMap(MAP_NAME);

        final HazelcastInstance hz6 = f.newHazelcastInstance(config);
        final IMap<Object, Object> map6 = hz6.getMap(MAP_NAME);

        waitForMigrationAndCheckSize(size, hz2, map2);
        waitForMigrationAndCheckSize(size, hz3, map3);
        waitForMigrationAndCheckSize(size, hz4, map4);
        waitForMigrationAndCheckSize(size, hz5, map5);
        waitForMigrationAndCheckSize(size, hz6, map6);

        shutdownAndWait(hz6);
        waitForMigrationAndCheckSize(size, hz, map);
        waitForMigrationAndCheckSize(size, hz2, map2);
        waitForMigrationAndCheckSize(size, hz3, map3);
        waitForMigrationAndCheckSize(size, hz4, map4);
        waitForMigrationAndCheckSize(size, hz5, map5);

        shutdownAndWait(hz2);
        waitForMigrationAndCheckSize(size, hz, map);
        waitForMigrationAndCheckSize(size, hz3, map3);
        waitForMigrationAndCheckSize(size, hz4, map4);
        waitForMigrationAndCheckSize(size, hz5, map5);

        shutdownAndWait(hz5);
        waitForMigrationAndCheckSize(size, hz, map);
        waitForMigrationAndCheckSize(size, hz3, map3);
        waitForMigrationAndCheckSize(size, hz4, map4);

        shutdownAndWait(hz3);
        waitForMigrationAndCheckSize(size, hz, map);
        waitForMigrationAndCheckSize(size, hz4, map4);

        shutdownAndWait(hz4);
        waitForMigrationAndCheckSize(size, hz, map);
    }

    /**
     * Test for the issue https://code.google.com/p/hazelcast/issues/detail?id=275.
     */
    @Test
    public void testBackupMigrationAndRecovery() throws Exception {
        testBackupMigrationAndRecovery(4, 1, 5000);
    }

    /**
     * Test for the issue https://code.google.com/p/hazelcast/issues/detail?id=395.
     */
    @Test
    public void testBackupMigrationAndRecovery2() throws Exception {
        testBackupMigrationAndRecovery(6, 2, 5000);
    }

    private void testBackupMigrationAndRecovery(int nodeCount, int backupCount, int mapSize) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(nodeCount);
        final String name = MAP_NAME;
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_BACKUP_SYNC_INTERVAL, "1");
        config.getMapConfig(name).setBackupCount(backupCount).setStatisticsEnabled(true);

        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];

        HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
        instances[0] = hz;
        IMap<Integer, String> map1 = hz.getMap(name);
        for (int i = 0; i < mapSize; i++) {
            map1.put(i, "value" + i);
        }
        checkMapSizes(mapSize, backupCount, instances);

        for (int i = 1; i < nodeCount; i++) {
            instances[i] = nodeFactory.newHazelcastInstance(config);
            checkMapSizes(mapSize, backupCount, instances);
        }

        final Random rand = new Random();
        for (int i = 1; i < nodeCount; i++) {
            int ix;
            do {
                ix = rand.nextInt(nodeCount);
            } while (instances[ix] == null);

            TestUtil.terminateInstance(instances[ix]);
            instances[ix] = null;
            checkMapSizes(mapSize, backupCount, instances);

        }

    }

    private static void checkMapSizes(final int expectedSize, int backupCount, HazelcastInstance... instances)
            throws InterruptedException {
        int nodeCount = 0;
        final IMap[] maps = new IMap[instances.length];
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < instances.length; j++) {
                final HazelcastInstance hz = instances[j];
                if (hz != null) {
                    if (i == 0) {
                        maps[j] = hz.getMap(MAP_NAME);
                        nodeCount++;
                    }
                    assertEquals(expectedSize, maps[j].size());
                }
            }
            Thread.sleep(10);
        }
        final int expectedBackupSize = Math.min(nodeCount - 1, backupCount) * expectedSize;

        for (int i = 0; i < 1200; i++) {
            long ownedSize = getTotalOwnedEntryCount(maps);
            long backupSize = getTotalBackupEntryCount(maps);
            if (ownedSize == expectedSize && backupSize == expectedBackupSize) {
                int votes = 0;
                for (HazelcastInstance hz : instances) {
                    if (hz != null) {
                        votes += TestUtil.getNode(hz).getPartitionService().hasOnGoingMigration() ? 0 : 1;
                    }
                }
                if (votes == nodeCount) {
                    break;
                }
            }
            Thread.sleep(500);
        }
        long actualBackupSize = getTotalBackupEntryCount(maps);
        if (expectedBackupSize > actualBackupSize) {
            fail("Missing backups, node-count: " + nodeCount + ", expected:<" + expectedBackupSize
                    + "> but was:<" + actualBackupSize + ">");
        }
    }

    private static long getTotalOwnedEntryCount(IMap... maps) {
        long total = 0;
        for (IMap map : maps) {
            if (map != null) {
                total += map.getLocalMapStats().getOwnedEntryCount();
            }
        }
        return total;
    }

    private static long getTotalBackupEntryCount(IMap... maps) {
        long total = 0;
        for (IMap map : maps) {
            if (map != null) {
                total += map.getLocalMapStats().getBackupEntryCount();
            }
        }
        return total;
    }

    @Test
    public void testIssue177BackupCount() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(10);
        final Config config = new Config();
        final String name = MAP_NAME;
        config.getMapConfig(name).setBackupCount(1).setStatisticsEnabled(true);

        final Random rand = new Random();
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(10);
        final int count = 10000;
        final int totalCount = count * (instances.length() - 1);
        final CountDownLatch latch = new CountDownLatch(instances.length());

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
                    } finally {
                        latch.countDown();
                    }
                }
            };
            thread.start();
        }

        assertTrue(latch.await(5, TimeUnit.MINUTES));

        final int trials = 50;
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

    /**
     * Test for issue #259.
     */
    @Test
    public void testBackupPutWhenOwnerNodeDead() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        final String name = MAP_NAME;

        final HazelcastInstance hz = nodeFactory.newHazelcastInstance();
        final HazelcastInstance hz2 = nodeFactory.newHazelcastInstance();
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
                TestUtil.terminateInstance(hz);
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
            assertTrue(latch.await(5, TimeUnit.MINUTES));
            assertEquals("Data lost!", size, map.size());
        } finally {
            ex.shutdownNow();
        }
    }

    /**
     * Test for issue #259.
     */
    @Test
    public void testBackupRemoveWhenOwnerNodeDead() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        final String name = MAP_NAME;

        final HazelcastInstance hz = nodeFactory.newHazelcastInstance();
        final HazelcastInstance hz2 = nodeFactory.newHazelcastInstance();
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
                TestUtil.terminateInstance(hz);
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
            assertTrue(latch.await(5, TimeUnit.MINUTES));
            assertEquals("Remove failed!", 0, map.size());
        } finally {
            ex.shutdown();
        }
    }

    /**
     * Tests data safety when multiple nodes start and a non-master node is shutdown
     * immediately after start and doing a partition based operation.
     */
    @Test
    public void testGracefulShutdown_Issue2804() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1111");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);

        Object key = "key";
        Object value = "value";

        IMap<Object, Object> map = h1.getMap(MAP_NAME);
        map.put(key, value);

        h2.shutdown();
        assertEquals(value, map.get(key));
    }
}
