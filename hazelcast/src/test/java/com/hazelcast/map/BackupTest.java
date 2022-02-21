/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BackupTest extends HazelcastTestSupport {

    private final String mapName = randomMapName();

    @Test
    public void testNodeStartAndGracefulShutdown_inSequence() throws Exception {
        int size = 10000;
        int nodeCount = 4;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(0);

        HazelcastInstance master = factory.newHazelcastInstance(config);
        IMap<Integer, Integer> map = master.getMap(mapName);
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < nodeCount; i++) {
            HazelcastInstance slave = factory.newHazelcastInstance(config);
            map = slave.getMap(mapName);
            master.shutdown();
            checkSize(size, map);
            master = slave;
        }
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        final int nodeCount = 6;
        final int size = 10000;
        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(0);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Collection<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            HazelcastInstance hz = factory.newHazelcastInstance(config);
            instances.add(hz);
            IMap<Integer, Integer> map = hz.getMap(mapName);
            if (i == 0) {
                for (int k = 0; k < size; k++) {
                    map.put(k, k);
                }
            }
            checkSize(size, map);
        }

        Iterator<HazelcastInstance> iterator = instances.iterator();
        while (iterator.hasNext()) {
            HazelcastInstance hz = iterator.next();
            iterator.remove();
            hz.shutdown();

            for (HazelcastInstance instance : instances) {
                IMap<Integer, Integer> map = instance.getMap(mapName);
                checkSize(size, map);
            }
        }
    }

    private static void checkSize(final int expectedSize, final IMap map) {
        assertEquals(expectedSize, map.size());
    }

    @Test
    public void testBackupMigrationAndRecovery_singleBackup() throws Exception {
        testBackupMigrationAndRecovery(4, 1, 5000);
    }

    @Test
    public void testBackupMigrationAndRecovery_twoBackups() throws Exception {
        testBackupMigrationAndRecovery(6, 2, 5000);
    }

    private void testBackupMigrationAndRecovery(int nodeCount, int backupCount, int mapSize) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(nodeCount);
        final String name = mapName;

        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), "1");
        config.getMapConfig(name).setBackupCount(backupCount).setStatisticsEnabled(true);

        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
            instances.add(hz);
            if (i == 0) {
                IMap<Integer, String> map = hz.getMap(name);
                for (int k = 0; k < mapSize; k++) {
                    map.put(k, "value" + k);
                }
            }
            checkMapSizes(mapSize, backupCount, instances);
        }

        Random rand = new Random();
        while (!instances.isEmpty()) {
            int ix = rand.nextInt(instances.size());
            HazelcastInstance removedInstance = instances.remove(ix);
            TestUtil.terminateInstance(removedInstance);
            checkMapSizes(mapSize, backupCount, instances);
        }
    }

    private void checkMapSizes(final int expectedSize, final int backupCount, List<HazelcastInstance> instances)
            throws InterruptedException {

        final int nodeCount = instances.size();
        if (nodeCount == 0) {
            return;
        }

        final IMap[] maps = new IMap[instances.size()];
        int i = 0;
        for (HazelcastInstance hz : instances) {
            IMap map = hz.getMap(mapName);
            assertEquals(expectedSize, map.size());
            maps[i++] = map;
        }

        final int expectedBackupSize = Math.min(nodeCount - 1, backupCount) * expectedSize;

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long ownedSize = getTotalOwnedEntryCount(maps);
                assertEquals("Missing owned entries, node-count: " + nodeCount, expectedSize, ownedSize);

                long backupSize = getTotalBackupEntryCount(maps);
                assertEquals("Missing owned entries, node-count: " + nodeCount + ", backup-count: " + backupCount,
                        expectedBackupSize, backupSize);
            }
        });
    }

    private static long getTotalOwnedEntryCount(IMap... maps) {
        long total = 0;
        for (IMap map : maps) {
            total += map.getLocalMapStats().getOwnedEntryCount();
        }
        return total;
    }

    private static long getTotalBackupEntryCount(IMap... maps) {
        long total = 0;
        for (IMap map : maps) {
            total += map.getLocalMapStats().getBackupEntryCount();
        }
        return total;
    }

    @Test
    public void testIssue177BackupCount() throws InterruptedException {
        final int nodeCount = 6;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory();
        final Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), "1");
        config.getMapConfig(mapName).setBackupCount(1).setStatisticsEnabled(true);

        final Random rand = new Random();
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(nodeCount);
        final int count = 10000;
        final int totalCount = count * (nodeCount - 1);
        final CountDownLatch latch = new CountDownLatch(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            final int index = i;
            new Thread() {
                public void run() {
                    sleepMillis(index * rand.nextInt(1000));
                    HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
                    instances.set(index, instance);
                    if (index != 0) {
                        // do not run on master node,
                        // let partition assignment be made during put ops.
                        IMap<Object, Object> map = instance.getMap(mapName);
                        for (int j = 0; j < count; j++) {
                            map.put(getName() + "-" + j, "value");
                        }
                    }
                    latch.countDown();
                }
            }.start();
        }

        assertTrue(latch.await(5, TimeUnit.MINUTES));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long totalOwned = 0L;
                long totalBackup = 0L;
                for (int i = 0; i < instances.length(); i++) {
                    HazelcastInstance hz = instances.get(i);
                    LocalMapStats stats = hz.getMap(mapName).getLocalMapStats();
                    totalOwned += stats.getOwnedEntryCount();
                    totalBackup += stats.getBackupEntryCount();
                }
                assertEquals("Owned entry count is wrong! ", totalCount, totalOwned);
                assertEquals("Backup entry count is wrong! ", totalCount, totalBackup);
            }
        });
    }

    /**
     * Test for issue #259.
     */
    @Test
    public void testBackupPutWhenOwnerNodeDead() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config config = getConfig();

        final HazelcastInstance hz1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance hz2 = nodeFactory.newHazelcastInstance(config);
        final IMap<Object, Object> map = hz2.getMap(mapName);

        final int threads = 16;
        final int perThreadSize = 1000;
        final int size = threads * perThreadSize;

        new Thread() {
            public void run() {
                IMap<Object, Object> m = hz1.getMap(mapName);
                while (m.size() < size / 2) {
                    sleepMillis(5);
                }
                TestUtil.terminateInstance(hz1);
            }
        }.start();

        final CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            final int index = i;
            new Thread() {
                public void run() {
                    for (int k = (index * perThreadSize); k < (index + 1) * perThreadSize; k++) {
                        map.put(k, k);
                        sleepMillis(1);
                    }
                    latch.countDown();
                }
            }.start();
        }

        assertTrue(latch.await(5, TimeUnit.MINUTES));
        assertEquals("Data lost!", size, map.size());
    }

    /**
     * Test for issue #259.
     */
    @Test
    public void testBackupRemoveWhenOwnerNodeDead() throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config config = getConfig();

        final HazelcastInstance hz1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance hz2 = nodeFactory.newHazelcastInstance(config);
        final IMap<Object, Object> map = hz2.getMap(mapName);

        final int threads = 16;
        final int perThreadSize = 1000;
        final int size = threads * perThreadSize;

        // initial load
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        new Thread() {
            public void run() {
                IMap<Object, Object> m = hz1.getMap(mapName);
                while (m.size() > size / 2) {
                    sleepMillis(5);
                }
                TestUtil.terminateInstance(hz1);
            }
        }.start();

        final CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            final int index = i;
            new Thread() {
                public void run() {
                    for (int k = (index * perThreadSize); k < (index + 1) * perThreadSize; k++) {
                        map.remove(k);
                        sleepMillis(1);
                    }
                    latch.countDown();
                }
            }.start();
        }

        assertTrue(latch.await(5, TimeUnit.MINUTES));
        assertEquals("Remove failed!", 0, map.size());
    }

    /**
     * Tests data safety when multiple nodes start and a non-master node is shutdown
     * immediately after start and doing a partition based operation.
     */
    @Test
    public void testGracefulShutdown_Issue2804() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1111");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);

        String key = "key";
        String value = "value";

        IMap<String, String> map = h1.getMap(mapName);
        map.put(key, value);

        h2.shutdown();
        assertEquals(value, map.get(key));
    }
}
