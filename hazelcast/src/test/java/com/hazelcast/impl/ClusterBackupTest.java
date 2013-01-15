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
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.*;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.impl.TestUtil.getConcurrentMapManager;
import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

/**
 * Run these tests with
 * -Xms512m -Xmx512m
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ClusterBackupTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

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
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
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

    /**
     * AtomicNumber.incrementAndGet backup issue
     *
     * @throws InterruptedException
     */
    @Test
    public void testIssue505() throws InterruptedException {
        HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance liteMember = Hazelcast.newHazelcastInstance(new Config().setLiteMember(true));
        AtomicNumber test = liteMember.getAtomicNumber("test");
        assertEquals(1, test.incrementAndGet());
        HazelcastInstance hazelcastInstance3 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(2, test.incrementAndGet());
        hazelcastInstance1.getLifecycleService().shutdown();
        assertEquals(3, test.incrementAndGet());
    }

    @Test
    public void issue390NoBackupWhenSuperClient() throws InterruptedException {
        final int size = 200;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        IMap map1 = h1.getMap("def");
        for (int i = 0; i < size; i++) {
            map1.put(i, new byte[1000]);
        }
        Config scconfig = new Config();
        scconfig.setLiteMember(true);
        HazelcastInstance sc = Hazelcast.newHazelcastInstance(scconfig);
        Config config = new Config();
        final CountDownLatch latch = new CountDownLatch(2);
        config.addListenerConfig(new ListenerConfig(new MigrationListener() {
            public void migrationStarted(MigrationEvent migrationEvent) {
            }

            public void migrationCompleted(MigrationEvent migrationEvent) {
                latch.countDown();
            }

            public void migrationFailed(final MigrationEvent migrationEvent) {

            }
        }));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap map2 = h2.getMap("def");
        assertTrue(latch.await(60, TimeUnit.SECONDS));
        assertEquals(size, getTotalOwnedEntryCount(map1, map2));
        assertEquals(size, getTotalBackupEntryCount(map1, map2));
    }

    @Test
    public void issue388NoBackupWhenSuperClient() throws InterruptedException {
        final int size = 900;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        Config scconfig = new Config();
        scconfig.setLiteMember(true);
        HazelcastInstance sc = Hazelcast.newHazelcastInstance(scconfig);
        IMap map1 = h1.getMap("def");
        IMap map2 = h2.getMap("def");
        IMap map3 = sc.getMap("def");
        for (int i = 0; i < size; ) {
            map1.put(i++, new byte[1000]);
            map2.put(i++, new byte[1000]);
            map3.put(i++, new byte[1000]);
        }
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, getTotalOwnedEntryCount(map1, map2));
        assertEquals(size, getTotalBackupEntryCount(map1, map2));
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
        ConcurrentMapManager c3 = getConcurrentMapManager(h3);
        MemberImpl member3 = (MemberImpl) h3.getCluster().getLocalMember();
        for (int i = 0; i < 271; i++) {
            PartitionInfo p = c3.getPartitionInfo(i);
            assertTrue(p.toString(), member3.getAddress().equals(p.getOwner()));
            assertFalse(p.toString(), p.isOwnerOrBackup(member1.getAddress(), PartitionInfo.MAX_REPLICA_COUNT));
            assertFalse(p.toString(), p.isOwnerOrBackup(member2.getAddress(), PartitionInfo.MAX_REPLICA_COUNT));
        }
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

    @Test
    @Ignore
    public void testCleanupAfterMigration() throws Exception {
        System.setProperty("hazelcast.log.state", "true");
        final int size = 10000;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        IMap map1 = h1.getMap("default");
        for (int i = 0; i < size; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(size, map1.size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        IMap map2 = h2.getMap("default");
        IMap map3 = h3.getMap("default");
        for (int i = 0; i < 100; i++) {
            assertEquals(size, map1.size());
            assertEquals(size, map2.size());
            assertEquals(size, map3.size());
        }
        ConcurrentMapManager c1 = getConcurrentMapManager(h1);
        ConcurrentMapManager c2 = getConcurrentMapManager(h2);
        ConcurrentMapManager c3 = getConcurrentMapManager(h3);
        c1.startCleanup(true, true);
        c2.startCleanup(true, true);
        c3.startCleanup(true, true);
        Thread.sleep(1000);
        CMap cmap1 = c1.getMap("c:default");
        CMap cmap2 = c2.getMap("c:default");
        CMap cmap3 = c3.getMap("c:default");
        // check record counts just after cleanup
        assertTrue(cmap1.mapRecords.size() <= size);
        assertTrue(cmap2.mapRecords.size() <= size);
        assertTrue(cmap3.mapRecords.size() <= size);
        assertEquals(cmap1.mapRecords.size(), getOwnedAndBackupCount(map1));
        assertEquals(cmap2.mapRecords.size(), getOwnedAndBackupCount(map2));
        assertEquals(cmap3.mapRecords.size(), getOwnedAndBackupCount(map3));
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(new Config());
        IMap map4 = h4.getMap("default");
        ConcurrentMapManager c4 = getConcurrentMapManager(h4);
        CMap cmap4 = c4.getMap("c:default");
        Thread.sleep(1000);
        c1.startCleanup(true, true);
        c2.startCleanup(true, true);
        c3.startCleanup(true, true);
        c4.startCleanup(true, true);
        Thread.sleep(8000);
        // check record counts just after cleanup
        assertEquals(cmap1.mapRecords.size(), getOwnedAndBackupCount(map1));
        assertEquals(cmap2.mapRecords.size(), getOwnedAndBackupCount(map2));
        assertEquals(cmap3.mapRecords.size(), getOwnedAndBackupCount(map3));
        assertEquals(cmap4.mapRecords.size(), getOwnedAndBackupCount(map4));
        h4.getLifecycleService().shutdown();
        Thread.sleep(1000);
        c1.startCleanup(true, true);
        c2.startCleanup(true, true);
        c3.startCleanup(true, true);
        Thread.sleep(8000);
        // check record counts just after cleanup
        assertEquals(cmap1.mapRecords.size(), getOwnedAndBackupCount(map1));
        assertEquals(cmap2.mapRecords.size(), getOwnedAndBackupCount(map2));
        assertEquals(cmap3.mapRecords.size(), getOwnedAndBackupCount(map3));
        System.setProperty("hazelcast.log.state", "false");
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
        TestMigrationListener migrationListener1 = new TestMigrationListener(2, 2);
        h1.getPartitionService().addMigrationListener(migrationListener1);
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
    public void testPartitionAndCMapRecordCounts() throws InterruptedException {
        final int[] clusterSize = new int[]{3, 5, 7, 6, 4, 2, 1};
        final int entryCount = 10000;
        final int totalPartitionCount = 271;
        final int mapBackupCount = 2;
        Config config = new Config();
        config.getProperties().put(GroupProperties.PROP_CLEANUP_DELAY_SECONDS, "1");
        config.getProperties().put(GroupProperties.PROP_PARTITION_MIGRATION_INTERVAL, "0");
        config.getProperties().put(GroupProperties.PROP_IMMEDIATE_BACKUP_INTERVAL, "0");
        config.getProperties().put(GroupProperties.PROP_PARTITION_TABLE_SEND_INTERVAL, "5");
        final String mapName = "testPartitionAndCMapRecordCounts";
        int testMapReplicaCount = mapBackupCount + 1;
        config.getMapConfig(mapName).setBackupCount(mapBackupCount);
        final IMap<Integer, Integer> testMap = Hazelcast.newHazelcastInstance(config).getMap(mapName);
        for (int i = 0; i < entryCount; i++) {
            testMap.put(i, i);
        }
        int k = 0;
        for (int i = 0; i < clusterSize.length; i++) {
            int size = clusterSize[k++ % clusterSize.length];
            System.out.println("Cluster size : " + size);
            while (Hazelcast.getAllHazelcastInstances().size() < size) {
                Hazelcast.newHazelcastInstance(config);
            }
            while (Hazelcast.getAllHazelcastInstances().size() > size) {
                Collection<HazelcastInstance> all = Hazelcast.getAllHazelcastInstances();
                for (HazelcastInstance hz : all) {
                    if (hz.getCluster().getMembers().iterator().next().localMember()) {  // shutdown master
                        hz.getLifecycleService().shutdown();
                        break;
                    }
                }
            }
            Collection<HazelcastInstance> set = Hazelcast.getAllHazelcastInstances();
            final int replicaMax = set.size();
            int wait = replicaMax * 5;
            System.out.println("Waiting " + wait + " seconds for partition arrangement...");
            Thread.sleep(1000 * wait);
            int[] partitionCounts = new int[PartitionInfo.MAX_REPLICA_COUNT];
            for (HazelcastInstance hazelcastInstance : set) {
                int[] pc = getPartitionCounts(getNode(hazelcastInstance));
                for (int j = 0; j < pc.length; j++) {
                    partitionCounts[j] += pc[j];
                }
            }
            for (int j = 0; j < partitionCounts.length; j++) {
                int partitionCount = partitionCounts[j];
                if (j < replicaMax) {
                    assertTrue("index: " + j + ", partitions: " + partitionCount + ", max-replica: " + replicaMax,
                            partitionCount == totalPartitionCount);
                } else {
                    assertTrue("index: " + j + ", partitions: " + partitionCount + ", max-replica: " + replicaMax,
                            partitionCount == 0);
                }
            }
            int[] recordCounts = new int[PartitionInfo.MAX_REPLICA_COUNT];
            for (HazelcastInstance hazelcastInstance : set) {
                int[] rc = getCMapRecordCounts(getNode(hazelcastInstance), mapName);
                for (int j = 0; j < rc.length; j++) {
                    recordCounts[j] += rc[j];
                }
            }
            for (int j = 0; j < recordCounts.length; j++) {
                int recordCount = recordCounts[j];
                if (j < Math.min(testMapReplicaCount, replicaMax)) {
                    assertTrue("index: " + j + ", records: " + recordCount + ", max-replica: " + replicaMax,
                            recordCount == entryCount);
                } else {
                    assertTrue("index: " + j + ", records: " + recordCount + ", max-replica: " + replicaMax,
                            recordCount == 0);
                }
            }
        }
    }

    private static int[] getPartitionCounts(Node node) {
        int[] counts = new int[PartitionInfo.MAX_REPLICA_COUNT];
        Address address = node.clusterManager.getThisAddress();
        PartitionManager partitionManager = node.concurrentMapManager.getPartitionManager();
        PartitionInfo[] partitions = partitionManager.getPartitions();
        for (PartitionInfo p : partitions) {
            for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                if (address.equals(p.getReplicaAddress(i))) {
                    counts[i]++;
                }
            }
        }
        return counts;
    }

    private static int[] getCMapRecordCounts(Node node, String name) {
        int[] counts = new int[PartitionInfo.MAX_REPLICA_COUNT];
        CMap map = node.concurrentMapManager.getMap(Prefix.MAP + name);
        Address address = node.clusterManager.getThisAddress();
        PartitionManager partitionManager = node.concurrentMapManager.getPartitionManager();
        Collection<Record> records = map.mapRecords.values();
        for (Record record : records) {
            int id = record.getBlockId();
            PartitionInfo p = partitionManager.getPartition(id);
            for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                if (address.equals(p.getReplicaAddress(i))) {
                    counts[i]++;
                    break;
                }
            }
        }
        return counts;
    }

    private static Node getNode(final HazelcastInstance hazelcastInstance) {
        FactoryImpl.HazelcastInstanceProxy proxy = (FactoryImpl.HazelcastInstanceProxy) hazelcastInstance;
        return proxy.getFactory().node;
    }

    @Test
    public void testIssue177BackupCount() throws InterruptedException {
        final Config config = new Config();
        config.setProperty("hazelcast.partition.migration.interval", "0");
        config.setProperty("hazelcast.wait.seconds.before.join", "1");
        config.setProperty(GroupProperties.PROP_BACKUP_REDO_ENABLED, "true");
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
        config.setProperty(GroupProperties.PROP_BACKUP_REDO_ENABLED, "true");
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
        config.setProperty(GroupProperties.PROP_BACKUP_REDO_ENABLED, "true");
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
