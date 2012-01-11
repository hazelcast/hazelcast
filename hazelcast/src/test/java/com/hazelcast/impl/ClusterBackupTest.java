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
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

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
        IMap map2 = h2.getMap("default");
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, getTotalOwnedEntryCount(map1, map2));
        assertEquals(size, getTotalBackupEntryCount(map1, map2));
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        IMap map3 = h3.getMap("default");
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        sleep(3000);
        assertEquals(size, getTotalOwnedEntryCount(map1, map2, map3));
        assertEquals(size, getTotalBackupEntryCount(map1, map2, map3));
        h2.getLifecycleService().shutdown();
        assertEquals(size, map1.size());
        assertEquals(size, map3.size());
        sleep(3000);
        assertEquals(size, getTotalOwnedEntryCount(map1, map3));
        assertEquals(size, getTotalBackupEntryCount(map1, map3));
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
}
