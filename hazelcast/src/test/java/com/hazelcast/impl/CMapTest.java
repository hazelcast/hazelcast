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
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class CMapTest extends TestUtil {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 10000)
    public void testAddListenerInfiniteLoop() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        ConcurrentMapManager concurrentMapManager = getConcurrentMapManager(h1);
        ListenerManager lm = concurrentMapManager.node.listenerManager;
        ListenerManager.AddRemoveListener arl = lm.new AddRemoveListener("default", true, true);
        BaseManager.TargetAwareOp op = arl.createNewTargetAwareOp(new Address("127.0.0.1", 6666));
        op.doOp();
        assertEquals(Constants.Objects.OBJECT_REDO, op.getResult());
    }

    @Test
    public void testTwoMemberPut() throws Exception {
        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap imap1 = h1.getMap("default");
        IMap imap2 = h2.getMap("default");
        assertEquals(0, imap1.size());
        assertEquals(0, imap2.size());
        CMap cmap1 = getCMap(h1, "default");
        CMap cmap2 = getCMap(h2, "default");
        assertNotNull(cmap1);
        assertNotNull(cmap2);
        Object key = "1";
        Object value = "value";
        Data dKey = toData(key);
        Data dValue = toData(value);
        imap1.put(key, value, 5, TimeUnit.SECONDS);
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(1, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap2.getMapIndexService().getOwnedRecords().size());
        Record record1 = cmap1.getRecord(dKey);
        Record record2 = cmap2.getRecord(dKey);
        long now = System.currentTimeMillis();
        long millisLeft1 = record1.getExpirationTime() - now;
        long millisLeft2 = record2.getExpirationTime() - now;
        assertTrue(millisLeft1 <= 5000 && millisLeft1 > 0);
        assertTrue(millisLeft2 <= 5000 && millisLeft2 > 0);
        assertTrue(record1.isActive());
        assertTrue(record2.isActive());
        assertEquals(1, record1.valueCount());
        assertEquals(1, record2.valueCount());
        assertEquals(dValue, record1.getValue());
        assertEquals(dValue, record2.getValue());
        Thread.sleep(6000);
        now = System.currentTimeMillis();
        assertFalse(record1.isValid(now));
        assertFalse(record2.isValid(now));
        Thread.sleep(20000);
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap2.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap1.mapRecords.size());
        assertEquals(0, cmap2.mapRecords.size());
        imap1.put(key, value, 10, TimeUnit.SECONDS);
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(1, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap2.getMapIndexService().getOwnedRecords().size());
        record1 = cmap1.getRecord(dKey);
        record2 = cmap2.getRecord(dKey);
        now = System.currentTimeMillis();
        millisLeft1 = record1.getExpirationTime() - now;
        millisLeft2 = record2.getExpirationTime() - now;
        assertTrue(millisLeft1 <= 10000 && millisLeft1 > 0);
        assertTrue(millisLeft2 <= 10000 && millisLeft2 > 0);
        assertTrue(record1.isActive());
        assertTrue(record2.isActive());
        assertTrue(record1.isValid(now));
        assertTrue(record2.isValid(now));
        assertEquals(1, record1.valueCount());
        assertEquals(1, record2.valueCount());
        assertTrue(migrateKey(key, h1, h2));
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(1, cmap2.getMapIndexService().getOwnedRecords().size());
        now = System.currentTimeMillis();
        millisLeft1 = record1.getExpirationTime() - now;
        millisLeft2 = record2.getExpirationTime() - now;
        assertTrue(millisLeft1 <= 10000 && millisLeft1 > 0);
        assertTrue(millisLeft2 <= 10000 && millisLeft2 > 0);
        assertTrue(record1.isActive());
        assertTrue(record2.isActive());
        assertTrue(record1.isValid(now));
        assertTrue(record2.isValid(now));
        assertEquals(1, record1.valueCount());
        assertEquals(1, record2.valueCount());
        Thread.sleep(10000);
        now = System.currentTimeMillis();
        assertFalse(record1.isValid(now));
        assertFalse(record2.isValid(now));
        Thread.sleep(20000);
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap2.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap1.mapRecords.size());
        assertEquals(0, cmap2.mapRecords.size());
        imap1.put("1", "value1");
        record1 = cmap1.getRecord(dKey);
        record2 = cmap2.getRecord(dKey);
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(1, cmap2.getMapIndexService().getOwnedRecords().size());
        now = System.currentTimeMillis();
        assertEquals(Long.MAX_VALUE, record1.getExpirationTime());
        assertEquals(Long.MAX_VALUE, record2.getExpirationTime());
        assertTrue(record1.isActive());
        assertTrue(record2.isActive());
        assertTrue(record1.isValid(now));
        assertTrue(record2.isValid(now));
        assertEquals(1, record1.valueCount());
        assertEquals(1, record2.valueCount());
        imap1.remove("1");
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap2.getMapIndexService().getOwnedRecords().size());
        Thread.sleep(20000);
        assertEquals(0, cmap1.mapRecords.size());
        assertEquals(0, cmap2.mapRecords.size());
    }

    public static boolean migrateKey(Object key, HazelcastInstance oldest, HazelcastInstance to) throws Exception {
        final MemberImpl currentOwnerMember = (MemberImpl) oldest.getPartitionService().getPartition(key).getOwner();
        final MemberImpl toMember = (MemberImpl) to.getCluster().getLocalMember();
        if (currentOwnerMember.equals(toMember)) {
            return false;
        }
        final ConcurrentMapManager concurrentMapManagerOldest = getConcurrentMapManager(oldest);
        final Address addressCurrentOwner = currentOwnerMember.getAddress();
        final Address addressNewOwner = toMember.getAddress();
        final int blockId = oldest.getPartitionService().getPartition(key).getPartitionId();
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == blockId && migrationEvent.getNewOwner().equals(toMember)) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        oldest.getPartitionService().addMigrationListener(migrationListener);
        to.getPartitionService().addMigrationListener(migrationListener);
        concurrentMapManagerOldest.enqueueAndReturn(new Processable() {
            public void process() {
                Block blockToMigrate = new Block(blockId, addressCurrentOwner, addressNewOwner);
                concurrentMapManagerOldest.partitionManager.lsBlocksToMigrate.add(blockToMigrate);
                concurrentMapManagerOldest.partitionManager.initiateMigration();
            }
        });
        if (!migrationLatch.await(20, TimeUnit.SECONDS)) {
            fail("Migration should get completed in 20 seconds!!");
        }
        assertEquals(toMember, oldest.getPartitionService().getPartition(key).getOwner());
        assertEquals(toMember, to.getPartitionService().getPartition(key).getOwner());
        return true;
    }

    @Test
    public void testShutdownSecondNodeWhileMigrating() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap imap1 = h1.getMap("default");
        for (int i = 0; i < 10000; i++) {
            imap1.put(i, "value" + i);
        }
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        migratePartition(28, h1, h2);
        assertEquals(getPartitionById(h1.getPartitionService(), 28).getOwner(), h2.getCluster().getLocalMember());
        assertEquals(getPartitionById(h2.getPartitionService(), 28).getOwner(), h2.getCluster().getLocalMember());
        assertEquals(getPartitionById(h3.getPartitionService(), 28).getOwner(), h2.getCluster().getLocalMember());
        initiateMigration(28, 20, h1, h2, h1);
        final ConcurrentMapManager chm2 = getConcurrentMapManager(h2);
        chm2.enqueueAndWait(new Processable() {
            public void process() {
                assertTrue(chm2.partitionManager.blocks[28].isMigrating());
            }
        }, 10);
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == 28 && migrationEvent.getNewOwner().equals(h1.getCluster().getLocalMember())) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        h3.getPartitionService().addMigrationListener(migrationListener);
        h1.getPartitionService().addMigrationListener(migrationListener);
        h2.getLifecycleService().shutdown();
        if (!migrationLatch.await(40, TimeUnit.SECONDS)) {
            for (Block block : getConcurrentMapManager(h1).partitionManager.blocks) {
                if (block.isMigrating()) {
                    System.out.println(block);
                }
            }
            for (Block block : getConcurrentMapManager(h3).partitionManager.blocks) {
                if (block.isMigrating()) {
                    System.out.println(block);
                }
            }
            fail("Migration should get completed in 20 seconds!!");
        }
    }

    @Test
    public void testShutdownOldestMemberWhileMigrating() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap imap1 = h1.getMap("default");
        for (int i = 0; i < 10000; i++) {
            imap1.put(i, "value" + i);
        }
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        assertEquals(getPartitionById(h1.getPartitionService(), 28).getOwner(), h1.getCluster().getLocalMember());
        assertEquals(getPartitionById(h2.getPartitionService(), 28).getOwner(), h1.getCluster().getLocalMember());
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == 28 && migrationEvent.getNewOwner().equals(h2.getCluster().getLocalMember())) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        h3.getPartitionService().addMigrationListener(migrationListener);
        h2.getPartitionService().addMigrationListener(migrationListener);
        initiateMigration(28, 20, h1, h1, h2);
        final ConcurrentMapManager chm1 = getConcurrentMapManager(h1);
        chm1.enqueueAndWait(new Processable() {
            public void process() {
                assertTrue(chm1.partitionManager.blocks[28].isMigrating());
            }
        }, 10);
        h1.getLifecycleService().shutdown();
        if (!migrationLatch.await(30, TimeUnit.SECONDS)) {
            for (Block block : getConcurrentMapManager(h2).partitionManager.blocks) {
                if (block.isMigrating() || block.getBlockId() == 28) {
                    System.out.println(block);
                }
            }
            for (Block block : getConcurrentMapManager(h3).partitionManager.blocks) {
                if (block.isMigrating() || block.getBlockId() == 28) {
                    System.out.println(block);
                }
            }
            fail("Migration should get completed in 30 seconds!!");
        }
    }

    @Test
    public void testShutdownMigrationTargetNodeWhileMigrating() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap imap1 = h1.getMap("default");
        for (int i = 0; i < 10000; i++) {
            imap1.put(i, "value" + i);
        }
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        assertEquals(getPartitionById(h1.getPartitionService(), 28).getOwner(), h1.getCluster().getLocalMember());
        assertEquals(getPartitionById(h2.getPartitionService(), 28).getOwner(), h1.getCluster().getLocalMember());
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == 28 && migrationEvent.getNewOwner().equals(h1.getCluster().getLocalMember())) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        h1.getPartitionService().addMigrationListener(migrationListener);
        h2.getPartitionService().addMigrationListener(migrationListener);
        initiateMigration(28, 20, h1, h1, h3);
        final ConcurrentMapManager chm1 = getConcurrentMapManager(h1);
        chm1.enqueueAndWait(new Processable() {
            public void process() {
                assertTrue(chm1.partitionManager.blocks[28].isMigrating());
            }
        }, 10);
        h3.getLifecycleService().shutdown();
        if (!migrationLatch.await(30, TimeUnit.SECONDS)) {
            for (Block block : getConcurrentMapManager(h1).partitionManager.blocks) {
                if (block.isMigrating() || block.getBlockId() == 28) {
                    System.out.println(block);
                }
            }
            for (Block block : getConcurrentMapManager(h2).partitionManager.blocks) {
                if (block.isMigrating() || block.getBlockId() == 28) {
                    System.out.println(block);
                }
            }
            fail("Migration should get completed in 30 seconds!!");
        }
    }

    public static boolean migratePartition(int partitionId, HazelcastInstance oldest, HazelcastInstance to) throws Exception {
        final MemberImpl currentOwnerMember = (MemberImpl) getPartitionById(oldest.getPartitionService(), partitionId).getOwner();
        final MemberImpl toMember = (MemberImpl) to.getCluster().getLocalMember();
        if (currentOwnerMember.equals(toMember)) {
            return false;
        }
        final ConcurrentMapManager concurrentMapManagerOldest = getConcurrentMapManager(oldest);
        final Address addressCurrentOwner = currentOwnerMember.getAddress();
        final Address addressNewOwner = toMember.getAddress();
        final int blockId = getPartitionById(oldest.getPartitionService(), partitionId).getPartitionId();
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == blockId && migrationEvent.getNewOwner().equals(toMember)) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        oldest.getPartitionService().addMigrationListener(migrationListener);
        to.getPartitionService().addMigrationListener(migrationListener);
        concurrentMapManagerOldest.enqueueAndReturn(new Processable() {
            public void process() {
                Block blockToMigrate = new Block(blockId, addressCurrentOwner, addressNewOwner);
                concurrentMapManagerOldest.partitionManager.lsBlocksToMigrate.add(blockToMigrate);
                concurrentMapManagerOldest.partitionManager.initiateMigration();
            }
        });
        if (!migrationLatch.await(30, TimeUnit.SECONDS)) {
            fail("Migration should get completed in 30 seconds!!");
        }
        assertEquals(toMember, getPartitionById(oldest.getPartitionService(), partitionId).getOwner());
        assertEquals(toMember, getPartitionById(to.getPartitionService(), partitionId).getOwner());
        return true;
    }

    public static boolean initiateMigration(final int partitionId, final int completeWaitSeconds, HazelcastInstance oldest, HazelcastInstance from, HazelcastInstance to) throws Exception {
        final MemberImpl currentOwnerMember = (MemberImpl) getPartitionById(oldest.getPartitionService(), partitionId).getOwner();
        final MemberImpl toMember = (MemberImpl) to.getCluster().getLocalMember();
        if (currentOwnerMember.equals(toMember)) {
            return false;
        }
        final ConcurrentMapManager concurrentMapManagerOldest = getConcurrentMapManager(oldest);
        final ConcurrentMapManager concurrentMapManagerFrom = getConcurrentMapManager(from);
        final Address addressCurrentOwner = currentOwnerMember.getAddress();
        final Address addressNewOwner = toMember.getAddress();
        final int blockId = getPartitionById(oldest.getPartitionService(), partitionId).getPartitionId();
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
                migrationLatch.countDown();
            }
        };
        from.getPartitionService().addMigrationListener(migrationListener);
        to.getPartitionService().addMigrationListener(migrationListener);
        concurrentMapManagerFrom.enqueueAndReturn(new Processable() {
            public void process() {
                concurrentMapManagerFrom.partitionManager.MIGRATION_COMPLETE_WAIT_SECONDS = completeWaitSeconds;
            }
        });
        concurrentMapManagerOldest.enqueueAndReturn(new Processable() {
            public void process() {
                Block blockToMigrate = new Block(blockId, addressCurrentOwner, addressNewOwner);
                concurrentMapManagerOldest.partitionManager.lsBlocksToMigrate.add(blockToMigrate);
                concurrentMapManagerOldest.partitionManager.initiateMigration();
            }
        });
        if (!migrationLatch.await(20, TimeUnit.SECONDS)) {
            fail("Migration should get started in 20 seconds!!");
        }
        return true;
    }

    public static Partition getPartitionById(PartitionService partitionService, int partitionId) {
        for (Partition partition : partitionService.getPartitions()) {
            if (partition.getPartitionId() == partitionId) {
                return partition;
            }
        }
        return null;
    }

    @Test
    public void testTTL() throws Exception {
        Config config = new Config();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        Node node = new Node(mockFactory, config);
        node.serviceThread = Thread.currentThread();
        CMap cmap = new CMap(node.concurrentMapManager, "c:myMap");
        Object key = "1";
        Object value = "istanbul";
        Data dKey = toData(key);
        Data dValue = toData(value);
        Request reqPut = newPutRequest(dKey, dValue);
        reqPut.ttl = 3000;
        cmap.put(reqPut);
        assertTrue(cmap.mapRecords.containsKey(toData(key)));
        Data actualValue = cmap.get(newGetRequest(dKey));
        assertThat(toObject(actualValue), equalTo(value));
        assertEquals(1, cmap.mapRecords.size());
        Record record = cmap.getRecord(dKey);
        assertNotNull(record);
        assertTrue(record.isActive());
        assertTrue(record.isValid());
        assertEquals(1, cmap.size());
        assertNotNull(cmap.locallyOwnedMap);
        assertNotNull(cmap.get(newGetRequest(dKey)));
        assertEquals(dValue, cmap.get(newGetRequest(dKey)));
        assertEquals(value, cmap.locallyOwnedMap.get(key));
        assertEquals(1, cmap.locallyOwnedMap.mapCache.size());
        assertTrue(record.getRemainingTTL() > 1000);
        Thread.sleep(1000);
        assertTrue(record.getRemainingTTL() < 2001);
        cmap.put(newPutRequest(dKey, dValue));
        assertTrue(record.getRemainingTTL() > 2001);
        assertTrue(record.isActive());
        assertTrue(record.isValid());
        Thread.sleep(1000);
        assertTrue(record.getRemainingTTL() < 2001);
        cmap.put(newPutRequest(dKey, dValue));
        assertTrue(record.getRemainingTTL() > 2001);
        assertTrue(record.isActive());
        assertTrue(record.isValid());
        Thread.sleep(5000);
        cmap.locallyOwnedMap.evict(System.currentTimeMillis());
        assertEquals(0, cmap.locallyOwnedMap.mapCache.size());
        assertEquals(OBJECT_REDO, cmap.locallyOwnedMap.get(key));
        assertEquals(0, cmap.size());
        assertTrue(cmap.evict(newEvictRequest(dKey)));
        assertTrue(cmap.shouldPurgeRecord(record, System.currentTimeMillis() + 10000));
        cmap.removeAndPurgeRecord(record);
        assertEquals(0, cmap.mapRecords.size());
    }

    @Test
    public void testPut() throws Exception {
        Config config = new Config();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        Node node = new Node(mockFactory, config);
        node.serviceThread = Thread.currentThread();
        CMap cmap = new CMap(node.concurrentMapManager, "c:myMap");
        Object key = "1";
        Object value = "istanbul";
        Data dKey = toData(key);
        Data dValue = toData(value);
        cmap.put(newPutRequest(dKey, dValue));
        assertTrue(cmap.mapRecords.containsKey(toData(key)));
        Data actualValue = cmap.get(newGetRequest(dKey));
        assertThat(toObject(actualValue), equalTo(value));
        assertEquals(1, cmap.mapRecords.size());
        Record record = cmap.getRecord(dKey);
        assertNotNull(record);
        assertTrue(record.isActive());
        assertTrue(record.isValid());
        assertEquals(1, cmap.size());
        cmap.remove(newRemoveRequest(dKey));
        assertTrue(System.currentTimeMillis() - record.getRemoveTime() < 100);
        assertEquals(1, cmap.mapRecords.size());
        record = cmap.getRecord(dKey);
        assertNotNull(record);
        assertFalse(record.isActive());
        assertTrue(record.isValid());
        assertEquals(0, cmap.size());
        cmap.put(newPutRequest(dKey, dValue, 1000));
        assertEquals(0, record.getRemoveTime());
        assertTrue(cmap.mapRecords.containsKey(toData(key)));
        Thread.sleep(1500);
        assertEquals(0, cmap.size());
        assertFalse(cmap.contains(newContainsRequest(dKey, null)));
    }

    public static Request newPutRequest(Data key, Data value) {
        return newPutRequest(key, value, -1);
    }

    public static Request newPutRequest(Data key, Data value, long ttl) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_PUT, key, value, ttl);
    }

    public static Request newRequest(ClusterOperation operation, Data key, Data value, long ttl) {
        Request request = new Request();
        request.setLocal(operation, null, key, value, -1, -1, ttl, null);
        return request;
    }

    public static Request newRemoveRequest(Data key) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_REMOVE, key, null, -1);
    }

    public static Request newEvictRequest(Data key) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_EVICT, key, null, -1);
    }

    public static Request newGetRequest(Data key) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_GET, key, null, -1);
    }

    public static Request newContainsRequest(Data key, Data value) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_CONTAINS, key, value, -1);
    }
}
