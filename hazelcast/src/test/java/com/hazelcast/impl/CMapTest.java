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
import com.hazelcast.core.Prefix;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class CMapTest {

    @Test
    public void testTwoMemberPut() throws Exception {
        Config config = new XmlConfigBuilder().build();
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
        MigrationListener migrationListener  = new MigrationListener() {
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

    public static ConcurrentMapManager getConcurrentMapManager(HazelcastInstance h) {
        FactoryImpl.HazelcastInstanceProxy hiProxy = (FactoryImpl.HazelcastInstanceProxy) h;
        return hiProxy.getFactory().node.concurrentMapManager;
    }

    public static CMap getCMap(HazelcastInstance h, String name) {
        ConcurrentMapManager concurrentMapManager = getConcurrentMapManager(h);
        String fullName = Prefix.MAP + name;
        return concurrentMapManager.getMap(fullName);
    }

    @Test
    public void testTTL() throws Exception {
        Config config = new XmlConfigBuilder().build();
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
        Config config = new XmlConfigBuilder().build();
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
        Thread.sleep(1000);
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
