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
import com.hazelcast.core.*;
import com.hazelcast.impl.base.CallState;
import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.nio.Data;
import com.hazelcast.util.Clock;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class CMapTest extends TestUtil {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    @Ignore
    public void testCallState() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final Node node1 = getNode(h1);
        final Node node2 = getNode(h2);
        Thread.sleep(100);
        final CountDownLatch latch = new CountDownLatch(1);
        final IMap imap1 = h1.getMap("default");
        new Thread(new Runnable() {
            public void run() {
                imap1.lock("1");
                latch.countDown();
            }
        }).start();
        latch.await();
//        final IMap imap2 = h2.getMap("default");
        final AtomicInteger threadId = new AtomicInteger();
        new Thread(new Runnable() {
            public void run() {
                ThreadContext.get().setCurrentFactory(node1.factory);
                threadId.set(ThreadContext.get().getThreadId());
                imap1.put("1", "value1");
            }
        }).start();
        Thread.sleep(1000);
        System.out.println(node1.getThisAddress() + " thread " + threadId.get());
        CallState callState1 = node1.getSystemLogService().getCallState(node1.getThisAddress(), threadId.get());
        if (callState1 != null) {
            for (Object callStateLog : callState1.getLogs()) {
                System.out.println(callStateLog);
            }
        }
        CallState callState2 = node2.getSystemLogService().getCallState(node1.getThisAddress(), threadId.get());
        System.out.println("========================");
        if (callState2 != null) {
            for (Object callStateLog : callState2.getLogs()) {
                System.out.println(callStateLog);
            }
        }
    }

    @Test
    public void testMigrationOfTTLAndLock() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        ConcurrentMapManager cmm1 = getConcurrentMapManager(h1);
        ConcurrentMapManager cmm2 = getConcurrentMapManager(h2);
        final IMap imap1 = h1.getMap("default");
        final IMap imap2 = h2.getMap("default");
        final Data dKey = toData("1");
        assertTrue(migrateKey("1", h1, h1, 0));
        assertTrue(migrateKey("1", h1, h2, 1));
        imap1.put("1", "value1", 60, TimeUnit.SECONDS);
        imap1.lock("1");
        Future put2 = imap2.putAsync("1", "value2");
        imap2.addEntryListener(new EntryAdapter() {
            @Override
            public void entryUpdated(EntryEvent entryEvent) {
                System.out.println(entryEvent);
            }
        }, "1", true);
        if (put2 == null) fail();
        Thread.sleep(1000);
        CMap cmap1 = getCMap(h1, "default");
        CMap cmap2 = getCMap(h2, "default");
        Record record1 = cmap1.getRecord(dKey);
        assertEquals(1, record1.getScheduledActionCount());
        for (ScheduledAction scheduledAction : record1.getScheduledActions()) {
            assertTrue(scheduledAction.isValid());
        }
        assertNotNull(record1.getListeners());
        assertEquals(1, record1.getListeners().size());
        DistributedLock lock = cmap1.getRecord(dKey).getLock();
        assertTrue(cmm1.thisAddress.equals(lock.getLockAddress()));
        assertTrue(lock.getLockThreadId() != -1);
        assertEquals(1, lock.getLockCount());
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(0, cmap2.getMapIndexService().getOwnedRecords().size());
        assertEquals(1, cmap1.getMapIndexService().getOwnedRecords().size());
        assertTrue(migrateKey("1", h1, h2, 0));
        assertTrue(migrateKey("1", h1, h1, 1));
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(1, cmap2.getMapIndexService().getOwnedRecords().size());
        assertTrue(cmap1.getRecord(dKey).getRemainingTTL() < 60000);
        assertTrue(cmap2.getRecord(dKey).getRemainingTTL() < 60000);
        Record record2 = cmap2.getRecord(dKey);
        lock = record2.getLock();
        assertTrue(cmm1.thisAddress.equals(lock.getLockAddress()));
        assertTrue(lock.getLockThreadId() != -1);
        assertEquals(1, lock.getLockCount());
        lock = cmap1.getRecord(dKey).getLock();
        assertTrue(cmm1.thisAddress.equals(lock.getLockAddress()));
        assertTrue(lock.getLockThreadId() != -1);
        assertEquals(1, lock.getLockCount());
        imap1.unlock("1");
        put2.get(10, TimeUnit.SECONDS);
        assertEquals("value2", imap1.get("1"));
        assertEquals("value2", imap2.get("1"));
        assertNotNull(record2.getListeners());
        assertEquals(1, record2.getListeners().size());
    }

    @Test
    public void testPutWithTwoMember() throws Exception {
        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
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
        assertEquals(1, cmap1.getMapIndexService().getOwnedRecords().size() + cmap2.getMapIndexService().getOwnedRecords().size());
        Record record1 = cmap1.getRecord(dKey);
        Record record2 = cmap2.getRecord(dKey);
        long now = Clock.currentTimeMillis();
        long millisLeft1 = record1.getExpirationTime() - now;
        long millisLeft2 = record2.getExpirationTime() - now;
        assertTrue(millisLeft1 <= 5000 && millisLeft1 > 0);
        assertTrue(millisLeft2 <= 5000 && millisLeft2 > 0);
        assertTrue(record1.isActive());
        assertTrue(record2.isActive());
        assertEquals(1, record1.valueCount());
        assertEquals(1, record2.valueCount());
        assertEquals(dValue, record1.getValueData());
        assertEquals(dValue, record2.getValueData());
        imap1.set("2", "value2", 5, TimeUnit.SECONDS);
        assertEquals("value2", imap1.get("2"));
        assertEquals("value2", imap2.get("2"));
        Thread.sleep(6000);
        assertNull(imap1.get("2"));
        assertNull(imap2.get("2"));
        now = Clock.currentTimeMillis();
        assertFalse(record1.isValid(now));
        assertFalse(record2.isValid(now));
        Thread.sleep(23000);
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap2.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap1.mapRecords.size());
        assertEquals(0, cmap2.mapRecords.size());
        imap1.put(key, value, 10, TimeUnit.SECONDS);
        assertTrue(migrateKey(key, h1, h1, 0));
        assertTrue(migrateKey(key, h1, h2, 1));
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(1, cmap1.getMapIndexService().getOwnedRecords().size() + cmap2.getMapIndexService().getOwnedRecords().size());
        record1 = cmap1.getRecord(dKey);
        record2 = cmap2.getRecord(dKey);
        now = Clock.currentTimeMillis();
        millisLeft1 = record1.getExpirationTime() - now;
        millisLeft2 = record2.getExpirationTime() - now;
        assertTrue(millisLeft1 <= 11000 && millisLeft1 > 0);
        assertTrue(millisLeft2 <= 11000 && millisLeft2 > 0);
        assertTrue(record1.isActive());
        assertTrue(record2.isActive());
        assertTrue(record1.isValid(now));
        assertTrue(record2.isValid(now));
        assertEquals(1, record1.valueCount());
        assertEquals(1, record2.valueCount());
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(1, cmap1.getMapIndexService().getOwnedRecords().size() + cmap2.getMapIndexService().getOwnedRecords().size());
        assertTrue(migrateKey(key, h1, h2, 0));
        assertTrue(migrateKey(key, h1, h1, 1));
        cmap1.startCleanup(true);
        cmap2.startCleanup(true);
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(1, cmap2.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        now = Clock.currentTimeMillis();
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
        Thread.sleep(11000);
        now = Clock.currentTimeMillis();
        assertFalse(record1.isValid(now));
        assertFalse(record2.isValid(now));
        Thread.sleep(20000);
        assertEquals(0, cmap1.mapRecords.size());
        assertEquals(0, cmap2.mapRecords.size());
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(0, cmap2.getMapIndexService().getOwnedRecords().size());
        imap1.put("1", "value1");
        record1 = cmap1.getRecord(dKey);
        record2 = cmap2.getRecord(dKey);
        assertEquals(1, cmap1.mapRecords.size());
        assertEquals(1, cmap2.mapRecords.size());
        assertEquals(0, cmap1.getMapIndexService().getOwnedRecords().size());
        assertEquals(1, cmap2.getMapIndexService().getOwnedRecords().size());
        now = Clock.currentTimeMillis();
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
        assertEquals(0, cmap1.mapIndexService.size());
        assertEquals(0, cmap2.mapIndexService.size());
    }

    @Test
    public void testTTL() throws Exception {
        Config config = new Config();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        // we mocked the node
        // do not forget to shutdown the connectionManager
        // so that server socket can be released.
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
        assertNotNull(cmap.get(newGetRequest(dKey)));
        assertEquals(dValue, cmap.get(newGetRequest(dKey)));
        assertTrue(record.getRemainingTTL() > 1000);
        Thread.sleep(1000);
        assertTrue(record.getRemainingTTL() < 2100);
        cmap.put(newPutRequest(dKey, dValue));
        assertTrue(record.getRemainingTTL() > 2001);
        assertTrue(record.isActive());
        assertTrue(record.isValid());
        Thread.sleep(1000);
        assertTrue(record.getRemainingTTL() < 2100);
        cmap.put(newPutRequest(dKey, dValue));
        assertTrue(record.getRemainingTTL() > 2001);
        assertTrue(record.isActive());
        assertTrue(record.isValid());
        Thread.sleep(5000);
        assertEquals(0, cmap.size());
        assertTrue(cmap.evict(newEvictRequest(dKey)));
        assertTrue(cmap.shouldPurgeRecord(record, Clock.currentTimeMillis() + 10000));
        cmap.removeAndPurgeRecord(record);
        assertEquals(0, cmap.mapRecords.size());
        assertEquals(0, cmap.size());
        assertEquals(0, cmap.mapIndexService.size());
        node.connectionManager.shutdown();
    }

    @Test
    public void testPut() throws Exception {
        Config config = new Config();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        // we mocked the node
        // do not forget to shutdown the connectionManager
        // so that server socket can be released.
        Node node = new Node(mockFactory, config);
        node.serviceThread = Thread.currentThread();
        CMap cmap = new CMap(node.concurrentMapManager, "c:myMap");
        Object key = "1";
        Object value = "istanbul";
        Data dKey = toData(key);
        Data dKey2 = toData("2");
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
        assertTrue(Clock.currentTimeMillis() - record.getRemoveTime() < 100);
        assertEquals(1, cmap.mapRecords.size());
        record = cmap.getRecord(dKey);
        assertNotNull(record);
        assertFalse(record.isActive());
        assertFalse(record.isValid());
        assertEquals(0, cmap.size());
        cmap.put(newPutRequest(dKey, dValue, 1000));
        cmap.put(newPutIfAbsentRequest(dKey2, dValue, 1000));
        assertEquals(0, record.getRemoveTime());
        assertTrue(cmap.mapRecords.containsKey(toData(key)));
        assertTrue(cmap.mapRecords.containsKey(toData("2")));
        Thread.sleep(1500);
        assertEquals(0, cmap.size());
        assertFalse(cmap.containsKey(newContainsRequest(dKey, null)));
        assertFalse(cmap.containsKey(newContainsRequest(dKey2, null)));
        node.connectionManager.shutdown();
    }

    @Test
    public void testContainsKeyUpdatesLastAccessTime() throws Exception {
        Config config = new Config();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        // we mocked the node
        // do not forget to shutdown the connectionManager
        // so that server socket can be released.
        Node node = new Node(mockFactory, config);
        node.serviceThread = Thread.currentThread();
        CMap cmap = new CMap(node.concurrentMapManager, "c:myMap");
        Object key = "1";
        Object value = "istanbul";
        Data dKey = toData(key);
        Data dValue = toData(value);
        cmap.put(newPutRequest(dKey, dValue));
        Record record = cmap.getRecord(dKey);
        long firstAccessTime = record.getLastAccessTime();
        Thread.sleep(10);
        assertTrue(cmap.containsKey(newContainsRequest(dKey, null)));
        record = cmap.getRecord(dKey);
        long secondAccessTime = record.getLastAccessTime();
        assertTrue("record access time should have been updated on containsKey()", secondAccessTime > firstAccessTime);
        node.connectionManager.shutdown();
    }
}
