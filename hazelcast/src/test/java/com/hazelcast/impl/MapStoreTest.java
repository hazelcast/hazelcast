/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.impl.ListenerLifecycleTest.CountingCountdownLatch;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Data;
import com.hazelcast.query.SqlPredicate;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.nio.IOUtil.toData;
import static org.junit.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapStoreTest extends TestUtil {

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

    @Test
    public void testPersistentQueue() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Config config = newConfig("themap", testMapStore, 0);
        config.getQueueConfig("default").setBackingMapRef("themap");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IQueue q1 = h1.getQueue("default");
        for (int i = 0; i < 100; i++) {
            q1.put("value" + i);
        }
        assertEquals(100, q1.size());
        assertEquals(100, testMapStore.store.size());
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(config);
        q1 = h1.getQueue("default");
        assertEquals(100, q1.size());
        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, q1.take());
        }
        for (int i = 0; i < 100; i++) {
            q1.put("value" + i);
        }
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IQueue q2 = h2.getQueue("default");
        assertEquals(100, q2.size());
        h1.getLifecycleService().shutdown();
        assertEquals(100, q2.size());
        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, q2.take());
        }
    }

    @Test
    public void testGetAllKeys() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Map store = testMapStore.getStore();
        Set keys = new HashSet();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            store.put(i, "value" + i);
            keys.add(i);
        }
        Config config = newConfig(testMapStore, 2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        IMap map3 = h3.getMap("default");
        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));
        assertEquals("value1", map3.get(1));
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        assertEquals(1000, map3.size());
        h3.getLifecycleService().shutdown();
        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
    }

    @Test
    public void testThreeMemberInit() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        testMapStore.setLoadAllKeys(true);
        Map store = testMapStore.getStore();
        Set keys = new HashSet();
        int size = 10000;
        for (int i = 0; i < size; i++) {
            store.put(i, "value" + i);
            keys.add(i);
        }
        final String mapName = "mymap";
        Config config = newConfig(mapName, testMapStore, 2);
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance[] instances = new HazelcastInstance[]{h1, h2, h3};
        final Random random = new Random();
        final CountDownLatch latch = new CountDownLatch(50);
        for (int i = 0; i < 50; i++) {
            new Thread(new Runnable() {
                public void run() {
                    instances[random.nextInt(3)].getMap(mapName);
                    latch.countDown();
                }
            }).start();
        }
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        instances[0].getMap(mapName);
        instances[1].getMap(mapName);
        instances[2].getMap(mapName);
        assertEquals("After load:", 15, testMapStore.callCount.get());
        IMap map1 = h1.getMap(mapName);
        IMap map2 = h2.getMap(mapName);
        IMap map3 = h3.getMap(mapName);
        for (int i = 0; i < size; i++) {
            assertEquals("value" + i, map3.get(i));
        }
        assertEquals("After gets:", 15, testMapStore.callCount.get());
    }

    @Test
    public void testThreeMemberGetAll() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        testMapStore.setLoadAllKeys(false);
        Map store = testMapStore.getStore();
        Set keys = new HashSet();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            store.put(i, "value" + i);
            keys.add(i);
        }
        Config config = newConfig(testMapStore, 2);
        config.setProperty(GroupProperties.PROP_PARTITION_MIGRATION_INTERVAL, "0");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL_KEYS, testMapStore.waitForEvent(5));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL_KEYS, testMapStore.waitForEvent(5));
        final CountDownLatch l = new CountDownLatch(1);
        map1.addEntryListener(new EntryAdapter() {

            public void entryAdded(EntryEvent entryEvent) {
                assertEquals("value1", entryEvent.getValue());
                l.countDown();
            }
        }, 1, true);
        assertEquals("value1", map1.get(1));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(5));
        assertTrue(l.await(10, TimeUnit.SECONDS));
        assertEquals("value1", map1.get(1));
        assertEquals(null, testMapStore.waitForEvent(3));
        Map loaded = map1.getAll(keys);
        assertEquals(size, loaded.size());
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL, testMapStore.waitForEvent(5));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL, testMapStore.waitForEvent(5));
        loaded = map2.getAll(keys);
        assertEquals(size, loaded.size());
        assertEquals(null, testMapStore.waitForEvent(5));
        for (int i = 0; i < size; i++) {
            map1.evict(i);
        }
        assertEquals(0, map1.size());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        Thread.sleep(5000); // wait till partitions become stable
        loaded = map1.getAll(keys);
        assertEquals(size, loaded.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL, testMapStore.waitForEvent(5));
        }
        assertEquals(0, testMapStore.getEventCount());
        loaded = map2.getAll(keys);
        assertEquals(size, loaded.size());
        assertEquals(null, testMapStore.waitForEvent(5));
    }

    @Test
    public void testOneMemberWriteThroughTxnalFailingStore() {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        testMapStore.setFail(false);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        Transaction txn = h1.getTransaction();
        txn.begin();
        assertEquals(0, map.size());
        assertEquals(0, testMapStore.dbSize());
        map.put("1", "value1");
        map.put("2", "value2");
        txn.commit();
        assertEquals(2, map.size());
        assertEquals(2, testMapStore.dbSize());
        txn = h1.getTransaction();
        txn.begin();
        assertEquals(2, map.size());
        assertEquals(2, testMapStore.dbSize());
        map.put("3", "value3");
        assertEquals(3, map.size());
        assertEquals(2, testMapStore.dbSize());
        testMapStore.setFail(true);
        map.put("4", "value4");
        try {
            txn.commit();
            fail("Should not commit the txn");
        } catch (Exception e) {
        }
        assertEquals(2, map.size());
        assertEquals(2, testMapStore.dbSize());
        map.evict("1");
        map.evict("2");
        assertEquals(0, map.size());
        assertEquals(2, testMapStore.dbSize());
        Set keys = new HashSet();
        keys.add("1");
        keys.add("2");
        try {
            map.getAll(keys);
            fail();
        } catch (Exception e) {
        }
        assertEquals(0, map.size());
        assertEquals(2, testMapStore.dbSize());
        testMapStore.setFail(false);
        map.getAll(keys);
        assertEquals(2, map.size());
        assertEquals(2, testMapStore.dbSize());
    }

    @Test
    public void testOneMemberWriteThroughFailingStore() throws Exception {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        testMapStore.setFail(true);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        try {
            map.get("1");
            fail("should have thrown exception");
        } catch (Exception e) {
        }
        assertEquals(1, testMapStore.loads.get());
        try {
            map.get("1");
            fail("should have thrown exception");
        } catch (Exception e) {
        }
        assertEquals(2, testMapStore.loads.get());
        try {
            map.put("1", "value");
            fail("should have thrown exception");
        } catch (Exception e) {
        }
        assertEquals(1, testMapStore.stores.get());
    }

    @Test
    public void testTwoMemberWriteThrough2() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1000, 0, 0);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        for (int i = 0; i < 1000; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(1000, testMapStore.getStore().size());
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        testMapStore.assertAwait(10);
        // 1000 put-load 1000 put-store call and 2 loadAllKeys
        assertEquals(2002, testMapStore.callCount.get());
    }

    @Test
    public void testTwoMemberWriteThrough() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        Employee employee = new Employee("joe", 25, true, 100.00);
        Employee employee2 = new Employee("jay", 35, false, 100.00);
        testMapStore.insert("1", employee);
        IMap map = h1.getMap("default");
        map.addIndex("name", false);
        assertEquals(0, map.size());
        assertEquals(employee, map.get("1"));
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        Collection values = map.values(new SqlPredicate("name = 'joe'"));
        assertEquals(1, values.size());
        assertEquals(employee, values.iterator().next());
        map.put("2", employee2);
        assertEquals(employee2, testMapStore.getStore().get("2"));
        assertEquals(2, testMapStore.getStore().size());
        assertEquals(2, map.size());
        map.remove("2");
        assertEquals(1, testMapStore.getStore().size());
        assertEquals(1, map.size());
        testMapStore.assertAwait(10);
        assertEquals(6, testMapStore.callCount.get());
    }

    @Test
    public void testOneMemberWriteThrough() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        Employee employee = new Employee("joe", 25, true, 100.00);
        Employee newEmployee = new Employee("ali", 26, true, 1000);
        testMapStore.insert("1", employee);
        testMapStore.insert("2", employee);
        testMapStore.insert("3", employee);
        testMapStore.insert("4", employee);
        testMapStore.insert("5", employee);
        testMapStore.insert("6", employee);
        testMapStore.insert("8", employee);
        IMap map = h1.getMap("default");
        map.addIndex("name", false);
        assertEquals(0, map.size());
        assertTrue(map.tryLock("1"));
        assertEquals(employee, map.get("1"));
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        assertEquals(employee, map.put("2", newEmployee));
        assertEquals(newEmployee, testMapStore.getStore().get("2"));
        assertEquals(2, map.size());
        Collection values = map.values(new SqlPredicate("name = 'joe'"));
        assertEquals(1, values.size());
        assertEquals(employee, values.iterator().next());
        map.remove("1");
        map.put("1", employee, 1, TimeUnit.SECONDS);
        Thread.sleep(2000);
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(employee, map.get("1"));
        map.evict("2");
        assertEquals(newEmployee, map.get("2"));
        assertEquals(employee, map.tryLockAndGet("3", 1, TimeUnit.SECONDS));
        assertEquals(employee, map.put("3", newEmployee));
        assertEquals(newEmployee, map.get("3"));
        assertEquals(employee, map.remove("4"));
        assertEquals(employee, map.tryLockAndGet("5", 1, TimeUnit.SECONDS));
        assertEquals(employee, map.remove("5"));
        assertEquals(employee, map.putIfAbsent("6", newEmployee));
        assertEquals(employee, map.get("6"));
        assertEquals(employee, testMapStore.getStore().get("6"));
        assertNull(map.get("7"));
        assertFalse(map.containsKey("7"));
        assertNull(map.putIfAbsent("7", employee));
        assertEquals(employee, map.get("7"));
        assertEquals(employee, testMapStore.getStore().get("7"));
        assertTrue(map.containsKey("8"));
        assertEquals(employee, map.get("8"));
    }

    @Test
    public void testOneMemberWriteThroughWithLRU() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        config.getMapConfig("default").setMaxSize(10).setEvictionPolicy("LRU");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        for (int i = 0; i < 20; i++) {
            map.put(i, new Employee("joe", i, true, 100.00));
        }
        assertTrue(map.size() > 5);
        assertTrue(map.size() <= 10);
    }

    @Test
    public void testOneMemberWriteThroughWithIndex() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        testMapStore.insert("1", "value1");
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        assertTrue(map.tryLock("1", 1, TimeUnit.SECONDS));
        assertEquals("value1", map.get("1"));
        map.unlock("1");
        assertEquals("value1", map.put("1", "value2"));
        assertEquals("value2", map.get("1"));
        assertEquals("value2", testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        assertTrue(map.evict("1"));
        assertEquals(0, map.size());
        assertEquals(1, testMapStore.getStore().size());
        assertEquals("value2", map.get("1"));
        assertEquals(1, map.size());
        map.remove("1");
        assertEquals(0, map.size());
        assertEquals(0, testMapStore.getStore().size());
        testMapStore.assertAwait(1);
        assertEquals(1, testMapStore.getInitCount());
        assertEquals("default", testMapStore.getMapName());
        assertEquals(h1, testMapStore.getHazelcastInstance());
    }

    @Test
    public void testOneMemberFailingWriteBehind() throws Exception {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        Config config = newConfig(testMapStore, 2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        CMap cmap = getCMap(h1, "default");
        assertEquals(0, testMapStore.db.size());
        testMapStore.setFail(true);
        map.put("1", "value1");
        Thread.sleep(3000);
        BlockingQueue listener = new LinkedBlockingQueue();
        testMapStore.addListener(listener);
        cmap.startCleanup(false);
        assertNotNull(listener.poll(20, TimeUnit.SECONDS));
        assertEquals(1, map.size());
        assertEquals(0, testMapStore.db.size());
        testMapStore.setFail(false);
        cmap.startCleanup(false);
        assertNotNull(listener.poll(20, TimeUnit.SECONDS));
        assertEquals(1, testMapStore.db.size());
        assertEquals("value1", testMapStore.db.get("1"));
    }

    @Test
    public void testOneMemberFlushOnShutdown() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 200);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        assertEquals(0, map1.size());
        for (int i = 0; i < 100; i++) {
            map1.put(i, i);
        }
        assertEquals(100, map1.size());
        assertEquals(0, testMapStore.getStore().size());
        h1.getLifecycleService().shutdown();
        assertEquals(100, testMapStore.getStore().size());
        assertEquals(1, testMapStore.getDestroyCount());
    }

    @Test
    public void testOneMemberFlush() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 200);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        assertEquals(100, map.size());
        assertEquals(0, testMapStore.getStore().size());
        assertEquals(100, map.getLocalMapStats().getDirtyEntryCount());
        getConcurrentMapManager(h1).flush(Prefix.MAP + "default");
        assertEquals(100, testMapStore.getStore().size());
        assertEquals(0, map.getLocalMapStats().getDirtyEntryCount());
        assertEquals(100, map.size());
    }

    @Test
    public void testOneMemberWriteBehind2() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 1);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL_KEYS, testMapStore.waitForEvent(2));
        assertEquals(0, map.size());
        map.put("1", "value1");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(2));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE, testMapStore.waitForEvent(2));
        assertEquals(1, map.size());
        assertEquals(1, testMapStore.getStore().size());
        map.remove("1");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.DELETE, testMapStore.waitForEvent(5));
        assertEquals(0, map.size());
        assertEquals(0, testMapStore.getStore().size());
    }

    @Test
    public void testOneMemberWriteBehind() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        testMapStore.insert("1", "value1");
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        assertEquals("value1", map.get("1"));
        assertEquals("value1", map.put("1", "value2"));
        assertEquals("value2", map.get("1"));
        // store should have the old data as we will write-behind
        assertEquals("value1", testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        assertTrue(map.evict("1"));
        assertEquals("value2", testMapStore.getStore().get("1"));
        assertEquals(0, map.size());
        assertEquals(1, testMapStore.getStore().size());
        assertEquals("value2", map.get("1"));
        assertEquals(1, map.size());
        map.remove("1");
        // store should have the old data as we will delete-behind
        assertEquals(1, testMapStore.getStore().size());
        assertEquals(0, map.size());
        testMapStore.assertAwait(12);
        assertEquals(0, testMapStore.getStore().size());
    }

    @Test
    public void testWriteBehindBackupLoaded() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 20);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        testMapStore.insert("1", "value1");
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        map.lock("1");
        assertEquals("value1", map.get("1"));
        assertEquals(1, map.size());
        Data key1 = toData("1");
        int partitionId = getConcurrentMapManager(h1).getPartitionId(key1);
        PartitionInfo p1 = getConcurrentMapManager(h1).getPartitionInfo(partitionId);
        PartitionInfo p2 = getConcurrentMapManager(h2).getPartitionInfo(partitionId);
        assertEquals(p1, p2);
        CMap cmap1 = getCMap(h1, "default");
        Record rec1 = cmap1.getRecord(key1);
        CMap cmap2 = getCMap(h2, "default");
        Record rec2 = cmap2.getRecord(key1);
        assertNotNull(rec1.getValueData());
        assertNotNull(rec2.getValueData());
        assertEquals(rec1.getLock(), rec2.getLock());
        h1.getLifecycleService().shutdown();
        IMap map2 = h2.getMap("default");
        assertEquals(1, map2.size());
        map2.putTransient("2", "value2", 100 * 24 * 6000, TimeUnit.SECONDS);
        CMap cmap = getCMap(h2, "default");
        Data key = toData("2");
        Record record = cmap.getRecord(key);
        assertFalse(record.isDirty());
        assertTrue(record.isValid());
        assertTrue(record.isActive());
        assertEquals("value2", record.getValue());
        map2.put("2", "value22");
        assertTrue(record.isDirty());
        assertTrue(record.isValid());
        assertTrue(record.isActive());
        map2.putTransient("2", "value222", 100 * 24 * 6000, TimeUnit.SECONDS);
        assertTrue(record.isDirty());
        assertTrue(record.isValid());
        assertTrue(record.isActive());
    }

    @Test
    public void testOneMemberWriteBehindWithEvictions() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Config config = newConfig(testMapStore, 2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL_KEYS, testMapStore.waitForEvent(20));
        for (int i = 0; i < 100; i++) {
            map.put(i, "value" + i);
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(10));
        }
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE_ALL, testMapStore.waitForEvent(20));
        assertEquals(100, testMapStore.getStore().size());
        for (int i = 0; i < 100; i++) {
            map.evict(i);
        }
        // we should not receive any store event.
        assertEquals(null, testMapStore.waitForEvent(10));
        assertEquals(100, testMapStore.getStore().size());
        for (int i = 0; i < 100; i++) {
            map.put(i, "value" + i);
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(10));
        }
        for (int i = 0; i < 100; i++) {
            map.evict(i);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE, testMapStore.waitForEvent(10));
        }
        assertEquals(null, testMapStore.waitForEvent(10));
        assertEquals(100, testMapStore.getStore().size());
        assertEquals(0, map.size());
        for (int i = 0; i < 100; i++) {
            map.put(i, "value" + i);
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(10));
        }
        for (int i = 0; i < 100; i++) {
            map.remove(i);
        }
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.DELETE_ALL, testMapStore.waitForEvent(20));
        assertEquals(0, testMapStore.getStore().size());
        assertEquals(0, map.size());
        assertEquals(null, testMapStore.waitForEvent(10));
    }

    @Test
    public void testOneMemberWriteBehindWithMaxIdle() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Config config = newConfig(testMapStore, 1);
        config.getMapConfig("default").setMaxIdleSeconds(4);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL_KEYS, testMapStore.waitForEvent(20));
        for (int i = 0; i < 10; i++) {
            map.put(i, "value" + i);
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(10));
        }
        CMap cmap = getCMap(h1, "default");
        cmap.startCleanup(true);
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE_ALL, testMapStore.waitForEvent(10));
        Thread.sleep(5000);
        cmap.startCleanup(true);
        assertEquals(null, testMapStore.waitForEvent(1));
        assertEquals(10, testMapStore.getStore().size());
        assertEquals(0, map.size());
        cmap.startCleanup(true);
        assertEquals(null, testMapStore.waitForEvent(10));
    }

    @Test
    public void issue587CallMapLoaderDuringRemoval() {
        final AtomicInteger loadCount = new AtomicInteger(0);
        final AtomicInteger storeCount = new AtomicInteger(0);
        final AtomicInteger deleteCount = new AtomicInteger(0);
        class SimpleMapStore2<K, V> extends SimpleMapStore<K, V>  {

            SimpleMapStore2(ConcurrentMap<K, V> store) {
                super(store);
            }

            public V load(K key) {
                loadCount.incrementAndGet();
                return super.load(key);
            }

            public void store(K key, V value) {
                storeCount.incrementAndGet();
                super.store(key, value);
            }

            public void delete(K key) {
                deleteCount.incrementAndGet();
                super.delete(key);
            }
        }
        final ConcurrentMap<String, Long> store = new ConcurrentHashMap<String, Long>();
        final MapStore<String, Long> myMapStore = new SimpleMapStore2<String, Long>(store);
        Config config = new Config();
        config
                .getMapConfig("myMap")
                .setMapStoreConfig(new MapStoreConfig()
                                           //.setWriteDelaySeconds(1)
                                           .setImplementation(myMapStore));
        HazelcastInstance hc = Hazelcast.newHazelcastInstance(config);
        store.put("one", 1l);
        store.put("two", 2l);
        assertEquals(0, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        IMap<String, Long> myMap = hc.getMap("myMap");
        assertEquals(1l, myMap.get("one").longValue());
        assertEquals(2l, myMap.get("two").longValue());
        assertEquals(2, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        assertNull(myMap.remove("ten"));
        assertEquals(3, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        myMap.put("three", 3L);
        myMap.put("four", 4L);
        assertEquals(5, loadCount.get());
        assertEquals(2, storeCount.get());
        assertEquals(0, deleteCount.get());
        myMap.remove("one");
        assertEquals(2, storeCount.get());
        assertEquals(1, deleteCount.get());
        assertEquals(5, loadCount.get());
    }

    @Test
    public void storedQueueWithDelaySecondsActionPollAndTake() throws InterruptedException {
        final ConcurrentMap<Long, String> STORE =
                new ConcurrentHashMap<Long, String>();
        STORE.put(1l, "Event1");
        STORE.put(2l, "Event2");
        STORE.put(3l, "Event3");
        STORE.put(4l, "Event4");
        STORE.put(5l, "Event5");
        STORE.put(6l, "Event6");
        Config config = new Config();
        config.getMapConfig("queue-map")
                .setMapStoreConfig(new MapStoreConfig()
                        .setWriteDelaySeconds(1)
                        .setImplementation(new SimpleMapStore<Long, String>(STORE)));
        config.getQueueConfig("tasks").setBackingMapRef("queue-map");
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        IQueue q = h.getQueue("tasks");
        assertEquals(STORE.size(), q.size());
//        assertEquals(STORE.get(1l), q.poll());
        assertEquals(STORE.get(1l), q.take());
        assertEquals(STORE.get(2l), q.take());
    }

    @Test
    public void testIssue583MapReplaceShouldTriggerMapStore() {
        final ConcurrentMap<String, Long> store = new ConcurrentHashMap<String, Long>();
        final MapStore<String, Long> myMapStore = new SimpleMapStore<String, Long>(store);
        Config config = new Config();
        config
                .getMapConfig("myMap")
                .setMapStoreConfig(new MapStoreConfig()
                                           .setImplementation(myMapStore));
        HazelcastInstance hc = Hazelcast.newHazelcastInstance(config);
        IMap<String, Long> myMap = hc.getMap("myMap");
        myMap.put("one", 1L);
        assertEquals(1L, myMap.get("one").longValue());
        assertEquals(1L, store.get("one").longValue());
        myMap.putIfAbsent("two", 2L);
        assertEquals(2L, myMap.get("two").longValue());
        assertEquals(2L, store.get("two").longValue());
        myMap.putIfAbsent("one", 5L);
        assertEquals(1L, myMap.get("one").longValue());
        assertEquals(1L, store.get("one").longValue());
        myMap.replace("one", 1L, 111L);
        assertEquals(111L, myMap.get("one").longValue());
        assertEquals(111L, store.get("one").longValue());
        myMap.replace("one", 1L);
        assertEquals(1L, myMap.get("one").longValue());
        assertEquals(1L, store.get("one").longValue());
    }

    @Test
    public void issue614() {
        final ConcurrentMap<Long, String> STORE =
                new ConcurrentHashMap<Long, String>();
        STORE.put(1l, "Event1");
        STORE.put(2l, "Event2");
        STORE.put(3l, "Event3");
        STORE.put(4l, "Event4");
        STORE.put(5l, "Event5");
        STORE.put(6l, "Event6");
        Config config = new Config();
        config
                .getMapConfig("map")
                .setMapStoreConfig(new MapStoreConfig()
                                           .setWriteDelaySeconds(1)
                                           .setImplementation(new SimpleMapStore<Long, String>(STORE)));
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        IMap map = h.getMap("map");
        Collection collection = map.values();
        LocalMapStats localMapStats = map.getLocalMapStats();
        assertEquals(0, localMapStats.getDirtyEntryCount());
    }

    protected Config newConfig(Object storeImpl, int writeDelaySeconds) {
        return newConfig("default", storeImpl, writeDelaySeconds);
    }

    protected Config newConfig(String mapName, Object storeImpl, int writeDelaySeconds) {
        Config config = new XmlConfigBuilder().build();
        MapConfig mapConfig = config.getMapConfig(mapName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(storeImpl);
        mapStoreConfig.setWriteDelaySeconds(writeDelaySeconds);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }

    public static class MapStoreAdaptor<K, V> implements MapStore<K, V> {

        public void delete(final K key) {
        }

        public void store(final K key, final V value) {
        }

        public void storeAll(final Map<K, V> map) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                store(entry.getKey(), entry.getValue());
            }
        }

        public void deleteAll(final Collection<K> keys) {
            for (K key : keys) {
                delete(key);
            }
        }

        public V load(final K key) {
            return null;
        }

        public Map<K, V> loadAll(final Collection<K> keys) {
            Map<K, V> result = new HashMap<K, V>();
            for (K key : keys) {
                V value = load(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }

        public Set<K> loadAllKeys() {
            return null;
        }
    }

    public static class SimpleMapStore<K, V> extends MapStoreAdaptor<K, V> {
        final Map<K, V> store ;
        private boolean loadAllKeys = true;

        public SimpleMapStore() {
            store = new ConcurrentHashMap<K, V>();
        }

        public SimpleMapStore(final Map<K, V> store) {
            this.store = store;
        }

        @Override
        public void delete(final K key) {
            store.remove(key);
        }

        @Override
        public V load(final K key) {
            return store.get(key);
        }

        @Override
        public void store(final K key, final V value) {
            store.put(key, value);
        }

        public Set<K> loadAllKeys() {
            if (loadAllKeys) {
                return store.keySet();
            }
            return null;
        }

        public void setLoadAllKeys(boolean loadAllKeys) {
            this.loadAllKeys = loadAllKeys;
        }

        @Override
        public void storeAll(final Map<K, V> kvMap) {
            store.putAll(kvMap);
        }
    }

    public static class TestMapStore extends MapStoreAdaptor implements MapLoaderLifecycleSupport, MapStore {

        final Map store = new ConcurrentHashMap();

        final CountDownLatch latchStore;
        final CountDownLatch latchStoreAll;
        final CountDownLatch latchDelete;
        final CountDownLatch latchDeleteAll;
        final CountDownLatch latchLoad;
        final CountDownLatch latchLoadAllKeys;
        final CountDownLatch latchLoadAll;
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicInteger initCount = new AtomicInteger();
        final AtomicInteger destroyCount = new AtomicInteger();
        private HazelcastInstance hazelcastInstance;
        private Properties properties;
        private String mapName;
        private boolean loadAllKeys = true;

        public TestMapStore() {
            this(0, 0, 0, 0, 0, 0);
        }

        public TestMapStore(int expectedStore, int expectedDelete, int expectedLoad) {
            this(expectedStore, 0, expectedDelete, 0, expectedLoad, 0);
        }

        public TestMapStore(int expectedStore, int expectedStoreAll, int expectedDelete,
                            int expectedDeleteAll, int expectedLoad, int expectedLoadAll) {
            this(expectedStore, expectedStoreAll, expectedDelete, expectedDeleteAll,
                 expectedLoad, expectedLoadAll, 0);
        }

        public TestMapStore(int expectedStore, int expectedStoreAll, int expectedDelete,
                            int expectedDeleteAll, int expectedLoad, int expectedLoadAll,
                            int expectedLoadAllKeys) {
            latchStore = new CountDownLatch(expectedStore);
            latchStoreAll = new CountDownLatch(expectedStoreAll);
            latchDelete = new CountDownLatch(expectedDelete);
            latchDeleteAll = new CountDownLatch(expectedDeleteAll);
            latchLoad = new CountDownLatch(expectedLoad);
            latchLoadAll = new CountDownLatch(expectedLoadAll);
            latchLoadAllKeys = new CountDownLatch(expectedLoadAllKeys);
        }

        public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
            this.hazelcastInstance = hazelcastInstance;
            this.properties = properties;
            this.mapName = mapName;
            initCount.incrementAndGet();
        }

        public boolean isLoadAllKeys() {
            return loadAllKeys;
        }

        public void setLoadAllKeys(boolean loadAllKeys) {
            this.loadAllKeys = loadAllKeys;
        }

        public void destroy() {
            destroyCount.incrementAndGet();
        }

        public int getInitCount() {
            return initCount.get();
        }

        public int getDestroyCount() {
            return destroyCount.get();
        }

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }

        public String getMapName() {
            return mapName;
        }

        public Properties getProperties() {
            return properties;
        }

        public void assertAwait(int seconds) throws InterruptedException {
            assertTrue("Store remaining: " + latchStore.getCount(), latchStore.await(seconds, TimeUnit.SECONDS));
            assertTrue("Store-all remaining: " + latchStoreAll.getCount(), latchStoreAll.await(seconds, TimeUnit.SECONDS));
            assertTrue("Delete remaining: " + latchDelete.getCount(), latchDelete.await(seconds, TimeUnit.SECONDS));
            assertTrue("Delete-all remaining: " + latchDeleteAll.getCount(), latchDeleteAll.await(seconds, TimeUnit.SECONDS));
            assertTrue("Load remaining: " + latchLoad.getCount(), latchLoad.await(seconds, TimeUnit.SECONDS));
            assertTrue("Load-al remaining: " + latchLoadAll.getCount(), latchLoadAll.await(seconds, TimeUnit.SECONDS));
        }

        Map getStore() {
            return store;
        }

        public void insert(Object key, Object value) {
            store.put(key, value);
        }

        public void store(Object key, Object value) {
            store.put(key, value);
            callCount.incrementAndGet();
            latchStore.countDown();
        }

        public Set loadAllKeys() {
            callCount.incrementAndGet();
            latchLoadAllKeys.countDown();
            if (!loadAllKeys) return null;
            return store.keySet();
        }

        public Object load(Object key) {
            callCount.incrementAndGet();
            latchLoad.countDown();
            return store.get(key);
        }

        public void storeAll(Map map) {
            store.putAll(map);
            callCount.incrementAndGet();
            latchStoreAll.countDown();
        }

        public void delete(Object key) {
            store.remove(key);
            callCount.incrementAndGet();
            latchDelete.countDown();
        }

        public Map loadAll(Collection keys) {
            Map map = new HashMap(keys.size());
            for (Object key : keys) {
                Object value = store.get(key);
                if (value != null) {
                    map.put(key, value);
                }
            }
            callCount.incrementAndGet();
            latchLoadAll.countDown();
            return map;
        }

        public void deleteAll(Collection keys) {
            for (Object key : keys) {
                store.remove(key);
            }
            callCount.incrementAndGet();
            latchDeleteAll.countDown();
        }
    }

    public static class TestEventBasedMapStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {

        protected enum STORE_EVENTS {
            STORE, STORE_ALL, DELETE, DELETE_ALL, LOAD, LOAD_ALL, LOAD_ALL_KEYS
        }

        protected final Map<K, V> store = new ConcurrentHashMap();
        protected final BlockingQueue events = new LinkedBlockingQueue();
        protected final AtomicInteger callCount = new AtomicInteger();
        protected final AtomicInteger initCount = new AtomicInteger();
        protected HazelcastInstance hazelcastInstance;
        protected Properties properties;
        protected String mapName;
        protected boolean loadAllKeys = true;

        public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
            this.hazelcastInstance = hazelcastInstance;
            this.properties = properties;
            this.mapName = mapName;
            initCount.incrementAndGet();
        }

        public void destroy() {
        }

        public int getEventCount() {
            return events.size();
        }

        public int getInitCount() {
            return initCount.get();
        }

        public boolean isLoadAllKeys() {
            return loadAllKeys;
        }

        public void setLoadAllKeys(boolean loadAllKeys) {
            this.loadAllKeys = loadAllKeys;
        }

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }

        public String getMapName() {
            return mapName;
        }

        public Properties getProperties() {
            return properties;
        }

        Object waitForEvent(int seconds) throws InterruptedException {
            return events.poll(seconds, TimeUnit.SECONDS);
        }

        Map getStore() {
            return store;
        }

        public void insert(K key, V value) {
            store.put(key, value);
        }

        public void store(K key, V value) {
            store.put(key, value);
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.STORE);
        }

        public V load(K key) {
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.LOAD);
            return store.get(key);
        }

        public void storeAll(Map map) {
            store.putAll(map);
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.STORE_ALL);
        }

        public void delete(K key) {
            store.remove(key);
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.DELETE);
        }

        public Set<K> loadAllKeys() {
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.LOAD_ALL_KEYS);
            if (!loadAllKeys) return null;
            return store.keySet();
        }

        public Map loadAll(Collection keys) {
            Map map = new HashMap(keys.size());
            for (Object key : keys) {
                Object value = store.get(key);
                if (value != null) {
                    map.put(key, value);
                }
            }
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.LOAD_ALL);
            return map;
        }

        public void deleteAll(Collection keys) {
            for (Object key : keys) {
                store.remove(key);
            }
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.DELETE_ALL);
        }
    }

    public static class FailAwareMapStore implements MapStore {

        final Map db = new ConcurrentHashMap();

        final AtomicLong deletes = new AtomicLong();
        final AtomicLong deleteAlls = new AtomicLong();
        final AtomicLong stores = new AtomicLong();
        final AtomicLong storeAlls = new AtomicLong();
        final AtomicLong loads = new AtomicLong();
        final AtomicLong loadAlls = new AtomicLong();
        final AtomicLong loadAllKeys = new AtomicLong();
        final AtomicBoolean shouldFail = new AtomicBoolean(false);
        final List<BlockingQueue> listeners = new CopyOnWriteArrayList<BlockingQueue>();

        public void addListener(BlockingQueue obj) {
            listeners.add(obj);
        }

        public void notifyListeners() {
            for (BlockingQueue listener : listeners) {
                listener.offer(new Object());
            }
        }

        public void delete(Object key) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    db.remove(key);
                }
            } finally {
                deletes.incrementAndGet();
                notifyListeners();
            }
        }

        public void setFail(boolean shouldFail) {
            this.shouldFail.set(shouldFail);
        }

        public int dbSize() {
            return db.size();
        }

        public boolean dbContainsKey(Object key) {
            return db.containsKey(key);
        }

        public Object dbGet(Object key) {
            return db.get(key);
        }

        public void store(Object key, Object value) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    db.put(key, value);
                }
            } finally {
                stores.incrementAndGet();
                notifyListeners();
            }
        }

        public Set loadAllKeys() {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    return db.keySet();
                }
            } finally {
                loadAllKeys.incrementAndGet();
            }
        }

        public Object load(Object key) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    return db.get(key);
                }
            } finally {
                loads.incrementAndGet();
            }
        }

        public void storeAll(Map map) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    db.putAll(map);
                }
            } finally {
                storeAlls.incrementAndGet();
                notifyListeners();
            }
        }

        public Map loadAll(Collection keys) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    Map results = new HashMap();
                    for (Object key : keys) {
                        Object value = db.get(key);
                        if (value != null) {
                            results.put(key, value);
                        }
                    }
                    return results;
                }
            } finally {
                loadAlls.incrementAndGet();
                notifyListeners();
            }
        }

        public void deleteAll(Collection keys) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    for (Object key : keys) {
                        db.remove(key);
                    }
                }
            } finally {
                deleteAlls.incrementAndGet();
                notifyListeners();
            }
        }
    }

    @Test
    public void testMapLoaderInitialization() {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig("testMapLoader-*");
        MapStoreConfig msConfig = new MapStoreConfig();
        mapConfig.setMapStoreConfig(msConfig);
        msConfig.setEnabled(true);
        Config configSuper = new Config();
        configSuper.setLiteMember(true);
        configSuper.addMapConfig(mapConfig);
        final int initialKeys = 5;
        msConfig.setImplementation(new MapLoader() {
            public Object load(Object key) {
                return "Value: " + key;
            }

            public Map loadAll(Collection keys) {
                Map map = new HashMap(keys.size());
                for (Object key : keys) {
                    map.put(key, load(key));
                }
                return map;
            }

            public Set loadAllKeys() {
                Set keys = new HashSet(3);
                for (int i = 0; i < initialKeys; i++) {
                    keys.add(i);
                }
                return keys;
            }
        });
        final HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance superClient = Hazelcast.newHazelcastInstance(configSuper);
        Hazelcast.newHazelcastInstance(config);
        assertEquals(initialKeys, member.getMap("testMapLoader-1").size());
        assertEquals(initialKeys, superClient.getMap("testMapLoader-1").size());
        assertEquals(initialKeys, superClient.getMap("testMapLoader-2").size());
        assertEquals(initialKeys, member.getMap("testMapLoader-2").size());
        Hazelcast.shutdownAll();
    }

    @Test
    /**
     * Issue 816.
     */
    public void testMapRemoveWithWriteBehindMapStore() {
        testMapRemoveWithMapStore(100);
    }

    @Test
    /**
     * Issue 816.
     */
    public void testMapRemoveWithWriteThroughMapStore() {
        testMapRemoveWithMapStore(0);
    }

    private void testMapRemoveWithMapStore(int delay) {
        SimpleMapStore<Integer, String> mapStore = new SimpleMapStore<Integer, String>(
                new ConcurrentHashMap<Integer, String>());
        Config c = new Config();
        c.getMapConfig("test").setMapStoreConfig(new MapStoreConfig().setEnabled(true)
                .setWriteDelaySeconds(delay).setImplementation(mapStore));
        for (int i = 1; i < 6; i++) {
            mapStore.store(i, "value" + i);
        }
        mapStore.setLoadAllKeys(false);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);
        IMap map = hz.getMap("test");

        assertEquals("value1", map.get(1));
        assertEquals("value1", map.remove(1));
        assertNull("get should be null!", map.get(1));

        assertEquals("value2", map.get(2));
        assertEquals("value2", map.remove(2));
        assertFalse("containsKey should be false!", map.containsKey(2));

        assertEquals("value3", map.get(3));
        assertEquals("value3", map.remove(3));
        assertNull("put should be null!", map.put(3, "valuex"));

        assertEquals("value4", map.get(4));
        assertEquals("value4", map.remove(4));
        assertNull("remove should be null!", map.remove(4));

        assertEquals("value5", map.get(5));
        assertEquals("value5", map.remove(5));
        assertNull("putIfAbsent should be null!", map.putIfAbsent(5, "valuex"));
    }

    @Test
    /**
     * Issue 816.
     */
    public void testMapEvictWithWriteBehindMapStore() {
        testMapEvictWithMapStore(100);
    }

    @Test
    /**
     * Issue 816.
     */
    public void testMapEvictWithWriteThroughMapStore() {
        testMapEvictWithMapStore(0);
    }

    private void testMapEvictWithMapStore(int delay) {
        SimpleMapStore<Integer, String> mapStore = new SimpleMapStore<Integer, String>(
                new ConcurrentHashMap<Integer, String>());
        Config c = new Config();
        c.getMapConfig("test").setMapStoreConfig(new MapStoreConfig().setEnabled(true)
                .setWriteDelaySeconds(delay).setImplementation(mapStore));
        for (int i = 1; i < 6; i++) {
            mapStore.store(i, "value" + i);
        }
        mapStore.setLoadAllKeys(false);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);
        IMap map = hz.getMap("test");

        assertEquals("value1", map.get(1));
        assertTrue("Evict 1", map.evict(1));
        assertEquals("value1", map.get(1));

        assertEquals("value2", map.get(2));
        assertTrue("Evict 2", map.evict(2));
        assertTrue("containsKey should be true!", map.containsKey(2));

        assertEquals("value3", map.get(3));
        assertTrue("Evict 3", map.evict(3));
        assertEquals("value3", map.put(3, "valuex"));

        assertEquals("value4", map.get(4));
        assertTrue("Evict 4", map.evict(4));
        assertEquals("value4", map.remove(4));

        assertEquals("value5", map.get(5));
        assertTrue("Evict 5", map.evict(5));
        assertEquals("value5", map.putIfAbsent(5, "valuex"));
    }

    /**
     * test for issue #96
     */
    @Test
    public void testRemoveExpiredEntryWithWriteBehindMapStore() throws InterruptedException {
        TestMapStore mapStore = new TestMapStore(1, 1, 0);
        Config c = new Config();
        c.setProperty(GroupProperties.PROP_CLEANUP_DELAY_SECONDS, "1");
        c.getMapConfig("test").setMapStoreConfig(new MapStoreConfig().setEnabled(true)
                .setWriteDelaySeconds(5).setImplementation(mapStore));
        mapStore.setLoadAllKeys(false);
        c.getMapConfig("test").setTimeToLiveSeconds(1);

        final CountDownLatch latch = new CountingCountdownLatch(2);
        IMap map = Hazelcast.newHazelcastInstance(c).getMap("test");
        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(final EntryEvent entryEvent) {
                if (entryEvent.getKey().equals(2)) {
                    latch.countDown();
                } else {
                    fail("Should not evict: " + entryEvent);
                }
            }

            public void entryRemoved(final EntryEvent entryEvent) {
                if (entryEvent.getKey().equals(1)) {
                    latch.countDown();
                } else {
                    fail("Should not remove: " + entryEvent);
                }
            }
        }, true);

        map.put(1, "value");
        map.put(2, "value");
        Thread.sleep(1500);
        assertEquals("value", map.remove(1));
        assertTrue("EntryListener failed!", latch.await(10, TimeUnit.SECONDS));
        mapStore.assertAwait(10);
    }

    /**
     * test for issue #185
     */
    @Test
    public void testMapSetShouldNotLoadData() throws InterruptedException {
        final AtomicBoolean fail = new AtomicBoolean(false);
        SimpleMapStore mapStore = new SimpleMapStore() {
            public Object load(final Object key) {
                fail.set(true);
                fail("MapStore load should not be called!");
                return super.load(key);
            }
        };
        mapStore.setLoadAllKeys(false);
        Config c = new Config();
        c.getMapConfig("test").setMapStoreConfig(new MapStoreConfig().setEnabled(true).setImplementation(mapStore));
        final String key = "key";
        mapStore.store.put(key, "value");

        IMap map = Hazelcast.newHazelcastInstance(c).getMap("test");
        map.set(key, "value2", 0, TimeUnit.SECONDS);
        assertFalse("MapStore load should not be called!", fail.get());

    }
}
