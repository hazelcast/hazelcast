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

package com.hazelcast.core;

import com.hazelcast.util.Clock;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.util.ResponseQueueFactory;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class TransactionTest {

    @AfterClass
    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMapGetSimple() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapGetSimple");
        txnMap.put("1", "value");
        txnMap.begin();
        assertEquals("value", txnMap.get("1"));
        assertEquals("value", txnMap.get("1"));
        assertEquals("value", txnMap.get("1"));
        assertEquals(1, txnMap.size());
        txnMap.commit();
        assertEquals(1, txnMap.size());
    }

    @Test
    public void testMapPutSimple() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapPutSimple");
        txnMap.begin();
        txnMap.put("1", "value");
        assertEquals(1, txnMap.size());
        txnMap.commit();
        assertEquals(1, txnMap.size());
    }

    @Test
    public void testMapPutAndGetSimple() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapPutAndGetSimple");
        txnMap.put("1", "value");
        txnMap.begin();
        assertEquals("value", txnMap.get("1"));
        assertEquals("value", txnMap.get("1"));
        assertEquals("value", txnMap.put("1", "value2"));
        assertEquals("value2", txnMap.put("1", "value3"));
        assertEquals("value3", txnMap.get("1"));
        assertEquals(1, txnMap.size());
        txnMap.commit();
        assertEquals(1, txnMap.size());
    }


    @Test
    public void testMapContainsKey() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapContainsKey");
        txnMap.put("1", "value");
        Assert.assertTrue(txnMap.containsKey("1"));
        txnMap.begin();
        Assert.assertTrue(txnMap.containsKey("1"));
        assertEquals("value", txnMap.get("1"));
        Assert.assertTrue(txnMap.containsKey("1"));
        txnMap.commit();
        Assert.assertTrue(txnMap.containsKey("1"));
        assertEquals(1, txnMap.size());
    }

    @Test
    public void testMapContainsValue() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapContainsValue");
        txnMap.put("1", "value");
        txnMap.put("2", "value");
        Assert.assertTrue(txnMap.containsValue("value"));
        txnMap.begin();
        Assert.assertTrue(txnMap.containsValue("value"));
        txnMap.commit();
        assertEquals(2, txnMap.size());
    }

    @Test
    public void testMultiMapContainsEntry() {
        TransactionalMultiMap txnMap = newTransactionalMultiMapProxy("testMultiMapContainsEntry");
        txnMap.put("1", "value");
        Assert.assertTrue(txnMap.containsEntry("1", "value"));
        txnMap.begin();
        txnMap.put("1", "value2");
        Assert.assertTrue(txnMap.containsEntry("1", "value"));
        Assert.assertTrue(txnMap.containsEntry("1", "value2"));
        txnMap.remove("1", "value2");
        Assert.assertTrue(txnMap.containsEntry("1", "value"));
        Assert.assertFalse(txnMap.containsEntry("1", "value2"));
        txnMap.commit();
        Assert.assertTrue(txnMap.containsEntry("1", "value"));
        assertEquals(1, txnMap.size());
    }

    @Test
    public void testMapIterateEntries() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapIterateEntries");
        txnMap.put("1", "value1");
        assertEquals(1, txnMap.size());
        txnMap.begin();
        txnMap.put("2", "value2");
        assertEquals(2, txnMap.size());
        Set<Map.Entry> entries = txnMap.entrySet();
        for (Map.Entry entry : entries) {
            if ("1".equals(entry.getKey())) {
                assertEquals("value1", entry.getValue());
            } else if ("2".equals(entry.getKey())) {
                assertEquals("value2", entry.getValue());
            } else throw new RuntimeException("cannot contain another entry with key " + entry.getKey());
        }
        txnMap.commit();
        assertEquals(2, txnMap.size());
    }

    @Test
    public void testMapIterateEntries2() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapIterateEntries2");
        assertEquals(0, txnMap.size());
        txnMap.begin();
        txnMap.put("1", "value1");
        txnMap.put("2", "value2");
        assertEquals(2, txnMap.size());
        Set<Map.Entry> entries = txnMap.entrySet();
        for (Map.Entry entry : entries) {
            if ("1".equals(entry.getKey())) {
                assertEquals("value1", entry.getValue());
            } else if ("2".equals(entry.getKey())) {
                assertEquals("value2", entry.getValue());
            } else throw new RuntimeException("cannot contain another entry with key " + entry.getKey());
        }
        txnMap.commit();
        assertEquals(2, txnMap.size());
    }

    @Test
    public void testMapIterateEntries3() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapIterateEntries3");
        txnMap.put("1", "value1");
        assertEquals(1, txnMap.size());
        txnMap.begin();
        txnMap.put("1", "value2");
        assertEquals(1, txnMap.size());
        Set<Map.Entry> entries = txnMap.entrySet();
        for (Map.Entry entry : entries) {
            if ("1".equals(entry.getKey())) {
                assertEquals("value2", entry.getValue());
            } else throw new RuntimeException("cannot contain another entry with key " + entry.getKey());
        }
        txnMap.rollback();
        assertEquals(1, txnMap.size());
        entries = txnMap.entrySet();
        for (Map.Entry entry : entries) {
            if ("1".equals(entry.getKey())) {
                assertEquals("value1", entry.getValue());
            } else throw new RuntimeException("cannot contain another entry with key " + entry.getKey());
        }
    }

    @Test
    public void testMapPutCommitSize() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapPutCommitSize");
        IMap imap = newMapProxy("testMapPutCommitSize");
        txnMap.put("1", "item");
        assertEquals(1, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.begin();
        txnMap.put(2, "newone");
        assertEquals(2, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.commit();
        assertEquals(2, txnMap.size());
        assertEquals(2, imap.size());
    }

    @Test
    public void testMapPutRollbackSize() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapPutRollbackSize");
        IMap imap = newMapProxy("testMapPutRollbackSize");
        txnMap.put("1", "item");
        assertEquals(1, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.begin();
        txnMap.put(2, "newone");
        assertEquals(2, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.rollback();
        assertEquals(1, txnMap.size());
        assertEquals(1, imap.size());
        assertNull(imap.getMapEntry(2));
        assertNull(imap.get(2));
    }

    @Test
    public void testSetCommit() {
        TransactionalSet set = newTransactionalSetProxy("testSetCommit");
        set.begin();
        set.add("item");
        assertEquals(1, set.size());
        set.commit();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetCommitTwoSameItems() {
        TransactionalSet set = newTransactionalSetProxy("testSetCommitTwoSameItems");
        set.begin();
        set.add("item");
        set.add("item");
        assertEquals(1, set.size());
        set.commit();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetRollback() {
        TransactionalSet set = newTransactionalSetProxy("testSetRollback");
        set.begin();
        set.add("item");
        set.rollback();
        assertEquals(0, set.size());
    }

    @Test
    public void testListCommit() {
        TransactionalList list = newTransactionalListProxy("testListCommit");
        list.begin();
        list.add("item");
        assertEquals(1, list.size());
        list.commit();
        assertEquals(1, list.size());
    }

    @Test
    public void testListCommitTwoSameItems() {
        TransactionalList list = newTransactionalListProxy("testListCommitTwoSameItems");
        list.begin();
        list.add("item");
        list.add("item");
        assertEquals(2, list.size());
        list.commit();
        assertEquals(2, list.size());
    }

    @Test
    public void testListRollback() {
        TransactionalList list = newTransactionalListProxy("testListRollback");
        list.begin();
        list.add("item");
        list.rollback();
        assertEquals(0, list.size());
    }

    @Test
    public void testSetAddWithTwoTxn() {
        Hazelcast.getSet("testSetAddWithTwoTxn").add("1");
        Hazelcast.getSet("testSetAddWithTwoTxn").add("1");
        TransactionalSet set = newTransactionalSetProxy("testSetAddWithTwoTxn");
        TransactionalSet set2 = newTransactionalSetProxy("testSetAddWithTwoTxn");
        assertEquals(1, set.size());
        assertEquals(1, set2.size());
        set.begin();
        set.add("2");
        assertEquals(2, set.size());
        assertEquals(1, set2.size());
        set.commit();
        assertEquals(2, set.size());
        assertEquals(2, set2.size());
        set2.begin();
        assertEquals(2, set.size());
        assertEquals(2, set2.size());
        set2.remove("1");
        assertEquals(2, set.size());
        assertEquals(1, set2.size());
        set2.commit();
        assertEquals(1, set.size());
        assertEquals(1, set2.size());
    }

    @Test
    public void testMapPutWithTwoTxn() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapPutWithTwoTxn");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapPutWithTwoTxn");
        txnMap.begin();
        txnMap.put("1", "value");
        assertEquals("value", txnMap.get("1"));
        txnMap.put("1", "value1");
        assertEquals("value1", txnMap.get("1"));
        txnMap.commit();
        assertEquals("value1", txnMap.get("1"));
        txnMap2.begin();
        assertEquals("value1", txnMap2.put("1", "value2"));
        assertEquals("value2", txnMap2.get("1"));
        txnMap2.commit();
        assertEquals("value2", txnMap.get("1"));
        assertEquals("value2", txnMap2.get("1"));
    }
//    @Test
//    public void testMapGetOnlyWithTwoTxn() {
//        TransactionalMap txnMap = newTransactionalMapProxy("testMapGetOnlyWithTwoTxn");
//        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapGetOnlyWithTwoTxn");
//        txnMap.begin();
//        assertNull(txnMap.get("1"));
//        txnMap2.begin();
//        txnMap2.put("1", "value1");
//        assertEquals("value1", txnMap2.get("1"));
//        assertNull(txnMap.get("1"));
//        txnMap2.commit();
//        assertNull(txnMap.get("1"));
//        txnMap.commit();
//        assertEquals("value1", txnMap.get("1"));
//        assertEquals("value1", txnMap2.get("1"));
//    }

    /**
     * issue 455
     */
    @Test
    public void testPutIfAbsentInTxn() {
        IMap<String, String> aMap = Hazelcast.getMap("testPutIfAbsentInTransaction");
        Transaction txn = Hazelcast.getTransaction();
        txn.begin();
        try {
            aMap.put("key1", "value1");
            assertEquals("value1", aMap.putIfAbsent("key1", "value3"));
            aMap.put("key1", "value1");
            txn.commit();
        } catch (Throwable t) {
            txn.rollback();
        }
        assertEquals("value1", aMap.get("key1"));
        assertEquals("value1", aMap.putIfAbsent("key1", "value3"));
        assertEquals("value1", aMap.get("key1"));
        txn = Hazelcast.getTransaction();
        txn.begin();
        try {
            assertEquals("value1", aMap.get("key1"));
            assertEquals("value1", aMap.putIfAbsent("key1", "value2"));
            assertEquals("value1", aMap.get("key1"));
            txn.commit();
        } catch (Throwable t) {
            txn.rollback();
        }
        assertEquals("value1", aMap.get("key1"));
    }

    @Test
    public void testMapRemoveWithTwoTxn() {
        IMap map = Hazelcast.getMap("testMapRemoveWithTwoTxn");
        map.put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testMapRemoveWithTwoTxn");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapRemoveWithTwoTxn");
        txnMap.begin();
        assertEquals("value", txnMap.remove("1"));
        assertEquals(0, txnMap.size());
        assertEquals(1, map.size());
        assertFalse(txnMap.containsKey("1"));
        assertTrue(map.containsKey("1"));
        txnMap.commit();
        assertEquals(0, txnMap.size());
        assertEquals(0, map.size());
        assertFalse(txnMap.containsKey("1"));
        assertFalse(map.containsKey("1"));
        txnMap2.begin();
        txnMap2.remove("1");
        txnMap2.commit();
    }

    @Test
    public void testTryLock() {
        Hazelcast.getMap("testTryLock").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testTryLock");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testTryLock");
        txnMap.lock("1");
        long start = Clock.currentTimeMillis();
        assertFalse(txnMap2.tryLock("1", 2, TimeUnit.SECONDS));
        long end = Clock.currentTimeMillis();
        long took = (end - start);
        assertTrue((took > 1000) ? (took < 4000) : false);
        assertFalse(txnMap2.tryLock("1"));
        txnMap.unlock("1");
        assertTrue(txnMap2.tryLock("1", 2, TimeUnit.SECONDS));
    }

    @Test
    public void testReentrantLock() {
        Hazelcast.getMap("testReentrantLock").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testReentrantLock");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testReentrantLock");
        try {
            assertEquals("value", txnMap.tryLockAndGet("1", 5, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            fail();
        }
        txnMap.lock("1");
        assertFalse(txnMap2.tryLock("1"));
        txnMap.unlock("1");
        assertFalse(txnMap2.tryLock("1"));
        txnMap.unlock("1");
        assertTrue(txnMap2.tryLock("1"));
    }

    @Test(timeout = 100000)
    public void testTransactionCommitRespectLockCount() throws InterruptedException {
        Hazelcast.getMap("testTransactionCommitRespectLockCount").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testTransactionCommitRespectLockCount");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testTransactionCommitRespectLockCount");
        txnMap.lock(1);
        txnMap.begin();
        txnMap.put(1, "value");
        txnMap.commit();
        assertFalse("Shouldn't acquire lock", txnMap2.tryLock(1));
        txnMap.unlock(1);
        Assert.assertTrue(txnMap2.tryLock(1));
    }

    @Test
    public void testTryLock2() throws Exception {
        Hazelcast.getMap("testTryLock").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testTryLock");
        final TransactionalMap txnMap2 = newTransactionalMapProxy("testTryLock");
        txnMap.lock("1");
        Future f = txnMap2.async("tryLock", "1", 101, TimeUnit.MILLISECONDS);
        Thread.sleep(2100);
        txnMap.unlock("1");
        assertFalse((Boolean) f.get(3, TimeUnit.SECONDS));
    }

    @Test
    public void testMapRemoveAndLock() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapRemoveAndLock");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapRemoveAndLock");
        txnMap.put("Hello", "World");
        txnMap.remove("Hello");
        assertTrue(txnMap.tryLock("Hello"));
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
        }
        assertEquals(false, txnMap2.tryLock("Hello"));
    }

    @Test
    public void testTryPut2() {
        IMap map = Hazelcast.getMap("testTryPut");
        map.tryPut("1", "value", 5, TimeUnit.SECONDS);
    }

    @Test
    public void testTryPut() {
        Map map = Hazelcast.getMap("testTryPut");
        map.put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testTryPut");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testTryPut");
        txnMap.lock("1");
        long start = Clock.currentTimeMillis();
        assertFalse(txnMap2.tryPut("1", "value2", 2, TimeUnit.SECONDS));
        long end = Clock.currentTimeMillis();
        long took = (end - start);
        assertTrue((took > 1000) ? (took < 4000) : false);
        assertEquals("value", map.get("1"));
        assertEquals("value", txnMap.get("1"));
        assertEquals("value", txnMap2.get("1"));
        txnMap.unlock("1");
        assertTrue(txnMap2.tryPut("1", "value2", 2, TimeUnit.SECONDS));
        assertEquals("value2", map.get("1"));
        assertEquals("value2", txnMap.get("1"));
        assertEquals("value2", txnMap2.get("1"));
    }

    @Test
    public void testMapRemoveRollback() {
        Hazelcast.getMap("testMapRemoveRollback").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testMapRemoveRollback");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapRemoveRollback");
        txnMap.begin();
        assertEquals(1, txnMap.size());
        txnMap.remove("1");
        assertEquals(0, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap.rollback();
        assertEquals(1, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap2.begin();
        txnMap2.remove("1");
        txnMap2.commit();
        assertEquals(0, txnMap.size());
        assertEquals(0, txnMap2.size());
    }

    @Test
    public void testMapRemoveWithTwoTxn2() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapRemoveWithTwoTxn2");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapRemoveWithTwoTxn2");
        txnMap.begin();
        txnMap.remove("1");
        txnMap.commit();
        txnMap2.begin();
        txnMap2.remove("1");
        txnMap2.commit();
    }

    @Test
    public void testMapTryRemove() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapTryRemove");
        IMap txnMap2 = Hazelcast.getMap("testMapTryRemove");
        txnMap.put("1", "value1");
        txnMap.lock("1");
        assertEquals("value1", txnMap.get("1"));
        assertEquals("value1", txnMap2.get("1"));
        long start = Clock.currentTimeMillis();
        try {
            assertNull(txnMap2.tryRemove("1", 0, TimeUnit.SECONDS));
            fail("Shouldn't be able to remove");
        } catch (TimeoutException e) {
            assertTrue(Clock.currentTimeMillis() - start < 1000);
        }
        start = Clock.currentTimeMillis();
        try {
            assertNull(txnMap2.tryRemove("1", 3, TimeUnit.SECONDS));
            fail("Shouldn't be able to remove");
        } catch (TimeoutException e) {
            long took = (Clock.currentTimeMillis() - start);
            assertTrue(took >= 3000 && took < 5000);
        }
        txnMap.unlock("1");
        try {
            assertEquals("value1", txnMap2.tryRemove("1", 0, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            fail();
        }
        assertNull(txnMap.get("1"));
        assertNull(txnMap2.get("1"));
        assertEquals(0, txnMap.size());
        assertEquals(0, txnMap2.size());
    }

    @Test
    public void testMapTryLockAndGet() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapTryLockAndGet");
        IMap txnMap2 = Hazelcast.getMap("testMapTryLockAndGet");
        txnMap.put("1", "value1");
        try {
            assertEquals("value1", txnMap.tryLockAndGet("1", 0, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            fail();
        }
        assertEquals("value1", txnMap.get("1"));
        assertEquals("value1", txnMap2.get("1"));
        long start = Clock.currentTimeMillis();
        try {
            txnMap2.tryLockAndGet("1", 0, TimeUnit.SECONDS);
            fail("Shouldn't be able to lock");
        } catch (TimeoutException e) {
            assertTrue(Clock.currentTimeMillis() - start < 1000);
        }
        start = Clock.currentTimeMillis();
        try {
            assertNull(txnMap2.tryLockAndGet("1", 3, TimeUnit.SECONDS));
            fail("Shouldn't be able to lock");
        } catch (TimeoutException e) {
            long took = (Clock.currentTimeMillis() - start);
            assertTrue(took >= 3000 && took < 5000);
        }
        txnMap.putAndUnlock("1", "value2");
        try {
            assertEquals("value2", txnMap2.tryLockAndGet("1", 0, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            fail();
        }
        assertEquals("value2", txnMap.get("1"));
        assertEquals("value2", txnMap2.get("1"));
    }

    @Test
    public void testMapRemoveWithTwoTxn3() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapRemoveWithTwoTxn3");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapRemoveWithTwoTxn3");
        txnMap.put("1", "value1");
        assertEquals(1, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap.begin();
        txnMap.remove("1");
        assertEquals(0, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap.commit();
        assertEquals(0, txnMap.size());
        assertEquals(0, txnMap2.size());
        txnMap.put("1", "value1");
        assertEquals(1, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap2.begin();
        txnMap2.remove("1");
        assertEquals(1, txnMap.size());
        assertEquals(0, txnMap2.size());
        txnMap2.commit();
        assertEquals(0, txnMap.size());
        assertEquals(0, txnMap2.size());
    }

    @Test
    public void testMultiMapPutWithTwoTxn() {
        TransactionalMultiMap txnMMap = newTransactionalMultiMapProxy("testMultiMapPutWithTwoTxn");
        TransactionalMultiMap txnMMap2 = newTransactionalMultiMapProxy("testMultiMapPutWithTwoTxn");
        txnMMap.begin();
        assertTrue(txnMMap.put("1", "value1"));
        assertEquals(1, txnMMap.size());
        assertEquals(0, txnMMap2.size());
        txnMMap.commit();
        assertEquals(1, txnMMap.size());
        assertEquals(1, txnMMap2.size());
        txnMMap2.begin();
        assertTrue(txnMMap2.put("1", "value2"));
        assertEquals(1, txnMMap.size());
        assertEquals(2, txnMMap2.size());
        txnMMap2.commit();
        assertEquals(2, txnMMap.size());
        assertEquals(2, txnMMap2.size());
        assertEquals(2, txnMMap.valueCount("1"));
        assertEquals(2, txnMMap2.valueCount("1"));
    }

    @Test
    public void testMultiMapRemoveWithTwoTxn() {
        MultiMap mmap = Hazelcast.getMultiMap("testMultiMapRemoveWithTwoTxn");
        assertTrue(mmap.put("1", "value1"));
        assertTrue(mmap.put("1", "value2"));
        assertTrue(mmap.put("1", "value3"));
        TransactionalMultiMap txnMMap = newTransactionalMultiMapProxy("testMultiMapRemoveWithTwoTxn");
        TransactionalMultiMap txnMMap2 = newTransactionalMultiMapProxy("testMultiMapRemoveWithTwoTxn");
        assertEquals(3, mmap.size());
        txnMMap.begin();
        assertTrue(txnMMap.remove("1", "value2"));
        assertFalse(txnMMap.remove("1", "value5"));
        assertEquals(2, txnMMap.size());
        assertEquals(3, txnMMap2.size());
        txnMMap.commit();
        assertEquals(2, mmap.size());
        txnMMap2.begin();
        assertEquals(2, mmap.size());
        assertEquals(2, txnMMap2.size());
        Collection values = txnMMap2.remove("1");
        assertEquals(2, values.size());
        assertEquals(2, txnMMap.size());
        assertEquals(0, txnMMap2.size());
        txnMMap2.commit();
        assertEquals(0, mmap.size());
    }

    @Test
    public void testMultiMapPutRemoveWithTxn() {
        MultiMap multiMap = Hazelcast.getMultiMap("testMultiMapPutRemoveWithTxn");
        multiMap.put("1", "C");
        multiMap.put("2", "x");
        multiMap.put("2", "y");
        TransactionalMultiMap txnMap = newTransactionalMultiMapProxy("testMultiMapPutRemoveWithTxn");
        txnMap.begin();
        txnMap.put("1", "A");
        txnMap.put("1", "B");
        Collection g1 = txnMap.get("1");
        System.err.println(g1);
        assertTrue(g1.contains("A"));
        assertTrue(g1.contains("B"));
        assertTrue(g1.contains("C"));
        assertTrue(txnMap.remove("1", "C"));
        assertEquals(4, txnMap.size());
        Collection g2 = txnMap.get("1");
        assertTrue(g2.contains("A"));
        assertTrue(g2.contains("B"));
        assertFalse(g2.contains("C"));
        Collection r1 = txnMap.remove("2");
        assertTrue(r1.contains("x"));
        assertTrue(r1.contains("y"));
        assertNull(txnMap.get("2"));
        Collection r2 = txnMap.remove("1");
        assertEquals(2, r2.size());
        assertTrue(r2.contains("A"));
        assertTrue(r2.contains("B"));
        assertNull(txnMap.get("1"));
        txnMap.commit();
        assertEquals(0, txnMap.size());
    }

    @Test
    public void testQueueOfferCommitSize() {
        TransactionalQueue txnq = newTransactionalQueueProxy("testQueueOfferCommitSize");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("testQueueOfferCommitSize");
        txnq.begin();
        txnq.offer("item");
        assertEquals(1, txnq.size());
        assertEquals(0, txnq2.size());
        txnq.commit();
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
        assertEquals("item", txnq2.poll());
    }

    @Test
    public void testQueueOfferRollbackSize() {
        TransactionalQueue txnq = newTransactionalQueueProxy("testQueueOfferRollbackSize");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("testQueueOfferRollbackSize");
        txnq.begin();
        txnq.offer("item");
        assertEquals(1, txnq.size());
        assertEquals(0, txnq2.size());
        txnq.rollback();
        assertEquals(0, txnq.size());
        assertEquals(0, txnq2.size());
    }

    @Test
    public void testQueueOfferCommitIterator() {
        TransactionalQueue txnq = newTransactionalQueueProxy("testQueueOfferCommitIterator");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("testQueueOfferCommitIterator");
        assertEquals(0, txnq.size());
        assertEquals(0, txnq2.size());
        txnq.begin();
        txnq.offer("item");
        Iterator it = txnq.iterator();
        int size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(0, size);
        txnq.commit();
        it = txnq.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
    }

    @Test
    public void testQueueOfferCommitIterator2() {
        TransactionalQueue txnq = newTransactionalQueueProxy("testQueueOfferCommitIterator2");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("testQueueOfferCommitIterator2");
        txnq.offer("item0");
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
        txnq.begin();
        txnq.offer("item");
        Iterator it = txnq.iterator();
        int size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(2, size);
        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        txnq.commit();
        it = txnq.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(2, size);
        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(2, size);
        assertEquals(2, txnq.size());
        assertEquals(2, txnq2.size());
    }

    @Test
    public void testQueueOfferRollbackIterator2() {
        TransactionalQueue txnq = newTransactionalQueueProxy("testQueueOfferRollbackIterator2");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("testQueueOfferRollbackIterator2");
        txnq.offer("item0");
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
        txnq.begin();
        txnq.offer("item");
        Iterator it = txnq.iterator();
        int size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(2, size);
        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        txnq.rollback();
        it = txnq.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
    }

    @Test
    public void testQueuePollCommitSize() {
        TransactionalQueue txnq = newTransactionalQueueProxy("testQueuePollCommitSize");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("testQueuePollCommitSize");
        txnq.offer("item1");
        txnq.offer("item2");
        assertEquals(2, txnq.size());
        assertEquals(2, txnq2.size());
        txnq.begin();
        assertEquals("item1", txnq.poll());
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
        txnq.commit();
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
        assertEquals("item2", txnq2.poll());
        assertEquals(0, txnq.size());
        assertEquals(0, txnq2.size());
    }

    @Test
    public void testQueuePollRollbackSize() {
        TransactionalQueue txnq = newTransactionalQueueProxy("testQueuePollRollbackSize");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("testQueuePollRollbackSize");
        txnq.offer("item1");
        txnq.offer("item2");
        assertEquals(2, txnq.size());
        assertEquals(2, txnq2.size());
        txnq.begin();
        assertEquals("item1", txnq.poll());
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
        txnq.rollback();
        assertEquals(2, txnq.size());
        assertEquals(2, txnq2.size());
        assertEquals("item1", txnq2.poll());
        assertEquals("item2", txnq2.poll());
    }

    @Test
    public void testQueueOrderAfterPollRollback() {
        IQueue<Integer> queue = newQueueProxy("testQueueOrderAfterPollRollback");
        TransactionalQueue<Integer> txn1 = newTransactionalQueueProxy("testQueueOrderAfterPollRollback");
        txn1.begin();
        txn1.offer(1);
        txn1.offer(2);
        txn1.offer(3);
        txn1.commit();
        assertEquals(3, queue.size());
        assertEquals(3, txn1.size());
        TransactionalQueue<Integer> txn2 = newTransactionalQueueProxy("testQueueOrderAfterPollRollback");
        txn2.begin();
        assertEquals(1, txn2.poll().intValue());
        assertEquals(2, txn2.peek().intValue());
        txn2.rollback();
        assertEquals(1, queue.poll().intValue());
        assertEquals(2, queue.poll().intValue());
        assertEquals(3, queue.poll().intValue());
    }

    @Test
    @Ignore
    public void testMapEntryLastAccessTime() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapEntryLastAccessTime");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapEntryLastAccessTime");
        txnMap.put("1", "value1");
        MapEntry mapEntry = txnMap.getMapEntry("1");
        txnMap.begin();
        txnMap.get("1");
        mapEntry = txnMap.getMapEntry("1");
        System.out.println("txn test time2 " + mapEntry.getLastAccessTime());
        txnMap.commit();
    }

    @After
    public void cleanUp() {
        Iterator<Instance> it = mapsUsed.iterator();
        while (it.hasNext()) {
            Instance instance = it.next();
            instance.destroy();
        }
        mapsUsed.clear();
    }

    static class Thing implements Serializable {
        String property;

        Thing(String property) {
            this.property = property;
        }

        String getProperty() {
            return property;
        }
    }

    @Test
    public void testMapPutPredicate() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapPutPredicate");
        txnMap.begin();
        txnMap.put("1", new Thing("thing1"));
        txnMap.put("2", new Thing("thing2"));
        txnMap.put("3", new Thing("thing3"));
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.get("property").equal("thing2");
        assertEquals(1, txnMap.values(predicate).size());
        txnMap.commit();
        assertEquals(3, txnMap.size());
    }

    @Test
    public void issue581testRemoveInTwoTransactionOneShouldReturnNull() throws InterruptedException {
        final IMap m = Hazelcast.getMap("issue581testRemoveInTwoTransactionOneShouldReturnNull");
        final AtomicReference<Object> t1Return = new AtomicReference<Object>();
        final AtomicReference<Object> t2Return = new AtomicReference<Object>();
//        final CountDownLatch latch = new CountDownLatch(1);
        m.put("a", "b");
        //Start the first thread, acquire the lock and call remove
        new Thread("1. thread") {
            @Override
            public void run() {
                try {
                    Transaction tx = Hazelcast.getTransaction();
                    tx.begin();
                    t1Return.set(m.remove("a"));
                    Thread.sleep(1000);
                    tx.commit();
                } catch (InterruptedException ex) {
                }
            }
        }.start();
        //Start the second thread, ensure it call remove after tx in thread1 is committed
        Thread t2 = new Thread("2. thread") {
            @Override
            public void run() {
                try {
                    Transaction tx = Hazelcast.getTransaction();
                    tx.begin();
                    Thread.sleep(1000);//Make sure the first thread acquires the lock
                    t2Return.set(m.remove("a"));
                    tx.commit();
                } catch (InterruptedException ex) {
                }
            }
        };
        t2.start();
        t2.join();
        Assert.assertEquals("b", t1Return.get());
        Assert.assertNull("The remove in the second thread should return null", t2Return.get());
    }

    @Test
    public void issue770TestIMapTryPutUnderTransaction() {
        final HazelcastInstance hz = Hazelcast.getDefaultInstance();
        Transaction tx = hz.getTransaction();
        tx.begin();
        IMap<Object, Object> map = hz.getMap("issue770TestIMapTryPutUnderTransaction");
        Assert.assertTrue(map.tryPut("key", "value", 100, TimeUnit.MILLISECONDS));
        Assert.assertTrue(map.tryPut("key", "value2", 100, TimeUnit.MILLISECONDS));
        tx.commit();
    }

    /**
     * Github issue #99
     */
    @Test
    public void issue99TestQueueTakeAndDuringRollback() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.getDefaultInstance();
        final IQueue q = hz.getQueue("issue99TestQueueTakeAndDuringRollback");
        q.offer(1L);

        Thread t1 = new Thread() {
            public void run() {
                Transaction tx = Hazelcast.getTransaction();
                try {
                    tx.begin();
                    q.take();
                    sleep(1000);
                    throw new RuntimeException();
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                } catch (Exception e) {
                    tx.rollback();
                }
            }
        };
        final AtomicBoolean fail = new AtomicBoolean(false);
        Thread t2 = new Thread() {
            public void run() {
                Transaction tx = Hazelcast.getTransaction();
                try {
                    tx.begin();
                    q.take();
                    tx.commit();
                    fail.set(false);
                } catch (Exception e) {
                    tx.rollback();
                    e.printStackTrace();
                    fail.set(true);
                }
            }
        };

        t1.start();
        t2.start();
        t2.join();
        assertFalse("Queue take failed after rollback!", fail.get());
    }

    /**
     * Github issue #108
     */
    @Test
    public void issue108TestNpeDuringCommit() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.getDefaultInstance();
        final IMap map = hz.getMap("issue108TestNpeDuringCommit");

        final AtomicBoolean test1 = new AtomicBoolean(false);
        Thread t1 = new Thread() {
            public void run() {
                Transaction tx = Hazelcast.getTransaction();
                try {
                    tx.begin();
                    map.put(1, 1);
                    sleep(1000);
                    tx.commit();
                    test1.set(true);
                } catch (Exception e) {
                    e.printStackTrace();
                    tx.rollback();
                    test1.set(false);
                }
            }
        };
        final AtomicBoolean test2 = new AtomicBoolean(false);
        Thread t2 = new Thread() {
            public void run() {
                Transaction tx = Hazelcast.getTransaction();
                try {
                    tx.begin();
                    map.put(1, 2);
                    tx.commit();
                    test2.set(true);
                } catch (Exception e) {
                    e.printStackTrace();
                    tx.rollback();
                    test2.set(true);
                }
            }
        };

        t1.start();
        t2.start();
        t1.join();
        t2.join();
        assertTrue("Map1 put should be successful!", test1.get());
        assertTrue("Map2 put should be successful!", test2.get());
    }

    /**
     * Github issue #114
     */
    @Test
    public void issue114TestQueueListenersUnderTransaction() throws InterruptedException {
        final CountDownLatch offerLatch = new CountDownLatch(2);
        final CountDownLatch pollLatch = new CountDownLatch(2);
        final IQueue<String> testQueue = Hazelcast.getQueue("issue114TestQueueListenersUnderTransaction");
        testQueue.addItemListener(new ItemListener<String>() {
            public void itemAdded(ItemEvent<String> item) {
                offerLatch.countDown();
            }
            public void itemRemoved(ItemEvent<String> item) {
                pollLatch.countDown();
            }
        }, true);

        Transaction tx = Hazelcast.getTransaction();
        tx.begin();
        testQueue.put("tx Hello");
        testQueue.put("tx World");
        tx.commit();

        tx = Hazelcast.getTransaction();
        tx.begin();
        Assert.assertEquals("tx Hello", testQueue.poll());
        Assert.assertEquals("tx World", testQueue.poll());
        tx.commit();

        Assert.assertTrue("Remaining offer listener count: " + offerLatch.getCount(), offerLatch.await(2, TimeUnit.SECONDS));
        Assert.assertTrue("Remaining poll listener count: " + pollLatch.getCount(), pollLatch.await(2, TimeUnit.SECONDS));
    }

    final List<Instance> mapsUsed = new CopyOnWriteArrayList<Instance>();

    TransactionalMap newTransactionalMapProxy(String name) {
        return (TransactionalMap) newTransactionalProxy(Hazelcast.getMap(name), TransactionalMap.class);
    }

    TransactionalMultiMap newTransactionalMultiMapProxy(String name) {
        return (TransactionalMultiMap) newTransactionalProxy(Hazelcast.getMultiMap(name), TransactionalMultiMap.class);
    }

    TransactionalQueue newTransactionalQueueProxy(String name) {
        return (TransactionalQueue) newTransactionalProxy(Hazelcast.getQueue(name), TransactionalQueue.class);
    }

    TransactionalSet newTransactionalSetProxy(String name) {
        return (TransactionalSet) newTransactionalProxy(Hazelcast.getSet(name), TransactionalSet.class);
    }

    TransactionalList newTransactionalListProxy(String name) {
        return (TransactionalList) newTransactionalProxy(Hazelcast.getList(name), TransactionalList.class);
    }

    <T extends Instance> T newTransactionalProxy(Instance instance, Class klass) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{klass};
        Object proxy = Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(instance));
        T txnInstance = (T) proxy;
        mapsUsed.add(txnInstance);
        return txnInstance;
    }

    IMap newMapProxy(String name) {
        return newMapProxy(Hazelcast.getDefaultInstance(), name, mapsUsed);
    }

    public static IMap newMapProxy(HazelcastInstance hz, String name, List listToAdd) {
        IMap imap = hz.getMap(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{IMap.class};
        IMap proxy = (IMap) Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(imap));
        if (listToAdd != null) {
            listToAdd.add(proxy);
        }
        return proxy;
    }

    IQueue newQueueProxy(String name) {
        return newQueueProxy(Hazelcast.getDefaultInstance(), name, mapsUsed);
    }

    public static IQueue newQueueProxy(HazelcastInstance hz, String name, List listToAdd) {
        IQueue queue = hz.getQueue(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{IQueue.class};
        IQueue proxy = (IQueue) Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(queue));
        if (listToAdd != null) {
            listToAdd.add(proxy);
        }
        return proxy;
    }

    interface TransactionalMultiMap extends MultiMap {
        void begin();

        void commit();

        void rollback();
    }

    interface TransactionalMap extends IMap {

        Future async(String methodName, Object... arg);

        void begin();

        void commit();

        void rollback();
    }

    interface TransactionalQueue<E> extends IQueue<E> {
        void begin();

        void commit();

        void rollback();
    }

    interface TransactionalSet extends ISet {
        void begin();

        void commit();

        void rollback();
    }

    interface TransactionalList extends IList {
        void begin();

        void commit();

        void rollback();
    }

    public static class ThreadBoundInvocationHandler implements InvocationHandler {
        final Object target;
        final ExecutorService es = Executors.newSingleThreadExecutor();
        final static Object NULL_OBJECT = new Object();

        public ThreadBoundInvocationHandler(Object target) {
            this.target = target;
        }

        public Object invoke(final Object o, final Method method, final Object[] args) throws Throwable {
            final String name = method.getName();
            final BlockingQueue<Object> resultQ = ResponseQueueFactory.newResponseQueue();
            if (name.equals("async")) {
                return invokeAsync(args);
            } else if (name.equals("begin") || name.equals("commit") || name.equals("rollback")) {
                es.execute(new Runnable() {
                    public void run() {
                        try {
                            Transaction txn = Hazelcast.getTransaction();
                            if (name.equals("begin")) {
                                txn.begin();
                            } else if (name.equals("commit")) {
                                txn.commit();
                            } else if (name.equals("rollback")) {
                                txn.rollback();
                            }
                            resultQ.put(NULL_OBJECT);
                        } catch (Exception e) {
                            try {
                                resultQ.put(e);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                });
            } else {
                es.execute(new Runnable() {
                    public void run() {
                        try {
                            Object result = method.invoke(target, args);
                            resultQ.put((result == null) ? NULL_OBJECT : result);
                        } catch (Exception e) {
                            try {
                                resultQ.put(e);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                });
            }
            Object result = resultQ.poll(5, TimeUnit.SECONDS);
            if (result == null) throw new RuntimeException("Method [" + name + "] took more than 5 seconds!");
            if (name.equals("destroy")) {
                es.shutdown();
            }
            if (result instanceof Throwable) {
                throw ((Throwable) result);
            }
            return (result == NULL_OBJECT) ? null : result;
        }

        public Future invokeAsync(final Object[] args) throws Exception {
            String methodName = (String) args[0];
            final Object[] newArgs = (args.length == 1) ? null : (Object[]) args[1];
            int len = (newArgs == null) ? 0 : newArgs.length;
            final Method method = findMethod(methodName, len);
            return es.submit(new Callable() {
                public Object call() throws Exception {
                    return method.invoke(target, newArgs);
                }
            });
        }

        private Method findMethod(String name, int argLen) {
            Method[] methods = target.getClass().getMethods();
            for (Method method : methods) {
                if (method.getName().equals(name)) {
                    if (argLen == method.getParameterTypes().length) {
                        return method;
                    }
                }
            }
            return null;
        }
    }
}
