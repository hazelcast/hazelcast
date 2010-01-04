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

package com.hazelcast.core;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class TransactionTest {
    @Test
    public void testMapPutSimple() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapPutSimple");
        txnMap.begin();
        txnMap.put("1", "value");
        txnMap.commit();
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
    }

    @Test
    public void testSetCommit() {
        TransactionalSet set = newTransactionalSetProxy("testSetCommit");
        set.begin();
        set.add("item");
        set.commit();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetCommitTwoSameItems() {
        TransactionalSet set = newTransactionalSetProxy("testSetCommitTwoSameItems");
        set.begin();
        set.add("item");
        set.add("item");
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
        txnMap.commit();
        txnMap2.begin();
        txnMap2.put("1", "value2");
        txnMap2.commit();
    }

    @Test
    public void testMapRemoveWithTwoTxn() {
        Hazelcast.getMap("testMapRemoveWithTwoTxn").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testMapRemoveWithTwoTxn");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapRemoveWithTwoTxn");
        txnMap.begin();
        txnMap.remove("1");
        txnMap.commit();
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
        long start = System.currentTimeMillis();
        assertFalse(txnMap2.tryLock("1", 2, TimeUnit.SECONDS));
        long end = System.currentTimeMillis();
        long took = (end - start);
        assertTrue((took > 1000) ? (took < 4000) : false);
        assertFalse(txnMap2.tryLock("1"));
        txnMap.unlock("1");
        assertTrue(txnMap2.tryLock("1", 2, TimeUnit.SECONDS));
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
        long start = System.currentTimeMillis();
        assertFalse(txnMap2.tryPut("1", "value2", 2, TimeUnit.SECONDS));
        long end = System.currentTimeMillis();
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
        assertEquals("item2", txnq2.poll());
        assertEquals("item1", txnq2.poll());
    }

    @Test
    @Ignore
    public void testMapEntryLastAccessTime() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMapEntryLastAccessTime");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMapEntryLastAccessTime");
        txnMap.put("1", "value1");
        MapEntry mapEntry = txnMap.getMapEntry("1");
        System.out.println("txn test time " + mapEntry.getLastAccessTime());
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

    List<Instance> mapsUsed = new CopyOnWriteArrayList<Instance>();

    TransactionalMap newTransactionalMapProxy(String name) {
        return (TransactionalMap) newTransactionalProxy(Hazelcast.getMap(name), TransactionalMap.class);
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
        IMap imap = Hazelcast.getMap(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{IMap.class};
        IMap proxy = (IMap) Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(imap));
        mapsUsed.add(proxy);
        return proxy;
    }

    interface TransactionalMap extends IMap {
        void begin();

        void commit();

        void rollback();
    }

    interface TransactionalQueue extends IQueue {
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

        public Object invoke(final Object o, final Method method, final Object[] objects) throws Throwable {
            final String name = method.getName();
            final BlockingQueue resultQ = new ArrayBlockingQueue(1);
            if (name.equals("begin") || name.equals("commit") || name.equals("rollback")) {
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
                            Object result = method.invoke(target, objects);
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
    }
}
