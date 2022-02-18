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

package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TxnMultiMapTest extends HazelcastTestSupport {

    @Test
    public void testTxnCommit() throws TransactionException {
        final String map1 = "map1";
        final String map2 = "map2";
        final String key = "1";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance();
        HazelcastInstance h2 = factory.newHazelcastInstance();

        boolean success = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                TransactionalMultiMap<String, String> txMap1 = context.getMultiMap(map1);
                TransactionalMultiMap<String, String> txMap2 = context.getMultiMap(map2);

                assertTrue(txMap1.put(key, "value1"));
                Object value1 = getSingleValue(txMap1, key);
                assertEquals("value1", value1);

                assertTrue(txMap2.put(key, "value2"));
                Object value2 = getSingleValue(txMap2, key);
                assertEquals("value2", value2);

                return true;
            }
        });
        assertTrue(success);

        assertEquals("value1", getSingleValue(h1.<String, String>getMultiMap(map1), key));
        assertEquals("value1", getSingleValue(h2.<String, String>getMultiMap(map1), key));

        assertEquals("value2", getSingleValue(h1.<String, String>getMultiMap(map2), key));
        assertEquals("value2", getSingleValue(h2.<String, String>getMultiMap(map2), key));
    }

    @Test
    public void testPutRemove() {
        String name = "defMM";

        Config config = new Config();
        config.getMultiMapConfig(name)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances(config);
        TransactionContext context = instances[0].newTransactionContext();

        try {
            context.beginTransaction();
            TransactionalMultiMap<String, String> txnMultiMap = context.getMultiMap(name);
            assertEquals(0, txnMultiMap.get("key1").size());
            assertEquals(0, txnMultiMap.valueCount("key1"));
            assertTrue(txnMultiMap.put("key1", "value1"));
            assertFalse(txnMultiMap.put("key1", "value1"));
            assertEquals(1, txnMultiMap.get("key1").size());
            assertEquals(1, txnMultiMap.valueCount("key1"));
            assertFalse(txnMultiMap.remove("key1", "value2"));
            assertTrue(txnMultiMap.remove("key1", "value1"));

            assertFalse(txnMultiMap.remove("key2", "value2"));
            context.commitTransaction();
        } catch (Exception e) {
            fail(e.getMessage());
            context.rollbackTransaction();
        }

        assertEquals(0, instances[1].getMultiMap(name).size());
        assertTrue(instances[2].getMultiMap(name).put("key1", "value1"));
        assertTrue(instances[2].getMultiMap(name).put("key2", "value2"));
    }

    @Test(expected = TransactionNotActiveException.class)
    public void testTxnMultimapOuterTransaction() {
        HazelcastInstance hz = createHazelcastInstance();

        TransactionContext transactionContext = hz.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalMultiMap<String, String> txnMultiMap = transactionContext.getMultiMap("testTxnMultimapOuterTransaction");
        txnMultiMap.put("key", "value");
        transactionContext.commitTransaction();
        txnMultiMap.get("key");
    }

    @Test
    public void testListener() {
        String mapName = "mm";
        long key = 1L;
        String value = "value";
        String value2 = "value2";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        final CountingEntryListener<String, String> listener = new CountingEntryListener<String, String>();

        MultiMap<String, String> multiMap = instance1.getMultiMap(mapName);
        multiMap.addEntryListener(listener, true);

        TransactionContext ctx1 = instance2.newTransactionContext();
        ctx1.beginTransaction();
        ctx1.getMultiMap(mapName).put(key, value);
        ctx1.commitTransaction();

        TransactionContext ctx2 = instance2.newTransactionContext();
        ctx2.beginTransaction();
        ctx2.getMultiMap(mapName).remove(key, value);
        ctx2.commitTransaction();

        TransactionContext ctx3 = instance2.newTransactionContext();
        ctx3.beginTransaction();
        ctx3.getMultiMap(mapName).put(key, value2);
        ctx3.commitTransaction();

        TransactionContext ctx4 = instance1.newTransactionContext();
        ctx4.beginTransaction();
        ctx4.getMultiMap(mapName).remove(key, value2);
        ctx4.commitTransaction();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(2, listener.getAddedCount());
                assertEquals(2, listener.getRemovedCount());
            }
        });
    }

    @Test
    public void testIssue1276Lock() {
        Long key = 1L;
        Long value = 1L;
        String mapName = "myMultimap";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        for (int i = 0; i < 2; i++) {
            TransactionContext ctx1 = instance1.newTransactionContext();
            ctx1.beginTransaction();
            BaseMultiMap<Long, Long> txProfileTasks1 = ctx1.getMultiMap(mapName);
            txProfileTasks1.put(key, value);
            ctx1.commitTransaction();

            TransactionContext ctx2 = instance2.newTransactionContext();
            ctx2.beginTransaction();
            BaseMultiMap<Long, Long> txProfileTasks2 = ctx2.getMultiMap(mapName);
            txProfileTasks2.remove(key, value);
            ctx2.commitTransaction();
        }
    }

    @Test
    public void testMultiMapContainsEntryTxn() {
        final HazelcastInstance instance = createHazelcastInstance();
        final TransactionContext context = instance.newTransactionContext();
        final MultiMap<Object, Object> mm = instance.getMultiMap("testMultiMapContainsEntry");
        mm.put("1", "value");
        assertTrue(mm.containsEntry("1", "value"));

        context.beginTransaction();
        TransactionalMultiMap<String, String> txnMap = context.getMultiMap("testMultiMapContainsEntry");
        txnMap.put("1", "value2");
        assertTrue(mm.containsEntry("1", "value"));
        assertFalse(mm.containsEntry("1", "value2"));
        txnMap.remove("1", "value2");
        assertTrue(mm.containsEntry("1", "value"));
        assertFalse(mm.containsEntry("1", "value2"));
        context.commitTransaction();

        assertTrue(mm.containsEntry("1", "value"));
        assertEquals(1, mm.size());
    }

    @Test
    public void testMultiMapPutRemoveWithTxn() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapPutRemoveWithTxn");

        multiMap.put("1", "C");
        multiMap.put("2", "x");
        multiMap.put("2", "y");

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        TransactionalMultiMap<String, String> txnMap = context.getMultiMap("testMultiMapPutRemoveWithTxn");
        txnMap.put("1", "A");
        txnMap.put("1", "B");
        Collection g1 = txnMap.get("1");
        assertContains(g1, "A");
        assertContains(g1, "B");
        assertContains(g1, "C");
        assertTrue(txnMap.remove("1", "C"));
        assertEquals(4, txnMap.size());
        Collection g2 = txnMap.get("1");
        assertContains(g2, "A");
        assertContains(g2, "B");
        assertFalse(g2.contains("C"));
        Collection r1 = txnMap.remove("2");
        assertContains(r1, "x");
        assertContains(r1, "y");
        assertEquals(0, txnMap.get("2").size());
        Collection r2 = txnMap.remove("1");
        assertEquals(2, r2.size());
        assertContains(r2, "A");
        assertContains(r2, "B");
        assertEquals(0, txnMap.get("1").size());
        assertEquals(0, txnMap.size());
        assertEquals(3, multiMap.size());
        context.commitTransaction();
        assertEquals(0, multiMap.size());
    }

    private static String getSingleValue(BaseMultiMap<String, String> multiMap, String key) {
        Collection<String> collection = multiMap.get(key);
        assertEquals(1, collection.size());
        return collection.iterator().next();
    }

    private static class CountingEntryListener<K, V> extends EntryAdapter<K, V> {

        private final AtomicInteger addedCount = new AtomicInteger();
        private final AtomicInteger removedCount = new AtomicInteger();

        public void entryAdded(EntryEvent<K, V> event) {
            addedCount.incrementAndGet();
        }

        public void entryRemoved(EntryEvent<K, V> event) {
            removedCount.incrementAndGet();
        }

        public int getAddedCount() {
            return addedCount.intValue();
        }

        public int getRemovedCount() {
            return removedCount.intValue();
        }
    }
}
