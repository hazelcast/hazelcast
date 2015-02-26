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

package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.BaseMultiMap;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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

/**
 * @author ali 4/5/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TxnMultiMapTest extends HazelcastTestSupport {

    @Test
    public void testTxnCommit() throws TransactionException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance();
        final HazelcastInstance h2 = factory.newHazelcastInstance();
        final String map1 = "map1";
        final String map2 = "map2";
        final String key = "1";

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMultiMap<Object, Object> txMap1 = context.getMultiMap(map1);
                final TransactionalMultiMap<Object, Object> txMap2 = context.getMultiMap(map2);

                assertTrue(txMap1.put(key, "value1"));
                Object value1 = getSingleValue(txMap1, key);
                assertEquals("value1", value1);

                assertTrue(txMap2.put(key, "value2"));
                Object value2 = getSingleValue(txMap2, key);
                assertEquals("value2", value2);

                return true;
            }
        });
        assertTrue(b);

        assertEquals("value1", getSingleValue(h1.getMultiMap(map1), key));
        assertEquals("value1", getSingleValue(h2.getMultiMap(map1), key));

        assertEquals("value2", getSingleValue(h1.getMultiMap(map2), key));
        assertEquals("value2", getSingleValue(h2.getMultiMap(map2), key));
    }

    private Object getSingleValue(BaseMultiMap multiMap, String key) {
        Collection c = multiMap.get(key);
        assertEquals(1, c.size());
        return c.iterator().next();
    }

    @Test
    public void testPutRemove() {
        Config config = new Config();
        final String name = "defMM";
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);

        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        TransactionContext context = instances[0].newTransactionContext();
        try {
            context.beginTransaction();
            TransactionalMultiMap mm = context.getMultiMap(name);
            assertEquals(0, mm.get("key1").size());
            assertEquals(0, mm.valueCount("key1"));
            assertTrue(mm.put("key1", "value1"));
            assertFalse(mm.put("key1", "value1"));
            assertEquals(1, mm.get("key1").size());
            assertEquals(1, mm.valueCount("key1"));
            assertFalse(mm.remove("key1", "value2"));
            assertTrue(mm.remove("key1", "value1"));

            assertFalse(mm.remove("key2", "value2"));
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
    public void testTxnMultimapOuterTransaction() throws Throwable {
        final HazelcastInstance h1 = createHazelcastInstance();

        final TransactionContext transactionContext = h1.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalMultiMap<Object, Object> mm = transactionContext.getMultiMap("testTxnMultimapOuterTransaction");
        mm.put("key", "value");
        transactionContext.commitTransaction();
        mm.get("key");
    }

    @Test
    public void testListener() throws InterruptedException {
        String mapName = "mm";
        long key = 1L;
        String value = "value";
        String value2 = "value2";
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        CountingEntryListener<Object, Object> listener = new CountingEntryListener<Object, Object>();

        MultiMap<Object, Object> map = instance1.getMultiMap(mapName);
        map.addEntryListener(listener, true);

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

        Thread.sleep(100);

        assertEquals(2, listener.getAddedCount());
        assertEquals(2, listener.getRemovedCount());
    }

    private class CountingEntryListener<K,V> extends EntryAdapter<K,V> {
        private final AtomicInteger addedCount = new AtomicInteger();
        private final AtomicInteger  removedCount = new AtomicInteger();

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

    @Test
    public void testIssue1276Lock() throws InterruptedException {
        Long key = 1L;
        Long value = 1L;
        String mapName = "myMultimap";
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        for (int i=0; i<2; i++) {
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
}
