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

package com.hazelcast.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.BaseMultiMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.*;

/**
 * @ali 4/5/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class TxnMultiMapTest extends HazelcastTestSupport {

    @Test
    public void testTxnCommit() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
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
    public void testPutRemove(){
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
            assertTrue(mm.put("key1","value1"));
            assertFalse(mm.put("key1", "value1"));
            assertEquals(1, mm.get("key1").size());
            assertEquals(1, mm.valueCount("key1"));
            assertFalse(mm.remove("key1","value2"));
            assertTrue(mm.remove("key1","value1"));

            assertFalse(mm.remove("key2","value2"));
            context.commitTransaction();
        } catch (Exception e){
            fail(e.getMessage());
            context.rollbackTransaction();
        }

        assertEquals(0, instances[1].getMultiMap(name).size());
        assertTrue(instances[2].getMultiMap(name).put("key1","value1"));
        assertTrue(instances[2].getMultiMap(name).put("key2","value2"));
    }

    @Test
    public void testPutRemoveList(){
        Config config = new Config();
        final String name = "defList";

        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        TransactionContext context = instances[0].newTransactionContext();
        try {
            context.beginTransaction();

            TransactionalList mm = context.getList(name);
            assertEquals(0, mm.size());
            assertTrue(mm.add("value1"));
            assertTrue(mm.add("value1"));
            assertEquals(2, mm.size());
            assertFalse(mm.remove("value2"));
            assertTrue(mm.remove("value1"));

            context.commitTransaction();
        } catch (Exception e){
            fail(e.getMessage());
            context.rollbackTransaction();
        }

        assertEquals(1, instances[1].getList(name).size());
        assertTrue(instances[2].getList(name).add("value1"));
    }
}
