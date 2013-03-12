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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapTransactionTest {

    @BeforeClass
    public static void init() {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanUp() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTxnCommit() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("1", "value");
                assertEquals("value", txMap.put("1", "value2"));
                assertEquals("value2", txMap.get("1"));
                assertEquals(true, txMap.containsKey("1"));
                assertNull(map2.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value2", map1.get("1"));
        assertEquals("value2", map2.get("1"));
    }

    @Test
    public void testTxnSet() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.set("1", "value");
                txMap.set("1", "value2");
                assertEquals("value2", txMap.get("1"));
                assertNull(map2.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value2", map1.get("1"));
        assertEquals("value2", map2.get("1"));
    }

    @Test
    public void testTxnRemove() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1","1");
        map2.put("2","2");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("3", "3");
                map2.put("4", "4");
                assertEquals("1", txMap.remove("1"));
                assertEquals("2", map2.remove("2"));
                assertEquals("1", map2.get("1"));
                assertEquals(null, txMap.get("1"));
                assertEquals(null, txMap.remove("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals(null, map1.get("1"));
        assertEquals(null, map2.get("1"));

        assertEquals(null, map1.get("2"));
        assertEquals(null, map2.get("2"));

        assertEquals("3", map1.get("3"));
        assertEquals("3", map2.get("3"));

        assertEquals("4", map1.get("4"));
        assertEquals("4", map2.get("4"));
    }

    @Test
    public void testTxnRemoveIfSame() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1","1");
        map2.put("2","2");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("3", "3");
                map2.put("4", "4");
                assertEquals(true, txMap.remove("1","1"));
                assertEquals(false, txMap.remove("2","1"));
                assertEquals("1", map2.get("1"));
                assertEquals(null, txMap.get("1"));
                assertEquals(true, txMap.remove("2","2"));
                assertEquals(false, txMap.remove("3",null));
                assertEquals(false, txMap.remove("5","2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals(null, map1.get("1"));
        assertEquals(null, map2.get("1"));

        assertEquals(null, map1.get("2"));
        assertEquals(null, map2.get("2"));

        assertEquals("3", map1.get("3"));
        assertEquals("3", map2.get("3"));

        assertEquals("4", map1.get("4"));
        assertEquals("4", map2.get("4"));
    }

    @Test
    public void testTxnDelete() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1","1");
        map2.put("2","2");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("3", "3");
                map2.put("4", "4");
                txMap.delete("1");
                map2.delete("2");
                assertEquals("1", map2.get("1"));
                assertEquals(null, txMap.get("1"));
                txMap.delete("2");
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals(null, map1.get("1"));
        assertEquals(null, map2.get("1"));

        assertEquals(null, map1.get("2"));
        assertEquals(null, map2.get("2"));

        assertEquals("3", map1.get("3"));
        assertEquals("3", map2.get("3"));

        assertEquals("4", map1.get("4"));
        assertEquals("4", map2.get("4"));
    }

    @Test
    public void testTxnPutIfAbsent() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.putIfAbsent("1", "value");
                assertEquals("value", txMap.putIfAbsent("1", "value2"));
                assertEquals("value", txMap.get("1"));
                assertNull(map2.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value", map1.get("1"));
        assertEquals("value", map2.get("1"));
    }

    @Test
    public void testTxnReplaceIfSame() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1","1");
        map2.put("2","2");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                assertEquals(true, txMap.replace("1","1", "11"));
                assertEquals(false, txMap.replace("5", "5", "55"));
                assertEquals(false, txMap.replace("2", "1", "22"));
                assertEquals("1", map2.get("1"));
                assertEquals("11", txMap.get("1"));
                assertEquals("2", map2.get("2"));
                assertEquals("2", txMap.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("11", map1.get("1"));
        assertEquals("11", map2.get("1"));
        assertEquals("2", map1.get("2"));
        assertEquals("2", map2.get("2"));
    }

    @Test
    public void testTxnReplace() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                assertNull(txMap.replace("1", "value"));
                txMap.put("1","value2");
                assertEquals("value2", txMap.replace("1", "value3"));
                assertEquals("value3", txMap.get("1"));
                assertNull(map2.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        }, new TransactionOptions().setTimeout(1, TimeUnit.SECONDS));
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value3", map1.get("1"));
        assertEquals("value3", map2.get("1"));
    }

    @Test
    public void testTxnReplace2() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1","value2");
        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                assertEquals("value2", txMap.replace("1", "value3"));
                assertEquals("value3", txMap.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value3", map1.get("1"));
        assertEquals("value3", map2.get("1"));
    }

//    @Test
//    public void testTxnRollback() {
//        Config config = new Config();
//        StaticNodeFactory factory = new StaticNodeFactory(2);
//        HazelcastInstance h1 = factory.newInstance(config);
//        HazelcastInstance h2 = factory.newInstance(config);
//        IMap map1 = h1.getMap("default");
//        IMap map2 = h2.getMap("default");
//        Transaction txn = h1.getTransaction();
//        txn.begin();
//        map1.put("1", "value");
//        map1.put("13", "value");
//        assertEquals("value", map1.get("1"));
//        assertEquals("value", map1.get("13"));
//        assertNull(map2.get("1"));
//        assertNull(map2.get("13"));
//        txn.rollback();
//        assertNull(map1.get("1"));
//        assertNull(map1.get("13"));
//        assertNull(map2.get("1"));
//        assertNull(map2.get("13"));
//    }

//    @Test(expected = IllegalStateException.class)
//    public void testAccessTransactionalMapAfterTxEnds() throws TransactionException {
//        Config config = new Config();
//        StaticNodeFactory factory = new StaticNodeFactory(2);
//        HazelcastInstance hz = factory.newHazelcastInstance(config);
//        TransactionContext txCtx = hz.newTransactionContext();
//        Transaction tx = txCtx.beginTransaction();
//        TransactionalMap txMap = txCtx.getMap("default");
//        tx.commit();
//        txMap.put("1", "value");
//    }
//
//    @Test
//    public void testTxnTimeout() {
//        Config config = new Config();
//        StaticNodeFactory factory = new StaticNodeFactory(2);
//        HazelcastInstance h1 = factory.newHazelcastInstance(config);
//        HazelcastInstance h2 = factory.newHazelcastInstance(config);
//        IMap map1 = h1.getMap("default");
//        IMap map2 = h2.getMap("default");
//        TransactionContext txCtx = h1.newTransactionContext();
//        Transaction tx = txCtx.getTransaction();
//        tx.setTransactionTimeout(1);
//        tx.begin();
//        TransactionalMap txMap = txCtx.getMap("default");
//        try {
//            txMap.put("1", "value");
//            Thread.sleep(1100);
//            txMap.put("13", "value");
//            tx.commit();
//            fail();
//        } catch (Throwable e) {
//            tx.rollback();
//        }

//        assertNull(map1.get("1"));
//        assertFalse(map1.isLocked("1"));
//        assertTrue(map1.tryLock("1"));

//        txCtx = h1.newTransactionContext();
//        tx = txCtx.beginTransaction();
//        txMap = txCtx.getMap("default");
//        try {
//            txMap.put("1", "value");
//            tx.commit();
//        } catch (Exception e) {
//            tx.rollback();
//            e.printStackTrace();
//            fail(e.getMessage());
//        }
//        assertEquals("value", map1.get("1"));
//        assertTrue(map1.isLocked("1"));
//    }


}
