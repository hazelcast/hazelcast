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

package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

/**
 * @author ali 6/10/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnMapTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
//        second = Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient(null);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testPutGet() throws Exception {
        final String name = "defMap";

        final TransactionContext context = hz.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> map = context.getMap(name);
        assertNull(map.put("key1", "value1"));
        assertEquals("value1", map.get("key1"));
        assertNull(hz.getMap(name).get("key1"));
        context.commitTransaction();

        assertEquals("value1", hz.getMap(name).get("key1"));
    }

    @Test
    public void testGetForUpdate() throws TransactionException {
        final IMap<String, Integer> map = hz.getMap("testTxnGetForUpdate");
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        map.put("var", 0);
        final AtomicBoolean pass = new AtomicBoolean(true);


        Runnable incrementor = new Runnable() {
            public void run() {
                try {
                    latch1.await(100, TimeUnit.SECONDS);
                    pass.set(map.tryPut("var", 1, 0, TimeUnit.SECONDS) == false);
                    latch2.countDown();
                } catch (Exception e) {
                }
            }
        };
        new Thread(incrementor).start();
        boolean b = hz.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                try {
                    final TransactionalMap<String, Integer> txMap = context.getMap("testTxnGetForUpdate");
                    txMap.getForUpdate("var");
                    latch1.countDown();
                    latch2.await(100, TimeUnit.SECONDS);
                } catch (Exception e) {
                }
                return true;
            }
        });
        assertTrue(b);
        assertTrue(pass.get());
        assertTrue(map.tryPut("var", 1, 0, TimeUnit.SECONDS));
    }


    @Test
    public void testKeySetValues() throws Exception {
        final String name = "testKeySetValues";
        IMap<Object, Object> map = hz.getMap(name);
        map.put("key1", "value1");
        map.put("key2", "value2");

        final TransactionContext context = hz.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> txMap = context.getMap(name);
        assertNull(txMap.put("key3", "value3"));


        assertEquals(3, txMap.size());
        assertEquals(3, txMap.keySet().size());
        assertEquals(3, txMap.values().size());
        context.commitTransaction();

        assertEquals(3, map.size());
        assertEquals(3, map.keySet().size());
        assertEquals(3, map.values().size());

    }

    @Test
    public void testKeysetAndValuesWithPredicates() throws Exception {
        final String name = "testKeysetAndValuesWithPredicates";
        IMap<Object, Object> map = hz.getMap(name);

        final SampleObjects.Employee emp1 = new SampleObjects.Employee("abc-123-xvz", 34, true, 10D);
        final SampleObjects.Employee emp2 = new SampleObjects.Employee("abc-123-xvz", 20, true, 10D);

        map.put(emp1, emp1);

        final TransactionContext context = hz.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> txMap = context.getMap(name);
        assertNull(txMap.put(emp2, emp2));

        assertEquals(2, txMap.size());
        assertEquals(2, txMap.keySet().size());
        assertEquals(0, txMap.keySet(new SqlPredicate("age = 10")).size());
        assertEquals(0, txMap.values(new SqlPredicate("age = 10")).size());
        assertEquals(2, txMap.keySet(new SqlPredicate("age >= 10")).size());
        assertEquals(2, txMap.values(new SqlPredicate("age >= 10")).size());

        context.commitTransaction();

        assertEquals(2, map.size());
//        assertEquals(1, txMap.keySet( new SqlPredicate( "age = 20" ) ).size() );
        assertEquals(2, map.values().size());

    }

}
