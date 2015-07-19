/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
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

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnMapTest {

    static HazelcastInstance client;
    static HazelcastInstance server;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testUnlockAfterRollback() {
        String mapName = randomString();
        String key = "key";

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> map = context.getMap(mapName);
        map.put(key, "value");
        context.rollbackTransaction();

        assertFalse(client.getMap(mapName).isLocked(key));
    }

    @Test
    public void testDeadLockFromClientInstance() throws InterruptedException {
        final String mapName = randomString();
        final String key = "key";
        final AtomicBoolean running = new AtomicBoolean(true);
        Thread t = new Thread() {
            public void run() {
                while (running.get()) {
                    client.getMap(mapName).get(key);
                }
            }
        };
        t.start();

        CBAuthorisation cb = new CBAuthorisation();
        cb.setAmount(15000);

        try {
            TransactionContext context = client.newTransactionContext();
            context.beginTransaction();

            TransactionalMap<String, CBAuthorisation> mapTransaction = context.getMap(mapName);
            // init data
            mapTransaction.put(key, cb);
            // start test deadlock, 3 set and concurrent, get deadlock

            cb.setAmount(12000);
            mapTransaction.set(key, cb);

            cb.setAmount(10000);
            mapTransaction.set(key, cb);

            cb.setAmount(900);
            mapTransaction.set(key, cb);

            cb.setAmount(800);
            mapTransaction.set(key, cb);

            cb.setAmount(700);
            mapTransaction.set(key, cb);

            context.commitTransaction();

        } catch (TransactionException e) {
            e.printStackTrace();
            fail();
        }
        running.set(false);
        t.join();
    }

    @SuppressWarnings("unused")
    private static class CBAuthorisation implements Serializable {
        private int amount;

        public void setAmount(int amount) {
            this.amount = amount;
        }

        public int getAmount() {
            return amount;
        }
    }

    @Test
    public void testTxnMapPut() throws Exception {
        String mapName = randomString();
        String key = "key";
        String value = "Value";
        IMap map = client.getMap(mapName);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txnMap = context.getMap(mapName);
        txnMap.put(key, value);
        context.commitTransaction();

        assertEquals(value, map.get(key));
    }

    @Test
    public void testTxnMapPut_BeforeCommit() throws Exception {
        String mapName = randomString();
        String key = "key";
        String value = "Value";

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txnMap = context.getMap(mapName);

        assertNull(txnMap.put(key, value));

        context.commitTransaction();
    }

    @Test
    public void testTxnMapGet_BeforeCommit() throws Exception {
        String mapName = randomString();
        String key = "key";
        String value = "Value";
        IMap map = client.getMap(mapName);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txnMap = context.getMap(mapName);

        txnMap.put(key, value);
        assertEquals(value, txnMap.get(key));
        assertNull(map.get(key));

        context.commitTransaction();
    }

    @Test
    public void testPutWithTTL() {
        String mapName = randomString();
        int ttlSeconds = 1;
        String key = "key";
        String value = "Value";
        IMap map = client.getMap(mapName);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txnMap = context.getMap(mapName);

        txnMap.put(key, value, ttlSeconds, TimeUnit.SECONDS);
        Object resultFromClientWhileTxnInProgress = map.get(key);

        context.commitTransaction();

        assertNull(resultFromClientWhileTxnInProgress);
        assertEquals(value, map.get(key));

        // wait for TTL to expire
        sleepSeconds(ttlSeconds + 1);

        assertNull(map.get(key));
    }

    @Test
    public void testGetForUpdate() throws TransactionException {
        final String mapName = randomString();
        final String key = "key";
        final int value = 888;

        int initialValue = 111;

        final CountDownLatch getKeyForUpdateLatch = new CountDownLatch(1);
        final CountDownLatch afterTryPutResult = new CountDownLatch(1);

        final IMap<String, Integer> map = client.getMap(mapName);
        map.put(key, initialValue);

        final AtomicBoolean tryPutResult = new AtomicBoolean(true);
        Runnable incrementer = new Runnable() {
            public void run() {
                try {
                    getKeyForUpdateLatch.await(30, TimeUnit.SECONDS);

                    boolean result = map.tryPut(key, value, 0, TimeUnit.SECONDS);
                    tryPutResult.set(result);

                    afterTryPutResult.countDown();
                } catch (Exception ignored) {
                }
            }
        };
        new Thread(incrementer).start();

        client.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                try {
                    TransactionalMap<String, Integer> txMap = context.getMap(mapName);
                    txMap.getForUpdate(key);
                    getKeyForUpdateLatch.countDown();
                    afterTryPutResult.await(30, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                }
                return true;
            }
        });

        assertFalse(tryPutResult.get());
    }

    @Test
    public void testKeySetValues() throws Exception {
        String mapName = randomString();
        IMap<String, String> map = client.getMap(mapName);
        map.put("key1", "value1");

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txMap = context.getMap(mapName);

        assertNull(txMap.put("key2", "value2"));
        assertEquals(2, txMap.size());
        assertEquals(2, txMap.keySet().size());
        assertEquals(2, txMap.values().size());

        context.commitTransaction();

        int size = map.size();
        assertEquals("map.size() should be 2, but was " + size, 2, size);
        size = map.keySet().size();
        assertEquals("map.keySet().size() should be 2, but was " + size, 2, size);
        size = map.values().size();
        assertEquals("map.values().size() should be 2, but was " + size, 2, size);
    }

    @Test
    public void testKeySetAndValuesWithPredicates() throws Exception {
        String mapName = randomString();
        IMap<SampleObjects.Employee, SampleObjects.Employee> map = client.getMap(mapName);

        SampleObjects.Employee emp1 = new SampleObjects.Employee("abc-123-xvz", 34, true, 10D);
        SampleObjects.Employee emp2 = new SampleObjects.Employee("abc-123-xvz", 20, true, 10D);

        map.put(emp1, emp1);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<SampleObjects.Employee, SampleObjects.Employee> txMap = context.getMap(mapName);

        assertNull(txMap.put(emp2, emp2));
        assertEquals(2, txMap.size());
        assertEquals(2, txMap.keySet().size());
        assertEquals(0, txMap.keySet(new SqlPredicate("age = 10")).size());
        assertEquals(0, txMap.values(new SqlPredicate("age = 10")).size());
        assertEquals(2, txMap.keySet(new SqlPredicate("age >= 10")).size());
        assertEquals(2, txMap.values(new SqlPredicate("age >= 10")).size());

        context.commitTransaction();

        assertEquals(2, map.size());
        assertEquals(2, map.values().size());
    }

    @Test
    public void testPutAndRoleBack() throws Exception {
        String mapName = randomString();
        String key = "key";
        String value = "value";
        IMap map = client.getMap(mapName);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> mapTxn = context.getMap(mapName);
        mapTxn.put(key, value);
        context.rollbackTransaction();

        assertNull(map.get(key));
    }

    @Test
    public void testTnxMapContainsKey() throws Exception {
        String mapName = randomString();
        IMap<String, String> map = client.getMap(mapName);
        map.put("key1", "value1");

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<String, String> txMap = context.getMap(mapName);
        txMap.put("key2", "value2");
        assertTrue(txMap.containsKey("key1"));
        assertTrue(txMap.containsKey("key2"));
        assertFalse(txMap.containsKey("key3"));

        context.commitTransaction();
    }

    @Test
    public void testTnxMapIsEmpty() throws Exception {
        String mapName = randomString();

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap txMap = context.getMap(mapName);
        assertTrue(txMap.isEmpty());
        context.commitTransaction();
    }

    @Test
    public void testTnxMapPutIfAbsent() throws Exception {
        String mapName = randomString();
        IMap<String, String> map = client.getMap(mapName);
        String keyValue1 = "keyValue1";
        String keyValue2 = "keyValue2";
        map.put(keyValue1, keyValue1);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<String, String> txMap = context.getMap(mapName);

        txMap.putIfAbsent(keyValue1, "NOT_THIS");
        txMap.putIfAbsent(keyValue2, keyValue2);

        context.commitTransaction();

        assertEquals(keyValue1, map.get(keyValue1));
        assertEquals(keyValue2, map.get(keyValue2));
    }

    @Test
    public void testTnxMapReplace() throws Exception {
        String mapName = randomString();
        IMap<String, String> map = client.getMap(mapName);
        String key1 = "key1";
        String key2 = "key2";
        String replaceValue = "replaceValue";
        map.put(key1, "OLD_VALUE");

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<String, String> txMap = context.getMap(mapName);

        txMap.replace(key1, replaceValue);
        txMap.replace(key2, "NOT_POSSIBLE");

        context.commitTransaction();

        assertEquals(replaceValue, map.get(key1));
        assertNull(map.get(key2));
    }

    @Test
    public void testTnxMapReplaceKeyValue() throws Exception {
        String mapName = randomString();
        String key1 = "key1";
        String oldValue1 = "old1";
        String newValue1 = "new1";
        String key2 = "key2";
        String oldValue2 = "old2";

        IMap<String, String> map = client.getMap(mapName);
        map.put(key1, oldValue1);
        map.put(key2, oldValue2);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<String, String> txMap = context.getMap(mapName);

        txMap.replace(key1, oldValue1, newValue1);
        txMap.replace(key2, "NOT_OLD_VALUE", "NEW_VALUE_CANT_BE_THIS");

        context.commitTransaction();

        assertEquals(newValue1, map.get(key1));
        assertEquals(oldValue2, map.get(key2));
    }

    @Test
    public void testTnxMapRemove() throws Exception {
        String mapName = randomString();
        String key = "key1";
        String value = "old1";

        IMap<String, String> map = client.getMap(mapName);
        map.put(key, value);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap txMap = context.getMap(mapName);

        txMap.remove(key);

        context.commitTransaction();

        assertNull(map.get(key));
    }

    @Test
    public void testTnxMapRemoveKeyValue() throws Exception {
        String mapName = randomString();
        String key1 = "key1";
        String oldValue1 = "old1";
        String key2 = "key2";
        String oldValue2 = "old2";

        IMap<String, String> map = client.getMap(mapName);
        map.put(key1, oldValue1);
        map.put(key2, oldValue2);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap txMap = context.getMap(mapName);

        txMap.remove(key1, oldValue1);
        txMap.remove(key2, "NO_REMOVE_AS_NOT_VALUE");

        context.commitTransaction();

        assertNull(map.get(key1));
        assertEquals(oldValue2, map.get(key2));
    }

    @Test
    public void testTnxMapDelete() throws Exception {
        String mapName = randomString();
        String key = "key1";
        String value = "old1";

        IMap<String, String> map = client.getMap(mapName);
        map.put(key, value);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap txMap = context.getMap(mapName);

        txMap.delete(key);

        context.commitTransaction();

        assertNull(map.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void testKeySetPredicateNull() throws Exception {
        String mapName = randomString();

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txMap = context.getMap(mapName);

        txMap.keySet(null);
    }

    @Test(expected = NullPointerException.class)
    public void testKeyValuesPredicateNull() throws Exception {
        String mapName = randomString();

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txMap = context.getMap(mapName);

        txMap.values(null);
    }
}
