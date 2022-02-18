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

package com.hazelcast.client.txn;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author ali 6/10/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnMapTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testUnlockAfterRollback() {
        final String mapName = randomString();
        final String key = "key";

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> map = context.getMap(mapName);
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

            TransactionalMap mapTransaction = context.getMap(mapName);
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

    public static class CBAuthorisation implements Serializable {
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
        final String mapName = randomString();
        final String key = "key";
        final String value = "Value";
        final IMap map = client.getMap(mapName);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> txnMap = context.getMap(mapName);
        txnMap.put(key, value);
        context.commitTransaction();

        assertEquals(value, map.get(key));
    }

    @Test
    public void testTxnMapPut_BeforeCommit() throws Exception {
        final String mapName = randomString();
        final String key = "key";
        final String value = "Value";
        final IMap map = client.getMap(mapName);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> txnMap = context.getMap(mapName);

        assertNull(txnMap.put(key, value));

        context.commitTransaction();
    }

    @Test
    public void testTxnMapGet_BeforeCommit() throws Exception {
        final String mapName = randomString();
        final String key = "key";
        final String value = "Value";
        final IMap map = client.getMap(mapName);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> txnMap = context.getMap(mapName);

        txnMap.put(key, value);
        assertEquals(value, txnMap.get(key));
        assertNull(map.get(key));

        context.commitTransaction();
    }

    @Test
    public void testPutWithTTL() {
        final String mapName = randomString();
        final String key = "key";
        final String value = "Value";
        final IMap map = client.getMap(mapName);

        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();

        TransactionalMap<Object, Object> txnMap = context.getMap(mapName);
        txnMap.put(key, value, 10, TimeUnit.SECONDS);
        assertNull(map.get(key));

        context.commitTransaction();

        // caution!: it can still happen that entry is evicted before map.get(key) returns.
        // hence following assertion can fail
        assertEquals(value, map.get(key));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(map.get(key));
            }
        });
    }

    @Test
    public void testGetForUpdate() throws TransactionException {
        final String mapName = randomString();
        final String key = "key";
        final int initialValue = 111;
        final int value = 888;

        final CountDownLatch getKeyForUpdateLatch = new CountDownLatch(1);
        final CountDownLatch afterTryPutResult = new CountDownLatch(1);

        final IMap<String, Integer> map = client.getMap(mapName);
        map.put(key, initialValue);

        final AtomicBoolean tryPutResult = new AtomicBoolean(true);
        Runnable incrementor = new Runnable() {
            public void run() {
                try {
                    getKeyForUpdateLatch.await(30, TimeUnit.SECONDS);

                    boolean result = map.tryPut(key, value, 0, TimeUnit.SECONDS);
                    tryPutResult.set(result);

                    afterTryPutResult.countDown();
                } catch (InterruptedException e) {
                }
            }
        };
        new Thread(incrementor).start();

        client.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                try {
                    final TransactionalMap<String, Integer> txMap = context.getMap(mapName);
                    txMap.getForUpdate(key);
                    getKeyForUpdateLatch.countDown();
                    afterTryPutResult.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                }
                return true;
            }
        });

        assertFalse(tryPutResult.get());
    }


    @Test
    public void testKeySetValues() throws Exception {
        final String mapName = randomString();
        IMap map = client.getMap(mapName);
        map.put("key1", "value1");
        map.put("key2", "value1");

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> txMap = context.getMap(mapName);

        assertNull(txMap.put("key3", "value2"));
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
        final String mapName = randomString();
        IMap map = client.getMap(mapName);

        final SampleTestObjects.Employee emp1 = new SampleTestObjects.Employee("abc-123-xvz", 34, true, 10D);
        final SampleTestObjects.Employee emp2 = new SampleTestObjects.Employee("abc-123-xvz", 20, true, 10D);

        map.put(emp1, emp1);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);

        assertNull(txMap.put(emp2, emp2));
        assertEquals(2, txMap.size());
        assertEquals(2, txMap.keySet().size());
        assertEquals(0, txMap.keySet(Predicates.sql("age = 10")).size());
        assertEquals(0, txMap.values(Predicates.sql("age = 10")).size());
        assertEquals(2, txMap.keySet(Predicates.sql("age >= 10")).size());
        assertEquals(2, txMap.values(Predicates.sql("age >= 10")).size());

        context.commitTransaction();

        assertEquals(2, map.size());
        assertEquals(2, map.values().size());
    }

    @Test
    public void testDuplicateValuesWithPredicates() throws Exception {
        final String mapName = randomString();
        IMap map = client.getMap(mapName);

        final SampleTestObjects.Employee emp1 = new SampleTestObjects.Employee("employee1", 10, true, 10D);

        map.put("employee1", emp1);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);

        assertNull(txMap.put("employee1_repeated", emp1));

        assertEquals(2, txMap.size());
        assertEquals(2, txMap.keySet(Predicates.sql("age = 10")).size());
        assertEquals(2, txMap.values(Predicates.sql("age = 10")).size());

        context.commitTransaction();

        assertEquals(2, map.keySet(Predicates.sql("age = 10")).size());
        assertEquals(2, map.values(Predicates.sql("age = 10")).size());
    }

    @Test
    public void testPutAndRoleBack() throws Exception {
        final String mapName = randomString();
        final String key = "key";
        final String value = "value";
        final IMap map = client.getMap(mapName);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> mapTxn = context.getMap(mapName);
        mapTxn.put(key, value);
        context.rollbackTransaction();

        assertNull(map.get(key));
    }

    @Test
    public void testTnxMapContainsKey() throws Exception {
        final String mapName = randomString();
        IMap map = client.getMap(mapName);
        map.put("key1", "value1");

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);
        txMap.put("key2", "value2");
        assertTrue(txMap.containsKey("key1"));
        assertTrue(txMap.containsKey("key2"));
        assertFalse(txMap.containsKey("key3"));

        context.commitTransaction();
    }

    @Test
    public void testTnxMapIsEmpty() throws Exception {
        final String mapName = randomString();
        IMap map = client.getMap(mapName);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);
        assertTrue(txMap.isEmpty());
        context.commitTransaction();
    }

    @Test
    public void testTnxMapPutIfAbsent() throws Exception {
        final String mapName = randomString();
        IMap map = client.getMap(mapName);
        final String keyValue1 = "keyValue1";
        final String keyValue2 = "keyValue2";
        map.put(keyValue1, keyValue1);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);

        txMap.putIfAbsent(keyValue1, "NOT_THIS");
        txMap.putIfAbsent(keyValue2, keyValue2);

        context.commitTransaction();

        assertEquals(keyValue1, map.get(keyValue1));
        assertEquals(keyValue2, map.get(keyValue2));
    }

    @Test
    public void testTnxMapReplace() throws Exception {
        final String mapName = randomString();
        IMap map = client.getMap(mapName);
        final String key1 = "key1";
        final String key2 = "key2";
        final String replaceValue = "replaceValue";
        map.put(key1, "OLD_VALUE");

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);

        txMap.replace(key1, replaceValue);
        txMap.replace(key2, "NOT_POSSIBLE");

        context.commitTransaction();

        assertEquals(replaceValue, map.get(key1));
        assertNull(map.get(key2));
    }

    @Test
    public void testTnxMapReplaceKeyValue() throws Exception {
        final String mapName = randomString();
        final String key1 = "key1";
        final String oldValue1 = "old1";
        final String newValue1 = "new1";
        final String key2 = "key2";
        final String oldValue2 = "old2";

        IMap map = client.getMap(mapName);
        map.put(key1, oldValue1);
        map.put(key2, oldValue2);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);

        txMap.replace(key1, oldValue1, newValue1);
        txMap.replace(key2, "NOT_OLD_VALUE", "NEW_VALUE_CANT_BE_THIS");

        context.commitTransaction();

        assertEquals(newValue1, map.get(key1));
        assertEquals(oldValue2, map.get(key2));
    }

    @Test
    public void testTnxMapRemove() throws Exception {
        final String mapName = randomString();
        final String key = "key1";
        final String value = "old1";

        IMap map = client.getMap(mapName);
        map.put(key, value);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);

        txMap.remove(key);

        context.commitTransaction();

        assertNull(map.get(key));
    }

    @Test
    public void testTnxMapRemoveKeyValue() throws Exception {
        final String mapName = randomString();
        final String key1 = "key1";
        final String oldValue1 = "old1";
        final String key2 = "key2";
        final String oldValue2 = "old2";

        IMap map = client.getMap(mapName);
        map.put(key1, oldValue1);
        map.put(key2, oldValue2);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);

        txMap.remove(key1, oldValue1);
        txMap.remove(key2, "NO_REMOVE_AS_NOT_VALUE");

        context.commitTransaction();

        assertNull(map.get(key1));
        assertEquals(oldValue2, map.get(key2));
    }

    @Test
    public void testTnxMapDelete() throws Exception {
        final String mapName = randomString();
        final String key = "key1";
        final String value = "old1";

        IMap map = client.getMap(mapName);
        map.put(key, value);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap txMap = context.getMap(mapName);

        txMap.delete(key);

        context.commitTransaction();

        assertNull(map.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void testKeySetPredicateNull() throws Exception {
        final String mapName = randomString();

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> txMap = context.getMap(mapName);

        txMap.keySet(null);
    }

    @Test(expected = NullPointerException.class)
    public void testKeyValuesPredicateNull() throws Exception {
        final String mapName = randomString();

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalMap<Object, Object> txMap = context.getMap(mapName);

        txMap.values(null);
    }

    @Test
    public void txn_map_get_skips_server_side_near_cache() {
        String mapName = "test";
        int keyInServerNearCache = 1;

        IMap serverMap = prepareServerAndGetServerMap(mapName, keyInServerNearCache);
        TransactionalMap clientTxnMap = getClientTransactionalMap(mapName);

        assertNotNull(clientTxnMap.get(keyInServerNearCache));
        assertEquals(0, serverMap.getLocalMapStats().getNearCacheStats().getHits());
    }

    @Test
    public void txn_map_containsKey_skips_server_side_near_cache() {
        String mapName = "test";
        int keyInServerNearCache = 1;

        IMap serverMap = prepareServerAndGetServerMap(mapName, keyInServerNearCache);
        TransactionalMap clientTxnMap = getClientTransactionalMap(mapName);

        assertTrue(clientTxnMap.containsKey(keyInServerNearCache));
        assertEquals(0, serverMap.getLocalMapStats().getNearCacheStats().getHits());
    }

    private IMap prepareServerAndGetServerMap(String mapName, int keyInServerNearCache) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);

        Config serverConfig = new Config();
        serverConfig.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(serverConfig);

        // populate server near cache.
        IMap map = server.getMap(mapName);
        map.put(keyInServerNearCache, 1);
        map.get(keyInServerNearCache);

        return map;
    }

    private TransactionalMap getClientTransactionalMap(String mapName) {
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        TransactionContext clientTxnContext = client.newTransactionContext();
        clientTxnContext.beginTransaction();
        return clientTxnContext.getMap(mapName);
    }
}
