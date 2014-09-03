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

package com.hazelcast.client.impl.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnMultiMapTest {

    private static final String multiMapBackedByList = "BackedByList*";
    static HazelcastInstance client;
    static HazelcastInstance server;

    @BeforeClass
    public static void init() {

        Config config = new Config();
        MultiMapConfig multiMapConfig = config.getMultiMapConfig(multiMapBackedByList);
        multiMapConfig.setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testRemove() throws Exception {
        final String mapName = randomString();
        final String key = "key";
        final String val = "value";

        MultiMap multiMap = client.getMultiMap(mapName);
        multiMap.put(key, val);

        TransactionContext tx = client.newTransactionContext();
        tx.beginTransaction();

        TransactionalMultiMap txnMultiMap = tx.getMultiMap(mapName);
        txnMultiMap.remove(key, val);

        tx.commitTransaction();

        assertEquals(Collections.EMPTY_SET, client.getMultiMap(mapName).get(key));
    }

    @Test
    public void testRemoveAll() throws Exception {
        final String mapName = randomString();
        final String key = "key";

        MultiMap multiMap = client.getMultiMap(mapName);
        for (int i = 0; i < 10; i++) {
            multiMap.put(key, i);
        }

        TransactionContext tx = client.newTransactionContext();
        tx.beginTransaction();

        TransactionalMultiMap txnMultiMap = tx.getMultiMap(mapName);
        txnMultiMap.remove(key);
        tx.commitTransaction();

        assertEquals(Collections.EMPTY_SET, multiMap.get(key));
    }

    @Test
    public void testConcrruentTxnPut() throws Exception {
        final String mapName = randomString();
        final MultiMap multiMap = client.getMultiMap(mapName);

        final int threads = 10;
        final ExecutorService ex = Executors.newFixedThreadPool(threads);
        final CountDownLatch latch = new CountDownLatch(threads);
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);

        for (int i = 0; i < threads; i++) {
            final int key = i;
            ex.execute(new Runnable() {
                public void run() {
                    multiMap.put(key, "value");

                    final TransactionContext context = client.newTransactionContext();
                    try {
                        context.beginTransaction();
                        final TransactionalMultiMap txnMultiMap = context.getMultiMap(mapName);
                        txnMultiMap.put(key, "value");
                        txnMultiMap.put(key, "value1");
                        txnMultiMap.put(key, "value2");
                        assertEquals(3, txnMultiMap.get(key).size());
                        context.commitTransaction();

                        assertEquals(3, multiMap.get(key).size());
                    } catch (Exception e) {
                        error.compareAndSet(null, e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            latch.await(1, TimeUnit.MINUTES);
            assertNull(error.get());
        } finally {
            ex.shutdownNow();
        }
    }

    @Test
    public void testPutAndRoleBack() throws Exception {
        final String mapName = randomString();
        final String key = "key";
        final String value = "value";
        final MultiMap multiMap = client.getMultiMap(mapName);

        TransactionContext tx = client.newTransactionContext();
        tx.beginTransaction();
        TransactionalMultiMap mulitMapTxn = tx.getMultiMap(mapName);
        mulitMapTxn.put(key, value);
        mulitMapTxn.put(key, value);
        tx.rollbackTransaction();

        assertEquals(Collections.EMPTY_SET, multiMap.get(key));
    }

    @Test
    public void testSize() throws Exception {
        final String mapName = randomString();
        final String key = "key";
        final String value = "value";
        final MultiMap multiMap = client.getMultiMap(mapName);

        multiMap.put(key, value);

        TransactionContext tx = client.newTransactionContext();
        tx.beginTransaction();
        TransactionalMultiMap mulitMapTxn = tx.getMultiMap(mapName);
        mulitMapTxn.put(key, "newValue");
        mulitMapTxn.put("newKey", value);
        assertEquals(3, mulitMapTxn.size());

        tx.commitTransaction();
    }

    @Test
    public void testCount() throws Exception {
        final String mapName = randomString();
        final String key = "key";
        final String value = "value";
        final MultiMap multiMap = client.getMultiMap(mapName);

        multiMap.put(key, value);

        TransactionContext tx = client.newTransactionContext();
        tx.beginTransaction();
        TransactionalMultiMap mulitMapTxn = tx.getMultiMap(mapName);
        mulitMapTxn.put(key, "newValue");

        assertEquals(2, mulitMapTxn.valueCount(key));

        tx.commitTransaction();
    }

    @Test
    public void testGet_whenBackedWithList() throws Exception {
        final String mapName = multiMapBackedByList+randomString();

        final String key = "key";
        final String value = "value";

        final MultiMap multiMap = server.getMultiMap(mapName);

        multiMap.put(key, value);

        TransactionContext tx = client.newTransactionContext();
        tx.beginTransaction();
        TransactionalMultiMap mulitMapTxn = tx.getMultiMap(mapName);
        Collection c = mulitMapTxn.get(key);
        assertFalse(c.isEmpty());
        tx.commitTransaction();
    }

    @Test
    public void testRemove_whenBackedWithList() throws Exception {
        final String mapName = multiMapBackedByList+randomString();

        final String key = "key";
        final String value = "value";

        final MultiMap multiMap = server.getMultiMap(mapName);

        multiMap.put(key, value);

        TransactionContext tx = client.newTransactionContext();
        tx.beginTransaction();
        TransactionalMultiMap mulitMapTxn = tx.getMultiMap(mapName);
        Collection c = mulitMapTxn.remove(key);
        assertFalse(c.isEmpty());
        tx.commitTransaction();
    }
}
