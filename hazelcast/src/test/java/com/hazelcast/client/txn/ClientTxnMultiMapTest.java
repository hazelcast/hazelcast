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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnMultiMapTest {

    private static final String multiMapBackedByList = "BackedByList*";
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;
    private HazelcastInstance server;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        server = hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
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

        assertTrue(client.getMultiMap(mapName).get(key).isEmpty());
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

        assertTrue(multiMap.get(key).isEmpty());
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
                    } catch (TransactionException e) {
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

        assertEquals(0, multiMap.get(key).size());
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
        final String mapName = multiMapBackedByList + randomString();

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
        final String mapName = multiMapBackedByList + randomString();

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
