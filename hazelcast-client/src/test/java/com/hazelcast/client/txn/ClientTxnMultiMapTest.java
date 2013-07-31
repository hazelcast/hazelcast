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
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * @author ali 6/10/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientTxnMultiMapTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;

    @BeforeClass
    public static void init(){
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
    public void testPutGetRemove() throws Exception {
        final MultiMap mm = hz.getMultiMap(name);
        final int threads = 10;
        final ExecutorService ex = Executors.newFixedThreadPool(threads);
        final CountDownLatch latch = new CountDownLatch(threads);
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);

        for (int i = 0; i < threads; i++) {
            final int finalI = i;
            ex.execute(new Runnable() {
                public void run() {
                    final String key = finalI + "key";
                    hz.getMultiMap(name).put(key, "value");
                    final TransactionContext context = hz.newTransactionContext();
                    try {
                        context.beginTransaction();
                        final TransactionalMultiMap multiMap = context.getMultiMap(name);
                        assertFalse(multiMap.put(key, "value"));
                        assertTrue(multiMap.put(key, "value1"));
                        assertTrue(multiMap.put(key, "value2"));
                        assertEquals(3, multiMap.get(key).size());
                        context.commitTransaction();

                        assertEquals(3, mm.get(key).size());
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
}
