/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.map.MapTransactionStressTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import java.util.Collection;
import java.util.concurrent.locks.LockSupport;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MultiMapTransactionStressTest extends HazelcastTestSupport {

    private static String DUMMY_TX_SERVICE = "dummy-tx-service";

    @Test
    public void testTransactionAtomicity_whenMultiMapGetIsUsed_withTransaction() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(createConfigWithDummyTxService());
        final String name = HazelcastTestSupport.generateRandomString(5);
        Thread producerThread = startProducerThread(hz, name);
        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 1000; i++) {
                String id = q.poll();
                if (id != null) {
                    TransactionContext tx = hz.newTransactionContext();
                    try {
                        tx.beginTransaction();
                        TransactionalMultiMap<Object, Object> multiMap = tx.getMultiMap(name);
                        Collection<Object> values = multiMap.get(id);
                        assertFalse(values.isEmpty());
                        multiMap.remove(id);
                        tx.commitTransaction();
                    } catch (TransactionException e) {
                        tx.rollbackTransaction();
                        e.printStackTrace();
                    }
                } else {
                    LockSupport.parkNanos(100);
                }
            }
        } finally {
            stopProducerThread(producerThread);
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapGetIsUsed_withoutTransaction() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(createConfigWithDummyTxService());
        final String name = HazelcastTestSupport.generateRandomString(5);
        Thread producerThread = startProducerThread(hz, name);
        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 1000; i++) {
                String id = q.poll();
                if (id != null) {
                    MultiMap<Object, Object> multiMap = hz.getMultiMap(name);
                    Collection<Object> values = multiMap.get(id);
                    assertFalse(values.isEmpty());
                    multiMap.remove(id);
                } else {
                    LockSupport.parkNanos(100);
                }
            }
        } finally {
            stopProducerThread(producerThread);
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapContainsKeyIsUsed_withoutTransaction() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(createConfigWithDummyTxService());
        final String name = HazelcastTestSupport.generateRandomString(5);
        Thread producerThread = startProducerThread(hz, name);
        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 1000; i++) {
                String id = q.poll();
                if (id != null) {
                    MultiMap<Object, Object> multiMap = hz.getMultiMap(name);
                    assertTrue(multiMap.containsKey(id));
                    multiMap.remove(id);
                } else {
                    LockSupport.parkNanos(100);
                }
            }
        } finally {
            stopProducerThread(producerThread);
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapContainsEntryIsUsed_withoutTransaction() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(createConfigWithDummyTxService());
        final String name = HazelcastTestSupport.generateRandomString(5);
        Thread producerThread = startProducerThread(hz, name);
        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 1000; i++) {
                String id = q.poll();
                if (id != null) {
                    MultiMap<Object, Object> multiMap = hz.getMultiMap(name);
                    assertTrue(multiMap.containsEntry(id, MapTransactionStressTest.ProducerThread.value));
                    multiMap.remove(id);
                } else {
                    LockSupport.parkNanos(100);
                }
            }
        } finally {
            stopProducerThread(producerThread);
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapValueCountIsUsed_withoutTransaction() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(createConfigWithDummyTxService());
        final String name = HazelcastTestSupport.generateRandomString(5);
        Thread producerThread = startProducerThread(hz, name);
        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 1000; i++) {
                String id = q.poll();
                if (id != null) {
                    MultiMap<Object, Object> multiMap = hz.getMultiMap(name);
                    assertEquals(1, multiMap.valueCount(id));
                    multiMap.remove(id);
                } else {
                    LockSupport.parkNanos(100);
                }
            }
        } finally {
            stopProducerThread(producerThread);
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapValueCountIsUsed_withTransaction() throws InterruptedException {
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(createConfigWithDummyTxService());
        final String name = HazelcastTestSupport.generateRandomString(5);
        Thread producerThread = startProducerThread(hz, name);
        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 1000; i++) {
                String id = q.poll();
                if (id != null) {
                    TransactionContext tx = hz.newTransactionContext();
                    try {
                        tx.beginTransaction();
                        TransactionalMultiMap<Object, Object> multiMap = tx.getMultiMap(name);
                        assertEquals(1, multiMap.valueCount(id));
                        multiMap.remove(id);
                        tx.commitTransaction();
                    } catch (TransactionException e) {
                        tx.rollbackTransaction();
                        e.printStackTrace();
                    }
                } else {
                    LockSupport.parkNanos(100);
                }
            }
        } finally {
            stopProducerThread(producerThread);
        }
    }

    private Config createConfigWithDummyTxService() {
        Config config = new Config();
        ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(new ServiceConfig().setName(DUMMY_TX_SERVICE)
                .setEnabled(true).setServiceImpl(new MapTransactionStressTest.DummyTransactionalService(DUMMY_TX_SERVICE)));
        return config;
    }

    private Thread startProducerThread(HazelcastInstance hz, String name) {
        Thread producerThread = new MapTransactionStressTest.ProducerThread(hz, name, DUMMY_TX_SERVICE);
        producerThread.start();
        return producerThread;
    }

    private void stopProducerThread(Thread producerThread) throws InterruptedException {
        producerThread.interrupt();
        producerThread.join(10000);
    }

}
