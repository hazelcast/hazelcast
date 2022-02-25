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

package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IQueue;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.map.impl.tx.MapTransactionStressTest.DummyTransactionalService;
import com.hazelcast.map.impl.tx.MapTransactionStressTest.ProducerThread;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MultiMapTransactionStressTest extends HazelcastTestSupport {

    private String name;
    private HazelcastInstance hz;
    private Thread producerThread;

    @Before
    public void setUp() {
        String dummyTxService = "dummy-tx-service";
        ServiceConfig serviceConfig = new ServiceConfig()
                .setName(dummyTxService)
                .setEnabled(true)
                .setImplementation(new DummyTransactionalService(dummyTxService));

        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(serviceConfig);

        hz = createHazelcastInstance(config);
        name = randomMapName();

        producerThread = new ProducerThread(hz, name, dummyTxService);
        producerThread.start();
    }

    @After
    public void tearDown() {
        producerThread.interrupt();
        assertJoinable(producerThread);
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapGetIsUsed_withTransaction() {
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
                parkNanos(100);
            }
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapGetIsUsed_withoutTransaction() {
        IQueue<String> q = hz.getQueue(name);
        for (int i = 0; i < 1000; i++) {
            String id = q.poll();
            if (id != null) {
                MultiMap<Object, Object> multiMap = hz.getMultiMap(name);
                Collection<Object> values = multiMap.get(id);
                assertFalse(values.isEmpty());
                multiMap.remove(id);
            } else {
                parkNanos(100);
            }
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapContainsKeyIsUsed_withoutTransaction() {
        IQueue<String> q = hz.getQueue(name);
        for (int i = 0; i < 1000; i++) {
            String id = q.poll();
            if (id != null) {
                MultiMap<Object, Object> multiMap = hz.getMultiMap(name);
                assertTrue(multiMap.containsKey(id));
                multiMap.remove(id);
            } else {
                parkNanos(100);
            }
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapContainsEntryIsUsed_withoutTransaction() {
        IQueue<String> q = hz.getQueue(name);
        for (int i = 0; i < 1000; i++) {
            String id = q.poll();
            if (id != null) {
                MultiMap<Object, Object> multiMap = hz.getMultiMap(name);
                assertTrue(multiMap.containsEntry(id, ProducerThread.VALUE));
                multiMap.remove(id);
            } else {
                parkNanos(100);
            }
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapValueCountIsUsed_withoutTransaction() {
        IQueue<String> q = hz.getQueue(name);
        for (int i = 0; i < 1000; i++) {
            String id = q.poll();
            if (id != null) {
                MultiMap<Object, Object> multiMap = hz.getMultiMap(name);
                assertEquals(1, multiMap.valueCount(id));
                multiMap.remove(id);
            } else {
                parkNanos(100);
            }
        }
    }

    @Test
    public void testTransactionAtomicity_whenMultiMapValueCountIsUsed_withTransaction() {
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
                parkNanos(100);
            }
        }
    }
}
