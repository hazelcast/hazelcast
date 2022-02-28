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

package com.hazelcast.map.impl.tx;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.internal.config.ServicesConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.collection.IQueue;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionLogRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapTransactionStressTest extends HazelcastTestSupport {

    private static final String DUMMY_TX_SERVICE = "dummy-tx-service";

    @Test
    public void testTransactionAtomicity_whenMapGetIsUsed_withTransaction() throws InterruptedException {
        final HazelcastInstance hz = createHazelcastInstance(createConfigWithDummyTxService());
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
                        TransactionalMap<String, Object> map = tx.getMap(name);
                        Object value = map.get(id);
                        Assert.assertNotNull(value);
                        map.delete(id);
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
    public void testTransactionAtomicity_whenMapGetIsUsed_withoutTransaction() throws InterruptedException {
        final HazelcastInstance hz = createHazelcastInstance(createConfigWithDummyTxService());
        final String name = HazelcastTestSupport.generateRandomString(5);
        Thread producerThread = startProducerThread(hz, name);
        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 1000; i++) {
                String id = q.poll();
                if (id != null) {
                    IMap<Object, Object> map = hz.getMap(name);
                    Object value = map.get(id);
                    Assert.assertNotNull(value);
                    map.delete(id);
                } else {
                    LockSupport.parkNanos(100);
                }
            }
        } finally {
            stopProducerThread(producerThread);
        }
    }

    @Test
    public void testTransactionAtomicity_whenMapContainsKeyIsUsed_withTransaction() throws InterruptedException {
        final HazelcastInstance hz = createHazelcastInstance(createConfigWithDummyTxService());
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
                        TransactionalMap<String, Object> map = tx.getMap(name);
                        assertTrue(map.containsKey(id));
                        map.delete(id);
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
    public void testTransactionAtomicity_whenMapContainsKeyIsUsed_withoutTransaction() throws InterruptedException {
        final HazelcastInstance hz = createHazelcastInstance(createConfigWithDummyTxService());
        final String name = HazelcastTestSupport.generateRandomString(5);
        Thread producerThread = startProducerThread(hz, name);
        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 1000; i++) {
                String id = q.poll();
                if (id != null) {
                    IMap<Object, Object> map = hz.getMap(name);
                    assertTrue(map.containsKey(id));
                    map.delete(id);
                } else {
                    LockSupport.parkNanos(100);
                }
            }
        } finally {
            stopProducerThread(producerThread);
        }
    }

    @Test
    public void testTransactionAtomicity_whenMapGetEntryViewIsUsed_withoutTransaction() throws InterruptedException {
        final HazelcastInstance hz = createHazelcastInstance(createConfigWithDummyTxService());
        final String name = HazelcastTestSupport.generateRandomString(5);
        Thread producerThread = startProducerThread(hz, name);
        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 1000; i++) {
                String id = q.poll();
                if (id != null) {
                    IMap<Object, Object> map = hz.getMap(name);
                    assertNotNull(map.getEntryView(id));
                    map.delete(id);
                } else {
                    LockSupport.parkNanos(100);
                }
            }
        } finally {
            stopProducerThread(producerThread);
        }
    }


    private Config createConfigWithDummyTxService() {
        Config config = getConfig();
        ServicesConfig servicesConfig = ConfigAccessor.getServicesConfig(config);
        servicesConfig.addServiceConfig(new ServiceConfig().setName(DUMMY_TX_SERVICE)
                .setEnabled(true).setImplementation(new DummyTransactionalService(DUMMY_TX_SERVICE)));
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

    @Test
    public void testTxnGetForUpdateAndIncrementStressTest() throws TransactionException, InterruptedException {
        Config config = getConfig();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap<String, Integer> map = h2.getMap("default");
        final String key = "count";
        int count1 = 13000;
        int count2 = 15000;
        final CountDownLatch latch = new CountDownLatch(count1 + count2);
        map.put(key, 0);
        new Thread(new TxnIncrementor(count1, h1, latch)).start();
        new Thread(new TxnIncrementor(count2, h2, latch)).start();
        latch.await(600, TimeUnit.SECONDS);
        assertEquals(new Integer(count1 + count2), map.get(key));
    }

    public static class ProducerThread extends Thread {

        public static final String VALUE = "some-value";

        private final HazelcastInstance hz;
        private final String name;
        private final String dummyServiceName;

        public ProducerThread(HazelcastInstance hz, String name, String dummyServiceName) {
            this.hz = hz;
            this.name = name;
            this.dummyServiceName = dummyServiceName;
        }

        public void run() {
            while (!isInterrupted()) {
                TransactionContext tx = hz.newTransactionContext();
                try {
                    tx.beginTransaction();
                    String id = UUID.randomUUID().toString();
                    TransactionalQueue<String> q = tx.getQueue(name);
                    q.offer(id);
                    DummyTransactionalObject slowTxObject = tx.getTransactionalObject(dummyServiceName, name);
                    slowTxObject.doSomethingTxnal();
                    TransactionalMap<String, Object> map = tx.getMap(name);
                    map.put(id, VALUE);
                    slowTxObject.doSomethingTxnal();
                    TransactionalMultiMap<Object, Object> multiMap = tx.getMultiMap(name);
                    multiMap.put(id, VALUE);
                    tx.commitTransaction();
                } catch (TransactionException e) {
                    tx.rollbackTransaction();
                    e.printStackTrace();
                }
            }
        }
    }

    public static class DummyTransactionalService implements TransactionalService, RemoteService {

        final String serviceName;

        public DummyTransactionalService(String name) {
            this.serviceName = name;
        }

        @Override
        public TransactionalObject createTransactionalObject(String name,
                                                             Transaction transaction) {
            return new DummyTransactionalObject(serviceName, name, transaction);
        }

        @Override
        public void rollbackTransaction(UUID transactionId) {
        }

        @Override
        public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
            return new DummyTransactionalObject(serviceName, objectName, null);
        }

        @Override
        public void destroyDistributedObject(String objectName, boolean local) {
        }
    }

    public static class DummyTransactionalObject implements TransactionalObject {

        final String serviceName;
        final String name;
        final Transaction transaction;

        DummyTransactionalObject(String serviceName, String name, Transaction transaction) {
            this.serviceName = serviceName;
            this.name = name;
            this.transaction = transaction;
        }

        public void doSomethingTxnal() {
            transaction.add(new SleepyTransactionLogRecord());
        }

        @Override
        public String getPartitionKey() {
            return null;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getServiceName() {
            return serviceName;
        }

        @Override
        public void destroy() {

        }
    }

    public static class SleepyTransactionLogRecord implements TransactionLogRecord {
        @Override
        public Object getKey() {
            return null;
        }

        @Override
        public Operation newPrepareOperation() {
            return newEmptyOperation();
        }

        @Override
        public Operation newCommitOperation() {
            return new Operation() {
                {
                    setPartitionId(0);
                }

                @Override
                public void run() throws Exception {
                    LockSupport.parkNanos(10000);
                }
            };
        }

        @Override
        public Operation newRollbackOperation() {
            return newEmptyOperation();
        }

        private Operation newEmptyOperation() {
            return new Operation() {
                {
                    setPartitionId(0);
                }

                @Override
                public void run() throws Exception {
                }
            };
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public String toString() {
            return "SleepyTransactionLogRecord{}";
        }

        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }

    static class TxnIncrementor implements Runnable {
        final String key = "count";
        final CountDownLatch latch;
        int count = 0;
        HazelcastInstance instance;


        TxnIncrementor(int count, HazelcastInstance instance, CountDownLatch latch) {
            this.count = count;
            this.instance = instance;
            this.latch = latch;
        }

        @Override
        public void run() {
            for (int i = 0; i < count; i++) {
                instance.executeTransaction(new TransactionalTask<Boolean>() {
                    public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                        final TransactionalMap<String, Integer> txMap = context.getMap("default");
                        Integer value = txMap.getForUpdate(key);
                        txMap.put(key, value + 1);
                        return true;
                    }
                });
                latch.countDown();
            }
        }
    }


}
