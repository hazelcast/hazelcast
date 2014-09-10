package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import com.hazelcast.transaction.impl.TransactionLog;
import com.hazelcast.transaction.impl.TransactionSupport;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapTransactionStressTest extends HazelcastTestSupport {

    @Test
    public void testTransactionAtomicity_whenMapGetIsUsed() throws InterruptedException {
        Config config = new Config();
        ServicesConfig servicesConfig = config.getServicesConfig();

        final String dummyTxService = "dummy-tx-service";
        servicesConfig.addServiceConfig(new ServiceConfig().setName(dummyTxService)
                .setEnabled(true).setServiceImpl(new DummyTransactionalService(dummyTxService)));

        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        final String name = HazelcastTestSupport.generateRandomString(5);

        Thread producerThread = new ProducerThread(hz, name, dummyTxService);
        producerThread.start();

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
            producerThread.interrupt();
            producerThread.join(10000);
        }

    }

    @Test
    public void testTransactionAtomicity_whenMapContainsKeyIsUsed() throws InterruptedException {
        Config config = new Config();
        ServicesConfig servicesConfig = config.getServicesConfig();

        final String dummyTxService = "dummy-tx-service";
        servicesConfig.addServiceConfig(new ServiceConfig().setName(dummyTxService)
                .setEnabled(true).setServiceImpl(new DummyTransactionalService(dummyTxService)));

        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        final String name = HazelcastTestSupport.generateRandomString(5);

        Thread producerThread = new ProducerThread(hz, name, dummyTxService);
        producerThread.start();

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
            producerThread.interrupt();
            producerThread.join(10000);
        }

    }

    private static class ProducerThread extends Thread {
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
                    map.put(id, "some-value");

                    tx.commitTransaction();
                } catch (TransactionException e) {
                    tx.rollbackTransaction();
                    e.printStackTrace();
                }
            }
        }
    }

    private static class DummyTransactionalService implements TransactionalService, RemoteService {

        final String serviceName;

        DummyTransactionalService(String name) {
            this.serviceName = name;
        }

        @Override
        public TransactionalObject createTransactionalObject(String name,
                                                             TransactionSupport transaction) {
            return new DummyTransactionalObject(serviceName, name, transaction);
        }

        @Override
        public void rollbackTransaction(String transactionId) {
        }

        @Override
        public DistributedObject createDistributedObject(String objectName) {
            return new DummyTransactionalObject(serviceName, objectName, null);
        }

        @Override
        public void destroyDistributedObject(String objectName) {
        }
    }

    private static class DummyTransactionalObject implements TransactionalObject {

        final String serviceName;
        final String name;
        final TransactionSupport transaction;

        DummyTransactionalObject(String serviceName, String name, TransactionSupport transaction) {
            this.serviceName = serviceName;
            this.name = name;
            this.transaction = transaction;
        }

        public void doSomethingTxnal() {
            transaction.addTransactionLog(new SleepyTransactionLog());
        }

        @Override
        public Object getId() {
            return name;
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

    private static class SleepyTransactionLog implements TransactionLog {
        @Override
        public Future prepare(NodeEngine nodeEngine) {
            return new EmptyFuture();
        }

        @Override
        public Future commit(NodeEngine nodeEngine) {
            LockSupport.parkNanos(10000);
            return new EmptyFuture();
        }

        @Override
        public Future rollback(NodeEngine nodeEngine) {
            return new EmptyFuture();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    private static class EmptyFuture implements Future {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }


    @Test
    public void testTxnGetForUpdateAndIncrementStressTest() throws TransactionException, InterruptedException {
        Config config = new Config();
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

    static class TxnIncrementor implements Runnable {
        int count = 0;
        HazelcastInstance instance;
        final String key = "count";
        final CountDownLatch latch;


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
