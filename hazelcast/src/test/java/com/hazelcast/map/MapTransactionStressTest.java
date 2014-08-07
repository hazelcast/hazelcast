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
import com.hazelcast.test.HazelcastSerialClassRunner;
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
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapTransactionStressTest extends HazelcastTestSupport {

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

    /*
     * Too see broken case, make {@link com.hazelcast.map.operation.GetOperation#shouldWait()}
     * to return false always
     */
    @Test
    public void testTransactionAtomicity_whenMapGetIsUsed() throws InterruptedException {
        Config config = new Config();
        ServicesConfig servicesConfig = config.getServicesConfig();

        final String sleepyServiceName = "sleepy-tx-service";
        servicesConfig.addServiceConfig(new ServiceConfig().setName(sleepyServiceName)
                .setEnabled(true).setServiceImpl(new SleepyTransactionalService(sleepyServiceName)));

        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        final String name = HazelcastTestSupport.generateRandomString(5);

        Thread producerThread = new UuidProducerThread(hz, name, sleepyServiceName);
        producerThread.start();

        try {
            IQueue<String> q = hz.getQueue(name);
            for (int i = 0; i < 10000; i++) {
                String id = q.poll();
                if (id != null) {
                    TransactionContext tx = hz.newTransactionContext();
                    try {
                        tx.beginTransaction();
                        TransactionalMap<String, Object> map = tx.getMap(name);
                        Object value = map.get(id);
                        Assert.assertNotNull("Read null value, transaction atomicity is broken!", value);

                        map.delete(id);
                        tx.commitTransaction();
                    } catch (TransactionException e) {
                        tx.rollbackTransaction();
                        e.printStackTrace();
                    }
                } else {
                    LockSupport.parkNanos(10);
                }
            }
        } finally {
            producerThread.interrupt();
            producerThread.join(10000);
        }
    }

    private static class UuidProducerThread extends Thread {
        private final HazelcastInstance hz;
        private final String name;
        private final String sleepyServiceName;

        public UuidProducerThread(HazelcastInstance hz, String name, String sleepyServiceName) {
            this.hz = hz;
            this.name = name;
            this.sleepyServiceName = sleepyServiceName;
        }

        public void run() {
            while (!isInterrupted()) {
                TransactionContext tx = hz.newTransactionContext();
                try {
                    tx.beginTransaction();
                    String id = UUID.randomUUID().toString();

                    TransactionalQueue<String> q = tx.getQueue(name);
                    q.offer(id);

                    // used to add internal delay during tx commit between queue.offer() and map.put()
                    SleepyTransactionalObject sleepyObject = tx.getTransactionalObject(sleepyServiceName, name);
                    sleepyObject.doSleepyTxnalOp();

                    TransactionalMap<String, Object> map = tx.getMap(name);
                    map.put(id, id);

                    tx.commitTransaction();
                } catch (TransactionException e) {
                    tx.rollbackTransaction();
                    e.printStackTrace();
                }
            }
        }
    }

    private static class SleepyTransactionalService implements TransactionalService, RemoteService {

        final String serviceName;

        SleepyTransactionalService(String name) {
            this.serviceName = name;
        }

        @Override
        public TransactionalObject createTransactionalObject(String name,
                TransactionSupport transaction) {
            return new SleepyTransactionalObject(serviceName, name, transaction);
        }

        @Override
        public void rollbackTransaction(String transactionId) {
        }

        @Override
        public DistributedObject createDistributedObject(String objectName) {
            return new SleepyTransactionalObject(serviceName, objectName, null);
        }

        @Override
        public void destroyDistributedObject(String objectName) {
        }
    }

    private static class SleepyTransactionalObject implements TransactionalObject {

        final String serviceName;
        final String name;
        final TransactionSupport transaction;

        SleepyTransactionalObject(String serviceName, String name, TransactionSupport transaction) {
            this.serviceName = serviceName;
            this.name = name;
            this.transaction = transaction;
        }

        public void doSleepyTxnalOp() {
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

    /**
     * Used to add delay to a single commit in a transaction
     */
    private static class SleepyTransactionLog implements TransactionLog {
        @Override
        public Future prepare(NodeEngine nodeEngine) {
            return new EmptyFuture();
        }

        @Override
        public Future commit(NodeEngine nodeEngine) {
            // add some delay to commit
            LockSupport.parkNanos(1000);
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

}
