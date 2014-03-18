package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
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


}
