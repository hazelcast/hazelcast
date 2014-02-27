package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.Acount;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TransferRecord;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;

/**
 * This tests verifies that map updates are not lost. So we have a client which is going to do updates on a map
 * and in the end we verify that the actual updates in the map, are the same as the expected updates.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PlayStressTest extends StressTestSupport {

    public static int TOTAL_HZ_INSTANCES = 5;
    public static int THREADS_PER_INSTANCE = 2;

    private Transactor[] stressThreads = new Transactor[TOTAL_HZ_INSTANCES * THREADS_PER_INSTANCE];

    protected int MAZ_ACOUNTS = 1000;
    private IMap<Integer, Acount> acounts=null;

    @Before
    public void setUp() {
        super.setUp();

        HazelcastInstance hz = super.cluster.getRandomNode();

        acounts = hz.getMap("A");

        for(int i=0; i<MAZ_ACOUNTS; i++){
            Acount a = new Acount(i, 100.0);
            acounts.put(a.getAcountNumber(), a);
        }


        int index=0;
        for (int i = 0; i < TOTAL_HZ_INSTANCES; i++) {

            HazelcastInstance instance = HazelcastClient.newHazelcastClient(new ClientConfig());

            for (int j = 0; j < THREADS_PER_INSTANCE; j++) {

                Transactor t = new Transactor(instance);
                t.start();
                stressThreads[index++] = t;
            }
        }
    }


    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testChangingCluster() {
        runTest(true, stressThreads);
    }

    @Test
    public void testFixedCluster() {
        runTest(false, stressThreads);
    }

    public void assertResult() {

        HazelcastInstance hz = super.cluster.getRandomNode();

        IMap trace = hz.getMap("T");

        Acount a = acounts.get(1);
        Acount b = acounts.get(2);

        System.out.println( "===>>> "+ a.getBalance() +" and "+ b.getBalance()+" total transactions "+trace.size());
    }


    public class Transactor extends TestThread{

        private HazelcastInstance instance;

        private IMap<Integer, Acount> acounts;
        private IMap<Long, TransferRecord> transactions;

        public Transactor(HazelcastInstance node){
            super();

            instance = node;

            acounts = instance.getMap("A");
            transactions = instance.getMap("T");
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                int from = 1;
                int to = 2;
                double amount = 10;

                lockTransfer(from, to, amount);
                //contexTransfer(from, to, amount);
            }
        }

        private void lockTransfer(int a, int b, double amount){
            acounts.lock(a);
            Acount from = acounts.get(a);

            acounts.lock(b);
            Acount to = acounts.get(b);

            TransferRecord tr = from.transferTo(to, amount);
            transactions.put(tr.getId(), tr);

            acounts.put(to.getAcountNumber(), to);
            acounts.put(from.getAcountNumber(), from);

            acounts.unlock(to.getAcountNumber());
            acounts.unlock(from.getAcountNumber());
        }

        private void contexTransfer(int a, int b, double amount){

            TransactionContext context = instance.newTransactionContext();
            context.beginTransaction();

            TransactionalMap<Integer, Acount> acounts = context.getMap("A");

            Acount from = acounts.get(a);
            Acount to = acounts.get(b);

            TransferRecord tr = from.transferTo(to, amount);
            transactions.put(tr.getId(), tr);

            acounts.put(to.getAcountNumber(), to);
            acounts.put(from.getAcountNumber(), from);

            context.commitTransaction();
        }
    }


    public class Depositor extends TestThread{

        private HazelcastInstance instance;
        private IMap<Integer, Acount> acounts;

        public Depositor(HazelcastInstance node){
            super();

            instance = node;
            acounts = instance.getMap("A");
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                int acountNumber = random.nextInt(MAZ_ACOUNTS);

                acounts.lock(acountNumber);

                Acount acount = acounts.get(acountNumber);
                acount.increase(10);
                acounts.put(acount.getAcountNumber(), acount);

                acounts.unlock(acount.getAcountNumber());

            }
        }
    }

}
