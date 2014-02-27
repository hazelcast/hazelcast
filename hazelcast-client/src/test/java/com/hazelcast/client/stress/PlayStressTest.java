package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.Account;
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

    public static final String ACCOUNTS_MAP = "ACOUNTS";
    public static final String PROCESED_TRANS_MAP ="PROCESSED";
    public static final String FAILED_TRANS_MAP ="FAIL";


    public static final int TOTAL_HZ_INSTANCES = 1;
    public static final int THREADS_PER_INSTANCE = 5;

    private Transactor[] stressThreads = new Transactor[TOTAL_HZ_INSTANCES * THREADS_PER_INSTANCE];

    protected static final int MAX_ACCOUNTS = 2;
    protected static final long INITIAL_VALUE = 100;
    protected static final long TOTAL_VALUE = INITIAL_VALUE * MAX_ACCOUNTS;

    private IMap<Integer, Account> accounts =null;

    @Before
    public void setUp() {
        super.setUp();

        HazelcastInstance hz = super.cluster.getRandomNode();

        accounts = hz.getMap(ACCOUNTS_MAP);

        for(int i=0; i< MAX_ACCOUNTS; i++){
            Account a = new Account(i, INITIAL_VALUE);
            accounts.put(a.getAcountNumber(), a);
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

        IMap processed = hz.getMap(PROCESED_TRANS_MAP);

        IMap failed = hz.getMap(FAILED_TRANS_MAP);


        System.out.println( "==>> procesed tnx "+processed.size()+" failed tnx "+failed.size());

        long total=0;
        for(Account a : accounts.values()){
            System.out.println("==>> bal "+a.getBalance());
            total += a.getBalance();
        }

        assertEquals(TOTAL_VALUE, total);

    }


    public class Transactor extends TestThread{

        private HazelcastInstance instance;

        private IMap<Integer, Account> accounts;
        private IMap<Long, TransferRecord> transactions;
        private IMap<Long, TransferRecord> failed;

        public Transactor(HazelcastInstance node){
            super();

            instance = node;

            accounts = instance.getMap(ACCOUNTS_MAP);
            transactions = instance.getMap(PROCESED_TRANS_MAP);
            failed = instance.getMap(FAILED_TRANS_MAP);
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                int from = 0; //random.nextInt(MAX_ACCOUNTS);
                int to = 1; //random.nextInt(MAX_ACCOUNTS);
                long amount = 1; //random.nextInt(100)+1;

                //lockTransfer(from, to, amount);
                contextTransfer(from, to, amount);
            }
        }

        private void lockTransfer(int a, int b, long amount){
            accounts.lock(a);
            Account from = accounts.get(a);

            accounts.lock(b);
            Account to = accounts.get(b);

            TransferRecord tr = from.transferTo(to, amount);
            transactions.put(tr.getId(), tr);

            accounts.put(to.getAcountNumber(), to);
            accounts.put(from.getAcountNumber(), from);

            accounts.unlock(to.getAcountNumber());
            accounts.unlock(from.getAcountNumber());
        }


        private void contextTransfer(int fromAccountNumber, int toAccountNumber, long amount){

            try{
                TransactionContext context = instance.newTransactionContext();
                context.beginTransaction();

                TransactionalMap<Integer, Account> accounts = context.getMap(ACCOUNTS_MAP);

                Account from = accounts.get(fromAccountNumber);
                Account to = accounts.get(toAccountNumber);

                try{
                    doTransaction(from, to, amount);

                    context.commitTransaction();

                }catch(Exception e){
                    context.rollbackTransaction();

                    TransferRecord rt = new TransferRecord(from, to, amount);
                    failed.put(rt.getId(), rt);
                }

            }catch (Throwable e) {
                System.err.println("==>> "+e);
            }
        }


        private void doTransaction(Account from, Account to, long amount){

            TransferRecord  record = from.transferTo(to, amount);
            transactions.put(record.getId(), record);

            accounts.put(to.getAcountNumber(), to);
            accounts.put(from.getAcountNumber(), from);

            //throwExceptionAtRandom();
        }


        private void throwExceptionAtRandom() throws Exception{
            if(random.nextInt(10)==0){
                throw new Exception();
            }
        }

    }


    public class Depositor extends TestThread{

        private HazelcastInstance instance;
        private IMap<Integer, Account> acounts;

        public Depositor(HazelcastInstance node){
            super();

            instance = node;
            acounts = instance.getMap("A");
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                int acountNumber = random.nextInt(MAX_ACCOUNTS);

                acounts.lock(acountNumber);

                Account acount = acounts.get(acountNumber);
                acount.increase(10);

                acounts.put(acount.getAcountNumber(), acount);

                acounts.unlock(acount.getAcountNumber());

            }
        }
    }

}
