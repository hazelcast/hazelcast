package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.Account;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TransferRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
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
public class TransactionContextAccountStressTest extends StressTestSupport {

    public static final String ACCOUNTS_MAP = "ACOUNTS";
    public static final String PROCESED_TRANS_MAP ="PROCESSED";
    public static final String FAILED_TRANS_MAP ="FAIL";


    public static final int TOTAL_HZ_INSTANCES = 2;
    public static final int THREADS_PER_INSTANCE = 2;

    private Transactor[] stressThreads = new Transactor[TOTAL_HZ_INSTANCES * THREADS_PER_INSTANCE];

    protected static final int MAX_ACCOUNTS = 100;
    protected static final long INITIAL_VALUE = 100;
    protected static final long TOTAL_VALUE = INITIAL_VALUE * MAX_ACCOUNTS;


    @Before
    public void setUp() {
        super.setUp();

        HazelcastInstance hz = super.cluster.getRandomNode();

        IMap<Integer, Account> accounts = hz.getMap(ACCOUNTS_MAP);
        for ( int i=0; i< MAX_ACCOUNTS; i++ ) {
            Account a = new Account(i, INITIAL_VALUE);
            accounts.put(a.getAcountNumber(), a);
        }


        int index=0;
        for ( int i = 0; i < TOTAL_HZ_INSTANCES; i++ ) {

            HazelcastInstance instance = HazelcastClient.newHazelcastClient();

            for ( int j = 0; j < THREADS_PER_INSTANCE; j++ ) {

                Transactor t = new Transactor(instance);
                t.start();
                stressThreads[index++] = t;
            }
        }

    }


    @After
    public void tearDown() {
        //super.tearDown();
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

        IMap<Integer, Account> accounts = hz.getMap(ACCOUNTS_MAP);
        IMap processed = hz.getMap(PROCESED_TRANS_MAP);
        IMap failed = hz.getMap(FAILED_TRANS_MAP);

        System.out.println( "==>> procesed tnx "+processed.size()+" failed tnx "+failed.size());

        long total=0;
        for(Account a : accounts.values()){
            System.out.println("==>> bal "+a.getBalance());
            total += a.getBalance();
        }

        assertEquals("", TOTAL_VALUE, total);
    }


    public class Transactor extends TestThread{

        private HazelcastInstance instance;

        public Transactor(HazelcastInstance node){
            super();

            instance = node;
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                int from = 0, to = 0;

                while ( from == to ) {
                    from = random.nextInt(MAX_ACCOUNTS);
                    to = random.nextInt(MAX_ACCOUNTS);
                }

                long amount = 1; //random.nextInt(100)+1;

                contextTransfer(from, to, amount);
            }
        }


        private void contextTransfer(int fromAccountNumber, int toAccountNumber, long amount){

            TransactionContext context = instance.newTransactionContext();
            context.beginTransaction();

            try{
                TransactionalMap<Integer, Account> accounts = context.getMap(ACCOUNTS_MAP);

                Account a = accounts.get(fromAccountNumber);
                Account b = accounts.get(toAccountNumber);

                TransferRecord  record = a.transferTo(b, amount);

                TransactionalMap<Long, TransferRecord> transactions = context.getMap(PROCESED_TRANS_MAP);
                transactions.put(record.getId(), record);

                accounts.put(a.getAcountNumber(), a);
                accounts.put(b.getAcountNumber(), b);

                //throwExceptionAtRandom();

                context.commitTransaction();

            }catch(Exception e){
                context.rollbackTransaction();

                IMap<Long, TransferRecord> failed = instance.getMap(FAILED_TRANS_MAP);
                TransferRecord rt = createFailedTransferRecored(fromAccountNumber, toAccountNumber, amount);
                failed.put(rt.getId(), rt);
            }
        }

        private TransferRecord createFailedTransferRecored(int from, int to, long amount){

            IMap<Integer, Account> accounts = instance.getMap(ACCOUNTS_MAP);

            Account a = accounts.get(from);
            Account b = accounts.get(to);

            TransferRecord t = new TransferRecord(a, b, amount);
            t.setReason("Role Back");
            return t;
        }

        private void throwExceptionAtRandom() throws Exception{
            if(random.nextInt(100)==0){
                throw new Exception("Random Test Exception");
            }
        }

    }
}
