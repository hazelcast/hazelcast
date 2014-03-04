package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.stress.helpers.Account;
import com.hazelcast.client.stress.helpers.FailedTransferRecord;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

/**
 * This test uses map key locks to lock the 2 accounts needed for a transfer transaction, and also uses a
 * TransactionContext to do roleback when a random test exception is thrown. we assert there there are no
 * concurrent transfer using the same account,  by checking the total value of the accounts is the same initial value
 * after we have shuffeled the balances between the acounts,
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AccountContextTransactionStressTest extends StressTestSupport {

    public static final String ACCOUNTS_MAP = "ACOUNTS";
    public static final String PROCESED_TRANS_MAP ="PROCESSED";
    public static final String FAILED_TRANS_MAP ="FAIL";


    public static final int TOTAL_HZ_CLIENT_INSTANCES = 3;
    public static final int THREADS_PER_INSTANCE = 5;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_CLIENT_INSTANCES * THREADS_PER_INSTANCE];

    protected static final int MAX_ACCOUNTS = 200;
    protected static final long INITIAL_VALUE = 0;
    protected static final long TOTAL_VALUE = INITIAL_VALUE * MAX_ACCOUNTS;
    protected static final int MAX_TRANSFER_VALUE = 100;


    @Before
    public void setUp() {
        super.setUp();

        HazelcastInstance hz = cluster.getRandomNode();

        IMap<Integer, Account> accounts = hz.getMap(ACCOUNTS_MAP);
        for ( int i=0; i< MAX_ACCOUNTS; i++ ) {
            Account a = new Account(i, INITIAL_VALUE);
            accounts.put(a.getAcountNumber(), a);
        }

        int index=0;
        for ( int i = 0; i < TOTAL_HZ_CLIENT_INSTANCES; i++ ) {

            HazelcastInstance instance = HazelcastClient.newHazelcastClient();

            for ( int j = 0; j < THREADS_PER_INSTANCE; j++ ) {

                StressThread t = new StressThread(instance);
                t.start();
                stressThreads[index++] = t;
            }
        }
    }


    @After
    public void tearDown() {
        super.tearDown();
    }

    //@Test
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
        IMap<Long, TransferRecord> processed = hz.getMap(PROCESED_TRANS_MAP);
        IMap<Long, FailedTransferRecord> failed = hz.getMap(FAILED_TRANS_MAP);

        long total=0;
        for(Account a : accounts.values()){
            total += a.getBalance();
        }

        List<TransferRecord> transactions = new ArrayList( processed.values() );
        Collections.sort(transactions, new TransferRecord.Comparator());

        for ( TransferRecord t : transactions ) {
            System.out.println(t);
        }

        List<FailedTransferRecord> failedList = new ArrayList( failed.values() );
        Collections.sort(failedList, new FailedTransferRecord.Comparator());

        for ( FailedTransferRecord f : failedList ) {
            System.out.println(f);
        }

        System.out.println( "==>> procesed tnx "+processed.size()+" failed tnx "+failed.size());

        assertEquals("concurrent transfers caused system total value gain/loss", TOTAL_VALUE, total);
    }


    public class StressThread extends TestThread{

        private HazelcastInstance instance;

        public StressThread(HazelcastInstance node){

            instance = node;
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                long amount = random.nextInt(MAX_TRANSFER_VALUE)+1;
                int from = 0;
                int to = 0;

                while ( from == to ) {
                    from = random.nextInt(MAX_ACCOUNTS);
                    to = random.nextInt(MAX_ACCOUNTS);
                }

                transferInContext(from, to, amount);
            }
        }


        private void transferInContext(int fromAccountNumber, int toAccountNumber, long amount){

            IMap<Integer, Account> accounts = instance.getMap(ACCOUNTS_MAP);
            IMap<Long, TransferRecord> transactions = instance.getMap(PROCESED_TRANS_MAP);

            try {
                String errorReason = "Time Out";

                if ( accounts.tryLock(fromAccountNumber, 50, TimeUnit.MILLISECONDS) ) {

                    if ( accounts.tryLock(toAccountNumber, 50, TimeUnit.MILLISECONDS) ) {
                        errorReason=null;


                        TransactionContext context = instance.newTransactionContext();
                        context.beginTransaction();

                        try{
                            TransactionalMap<Integer, Account> accountsTnx = context.getMap(ACCOUNTS_MAP);

                            Account a = accountsTnx.get(fromAccountNumber);
                            Account b = accountsTnx.get(toAccountNumber);

                            TransferRecord  record = a.transferTo(b, amount);

                            TransactionalMap<Long, TransferRecord> transactionsTnx = context.getMap(PROCESED_TRANS_MAP);
                            transactionsTnx.put(record.getId(), record);

                            accountsTnx.put(a.getAcountNumber(), a);
                            accountsTnx.put(b.getAcountNumber(), b);

                            throwExceptionAtRandom();

                            context.commitTransaction();

                        }catch(Exception e){
                            context.rollbackTransaction();
                            errorReason = "Role Back "+e.getMessage();
                        }

                        accounts.unlock(toAccountNumber);
                    }

                    accounts.unlock(fromAccountNumber);
                }

                if(errorReason != null){
                    IMap failed = instance.getMap(FAILED_TRANS_MAP);
                    FailedTransferRecord trace = new FailedTransferRecord(fromAccountNumber, toAccountNumber, amount);
                    trace.setReason(errorReason);
                    failed.put(trace.getId(), trace);
                }

            } catch (InterruptedException e) {}
        }

        private void throwExceptionAtRandom() throws Exception{
            if(random.nextInt(100)==0){
                throw new Exception("Random Test Exception");
            }
        }
    }

}
