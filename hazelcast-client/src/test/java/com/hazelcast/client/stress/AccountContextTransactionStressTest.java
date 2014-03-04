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

    private IMap<Long, TransferRecord> processed;
    private IMap<Long, FailedTransferRecord> failed;
    private IMap<Integer, Account> accounts;


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

        processed = hz.getMap(PROCESED_TRANS_MAP);
        failed = hz.getMap(FAILED_TRANS_MAP);
        accounts = hz.getMap(ACCOUNTS_MAP);

        for ( int i=0; i < MAX_ACCOUNTS; i++ ) {
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

        for(StressThread s: stressThreads){
            s.instance.shutdown();
        }
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

        long total=0;
        for(Account a : accounts.values()){
            total += a.getBalance();
        }

        int roleBacks=0;
        for(StressThread s : stressThreads){
            roleBacks += s.roleBacksTrigered;
        }

        System.out.println( "==>> procesed tnx "+processed.size()+" failed tnx "+failed.size());

        assertEquals("number of role Backs triggered and failed transaction count not equal", roleBacks, failed.size());

        assertEquals("concurrent transfers caused system total value gain/loss", TOTAL_VALUE, total);
    }


    public class StressThread extends TestThread{

        private HazelcastInstance instance;
        private IMap<Integer, Account> accounts;

        public int roleBacksTrigered=0;

        public StressThread(HazelcastInstance node){

            instance = node;
            accounts = instance.getMap(ACCOUNTS_MAP);
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

            try {
                if ( accounts.tryLock(fromAccountNumber, 50, TimeUnit.MILLISECONDS) ) {

                    if ( accounts.tryLock(toAccountNumber, 50, TimeUnit.MILLISECONDS) ) {

                        TransactionContext context = instance.newTransactionContext();
                        context.beginTransaction();

                        try{
                            TransactionalMap<Integer, Account> accountsContext = context.getMap(ACCOUNTS_MAP);
                            TransactionalMap<Long, TransferRecord> transactionsContext = context.getMap(PROCESED_TRANS_MAP);

                            Account a = accountsContext.get(fromAccountNumber);
                            Account b = accountsContext.get(toAccountNumber);

                            TransferRecord  record = a.transferTo(b, amount);

                            transactionsContext.put(record.getId(), record);

                            accountsContext.put(a.getAcountNumber(), a);
                            accountsContext.put(b.getAcountNumber(), b);

                            trigerRoleBack_atRandom();

                            context.commitTransaction();

                        }catch(Exception e){
                            context.rollbackTransaction();

                            FailedTransferRecord trace = new FailedTransferRecord(fromAccountNumber, toAccountNumber, amount);
                            failed.put(trace.getId(), trace);
                        }

                        accounts.unlock(toAccountNumber);
                    }

                    accounts.unlock(fromAccountNumber);
                }
            } catch (InterruptedException e) {}
        }

        private void trigerRoleBack_atRandom() throws Exception{
            if(random.nextInt(100)==0){
                roleBacksTrigered++;
                throw new Exception("Random Test Exception");
            }
        }
    }

}
