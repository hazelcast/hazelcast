package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.stress.helpers.*;
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
public class AccountContextTransactionStressTest extends StressTestSupport<AccountContextTransactionStressTest.StressThread>{

    public static final String ACCOUNTS_MAP = "ACOUNTS";
    public static final String PROCESED_TRANS_MAP ="PROCESSED";
    public static final String FAILED_TRANS_MAP ="FAIL";

    private IMap<Long, TransferRecord> processed;
    private IMap<Long, FailedTransferRecord> failed;
    private IMap<Integer, Account> accounts;

    //keeping number of accounts low for high Lock contention as we are locking on map keys (account numbers)
    protected static final int MAX_ACCOUNTS = 3000;
    protected static final long INITIAL_VALUE = 100;
    protected static final long TOTAL_VALUE = INITIAL_VALUE * MAX_ACCOUNTS;
    protected static final int MAX_TRANSFER_VALUE = 100;

    @Before
    public void setUp() {
        RUNNING_TIME_SECONDS=10;
        super.setUp(this);

        HazelcastInstance hz = cluster.getRandomNode();

        processed = hz.getMap(PROCESED_TRANS_MAP);
        failed = hz.getMap(FAILED_TRANS_MAP);
        accounts = hz.getMap(ACCOUNTS_MAP);

        for ( int i=0; i < MAX_ACCOUNTS; i++ ) {
            Account a = new Account(i, INITIAL_VALUE);
            accounts.put(a.getAcountNumber(), a);
        }
    }


    @Test
    public void testFixedCluster() {
        runTest(false);
    }

    public void assertResult() {

        long acutalValue=0;
        for(Account a : accounts.values()){
            acutalValue += a.getBalance();
        }

        int expeted_roleBacks=0;
        int expeted_transactionsProcessed=0;

        for(StressThread s : stressThreads){
            expeted_roleBacks += s.roleBacksTriggered;
            expeted_transactionsProcessed += s.transactionsProcessed;
        }

        assertEquals("number of processed transactions not equal expeted", expeted_transactionsProcessed, processed.size());

        assertEquals("number of role Backs triggered and failed transaction count not equal", expeted_roleBacks, failed.size());

        assertEquals("concurrent transfers caused system total value gain/loss", TOTAL_VALUE+1, acutalValue);
    }

    public class StressThread extends TestThread {
        private IMap<Integer, Account> accounts;

        public int transactionsProcessed=0;
        public int roleBacksTriggered=0;

        public StressThread(HazelcastInstance node){
            super(node);
            accounts = instance.getMap(ACCOUNTS_MAP);
        }

        @Override
        public void doRun() throws Exception {

            long amount = random.nextInt(MAX_TRANSFER_VALUE)+1;
            int from = 0;
            int to = 0;

            while ( from == to ) {
                from = random.nextInt(MAX_ACCOUNTS);
                to = random.nextInt(MAX_ACCOUNTS);
            }

            transfer(from, to, amount);
        }

        //this method is responsible to obtaining the lock to do the transaction for the 2 accounts
        //and releasing the locks in the event of any thrown exceptions
        private void transfer(int fromAccountNumber, int toAccountNumber, long amount){
            try {
                if ( accounts.tryLock(fromAccountNumber, 50, TimeUnit.MILLISECONDS) ) {
                    try{
                        if ( accounts.tryLock(toAccountNumber, 50, TimeUnit.MILLISECONDS) ) {
                            try {
                                //now all needed locks are obtained do the transation between acounts
                                transferInTransactionContext(fromAccountNumber, toAccountNumber, amount);

                            }finally {
                                accounts.unlock(toAccountNumber);
                            }
                        }
                    }finally {
                        accounts.unlock(fromAccountNumber);
                    }
                }
            } catch (InterruptedException e) {
            }
        }

        private void transferInTransactionContext(int fromAccountNumber, int toAccountNumber, long amount){

            TransactionContext context = instance.newTransactionContext();
            context.beginTransaction();

            try{
                TransactionalMap<Integer, Account> accountsContext = context.getMap(ACCOUNTS_MAP);
                TransactionalMap<Long, TransferRecord> processedTransactions = context.getMap(PROCESED_TRANS_MAP);

                Account a = accountsContext.get(fromAccountNumber);
                Account b = accountsContext.get(toAccountNumber);

                TransferRecord  record = a.transferTo(b, amount);

                //keep track of all the transactions that have processed correctly
                processedTransactions.put(record.getId(), record);

                accountsContext.put(a.getAcountNumber(), a);
                accountsContext.put(b.getAcountNumber(), b);

                trigerRoleBack_WithException_AtRandom();

                context.commitTransaction();

                //keep a count of the number of processed Transactions corectley
                transactionsProcessed++;

            }catch(Exception e){

                context.rollbackTransaction();

                //keeps track of all the Transactions that failed due to a role Back triggered
                FailedTransferRecord trace = new FailedTransferRecord(fromAccountNumber, toAccountNumber, amount);
                failed.put(trace.getId(), trace);
            }
        }

        /**
         * cause a role back by throwing an exception and count the number this thread cause
         */
        private void trigerRoleBack_WithException_AtRandom() throws Exception{
            if(random.nextInt(100)==0){
                roleBacksTriggered++;
                throw new Exception("Random Test Exception");
            }
        }
    }
}
