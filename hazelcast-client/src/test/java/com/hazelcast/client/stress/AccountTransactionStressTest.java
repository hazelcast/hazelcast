package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.stress.helpers.Account;
import com.hazelcast.client.stress.helpers.FailedTransferRecord;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TransferRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
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
 * this test using map key locks, which allows to process transfers involving diffrent accounts concrruntly.
 * by process transfers using a common acount sequentuali.  at the end we chek if we have lost of gained value from
 * the system.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AccountTransactionStressTest extends StressTestSupport {

    private static final String ACCOUNTS_MAP = "ACOUNTS";
    private  IMap<Integer, Account> accounts;

    public static final int TOTAL_HZ_CLIENT_INSTANCES = 3;
    public static final int THREADS_PER_INSTANCE = 5;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_CLIENT_INSTANCES * THREADS_PER_INSTANCE];

    protected static final int MAX_ACCOUNTS = 200;
    protected static final long INITIAL_VALUE = 100;
    protected static final long TOTAL_VALUE = INITIAL_VALUE * MAX_ACCOUNTS;
    protected static final int MAX_TRANSFER_VALUE = 100;


    @Before
    public void setUp() {
        super.setUp();

        HazelcastInstance hz = cluster.getRandomNode();
        accounts = hz.getMap(ACCOUNTS_MAP);

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

        assertEquals("concurrent transfers caused system total value gain/loss", TOTAL_VALUE, total);
    }


    public class StressThread extends TestThread{

        private HazelcastInstance instance;
        private IMap<Integer, Account> accounts;

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

                lockAccounts_andTransfer(from, to, amount);
            }
        }

        private void lockAccounts_andTransfer(int fromAccountNumber, int toAccountNumber, long amount) throws InterruptedException {

            if ( accounts.tryLock(fromAccountNumber, 50, TimeUnit.MILLISECONDS) ) {
                try{
                    if ( accounts.tryLock(toAccountNumber, 50, TimeUnit.MILLISECONDS) ) {
                        try{

                            transfer(fromAccountNumber, toAccountNumber, amount);

                        } finally {
                            accounts.unlock(toAccountNumber);
                        }
                    }
                } finally {
                    accounts.unlock(fromAccountNumber);
                }
            }
        }

        private void transfer(int fromAccountNumber, int toAccountNumber, long amount){

            Account from = accounts.get(fromAccountNumber);
            Account to = accounts.get(toAccountNumber);

            from.transferTo(to, amount);

            accounts.put(from.getAcountNumber(), from);
            accounts.put(to.getAcountNumber(), to);
        }
    }

}
