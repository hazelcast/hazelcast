package com.hazelcast.client.stress;

import com.hazelcast.client.stress.helpers.Account;
import com.hazelcast.client.stress.support.StressTestSupport;
import com.hazelcast.client.stress.support.TestThread;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This test uses one global instance of a distributed HZ lock, to control transfer transactions between acounts.
 * so only one transfer can be run at a time.  we as the end check the total value of the accounts to assert we haven
 * not lost or gained value from the system
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AccountTransactionGlobalLockStressTest extends StressTestSupport<AccountTransactionGlobalLockStressTest.StressThread> {

    public static boolean TEST_CASE;
    public static boolean TEST_CASE_LOCK = true;
    public static boolean TEST_CASE_TRY_LOCK = false;

    public final String ACCOUNTS_MAP = "ACCOUNTS";
    private IMap<Integer, Account> accounts;

    protected static final int MAX_ACCOUNTS = 2;
    protected static final long INITIAL_VALUE = 100;
    protected static final long TOTAL_VALUE = INITIAL_VALUE * MAX_ACCOUNTS;
    protected static final int MAX_TRANSFER_VALUE = 100;

    @Before
    public void setUp() {
        cluster.initCluster();
        initStressThreadsWithClient(this);

        HazelcastInstance hz = stressThreads.get(0).instance;
        accounts = hz.getMap(ACCOUNTS_MAP);

        for ( int i=0; i< MAX_ACCOUNTS; i++ ) {
            Account a = new Account(i, INITIAL_VALUE);
            accounts.put(a.getAcountNumber(), a);
        }
    }

    @Test
    public void lock_testFixedCluster() {
        //CONTROL which test case we are checking
        TEST_CASE = TEST_CASE_LOCK;
        runTest(false);
    }

    /**
     * this test case randomly fails we see some exceptions thrown
     * ResponseAlreadySentException: NormalResponse already sent for callback:
     * at the end of the test one of the threads is blocked and could not be joined
     */
    @Test
    public void tryLock_testFixedCluster() {
        //CONTROL which test case we are checking
        TEST_CASE = TEST_CASE_TRY_LOCK;
        runTest(false);
    }

    @Test
    public void lock_testChangingCluster() {
        TEST_CASE = TEST_CASE_LOCK;
        runTest(true);
    }

    @Test
    public void tryLock_testChangingCluster() {
        TEST_CASE = TEST_CASE_TRY_LOCK;
        runTest(true);
    }


    public void assertResult() {

        long total=0;
        for(Account a : accounts.values()){
            total += a.getBalance();
        }
        assertEquals("concurrent transfers caused system total value gain/loss", TOTAL_VALUE, total);
    }

    public class StressThread extends TestThread {
        private ILock lock;
        IMap<Integer, Account> accounts = null;

        public StressThread( HazelcastInstance node ){
            super(node);
            //with this "global Lock only one transfer at a time is allowed"
            lock = instance.getLock("globalLock");
            accounts = instance.getMap(ACCOUNTS_MAP);
        }

        //@Override
        public void testLoop() throws Exception {

            long amount = random.nextInt(MAX_TRANSFER_VALUE)+1;
            int from = 0;
            int to = 0;

            while ( from == to ) {
                from = random.nextInt(MAX_ACCOUNTS);
                to = random.nextInt(MAX_ACCOUNTS);
            }

            //WHICH TEST CASE ARE WE CHECKING
            if ( TEST_CASE == TEST_CASE_LOCK){
                lock_AndTransfer(to, from, amount);
            }
            else{
                tryLock_AndTransfer(to, from, amount);
            }
        }

        private void lock_AndTransfer(int fromAccountNumber, int toAccountNumber, long amount) throws Exception {

            lock.lock();
            try{
                transfer(fromAccountNumber, toAccountNumber, amount);

            }finally {
                lock.unlock();
            }
        }

        private void tryLock_AndTransfer(int fromAccountNumber, int toAccountNumber, long amount) throws Exception {

            if(lock.tryLock(10, TimeUnit.MILLISECONDS)){
                try{
                    transfer(fromAccountNumber, toAccountNumber, amount);

                }finally {
                    lock.unlock();
                }
            }
        }

        private void transfer(int fromAccountNumber, int toAccountNumber, long amount){
            Account fromAccount = accounts.get(fromAccountNumber);
            Account toAccount = accounts.get(toAccountNumber);

            fromAccount.transferTo(toAccount, amount);

            accounts.put(fromAccount.getAcountNumber(), fromAccount);
            accounts.put(toAccount.getAcountNumber(), toAccount);
        }
    }
}
