package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.Account;
import com.hazelcast.client.stress.helpers.FailedTransferRecord;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TransferRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
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
public class AccountTransactionGlobalLockStressTest extends StressTestSupport {

    public static boolean TEST_CASE;
    public static boolean TEST_CASE_LOCK = true;
    public static boolean TEST_CASE_TRY_LOCK = false;

    public static int TOTAL_HZ_CLIENT_INSTANCES = 3;
    public static int THREADS_PER_INSTANCE = 5;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_CLIENT_INSTANCES * THREADS_PER_INSTANCE];

    public final String ACCOUNTS_MAP = "ACCOUNTS";
    private IMap<Integer, Account> accounts;

    protected static final int MAX_ACCOUNTS = 2;
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
        for (int i = 0; i < TOTAL_HZ_CLIENT_INSTANCES; i++) {

            HazelcastInstance instance = HazelcastClient.newHazelcastClient(new ClientConfig());

            for (int j = 0; j < THREADS_PER_INSTANCE; j++) {

                StressThread t = new StressThread(instance);
                t.start();
                stressThreads[index++] = t;
            }
        }
    }

    //@Test
    public void testChangingCluster() {
        setKillThread(new KillMemberThread());
        runTest(true, stressThreads);
    }

    @Test
    public void lock_testFixedCluster() {

        //CONTROL which test case we are checking
        TEST_CASE = TEST_CASE_LOCK;
        runTest(false, stressThreads);
    }



    @Test
    //this test case randomly fails  looks like using a tryLock, some times blocks are will not return as the test some
    //times fails with stress threads could not be joined  and we see some exceptions thrown
    //ResponseAlreadySentException: NormalResponse already sent for callback:
    public void tryLock_testFixedCluster() {

        //CONTROL which test case we are checking
        TEST_CASE = TEST_CASE_TRY_LOCK;
        runTest(false, stressThreads);
    }

    public void assertResult() {

        long total=0;
        for(Account a : accounts.values()){
            total += a.getBalance();
        }

        assertEquals("concurrent transfers caused system total value gain/loss", TOTAL_VALUE, total);
    }



    public class StressThread extends TestThread {

        private HazelcastInstance instance;

        private ILock lock;
        IMap<Integer, Account> accounts = null;

        public StressThread( HazelcastInstance node ){

            this.instance = node;

            //with this "global Lock only one transfer at a time is allowed"
            lock = instance.getLock("globalLock");

            accounts = instance.getMap(ACCOUNTS_MAP);
        }

        //@Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                long amount = random.nextInt(MAX_TRANSFER_VALUE)+1;
                int from = 0;
                int to = 0;

                while ( from == to ) {
                    from = random.nextInt(MAX_ACCOUNTS);
                    to = random.nextInt(MAX_ACCOUNTS);
                }

                //WHICH TEST CASE ARE WE CHECKING
                if ( TEST_CASE == TEST_CASE_LOCK){
                    lock_Andtransfer(to, from, amount);
                }
                else{
                    trylock_Andtransfer(to, from, amount);
                }
            }
        }

        private void lock_Andtransfer(int fromAccountNumber, int toAccountNumber, long amount) throws Exception {

            lock.lock();
            try{
                transfer(fromAccountNumber, toAccountNumber, amount);

            }finally {
                lock.unlock();
            }
        }

        private void trylock_Andtransfer(int fromAccountNumber, int toAccountNumber, long amount) throws Exception {

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
