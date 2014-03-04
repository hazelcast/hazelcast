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

    public static int TOTAL_HZ_CLIENT_INSTANCES = 3;
    public static int THREADS_PER_INSTANCE = 5;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_CLIENT_INSTANCES * THREADS_PER_INSTANCE];

    public static final String ACCOUNTS_MAP = "ACOUNTS";
    public static final String PROCESED_TRANS_MAP ="PROCESSED";
    public static final String FAILED_TRANS_MAP ="FAIL";

    protected static final int MAX_ACCOUNTS = 2;
    protected static final long INITIAL_VALUE = 100;
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
        for (int i = 0; i < TOTAL_HZ_CLIENT_INSTANCES; i++) {

            HazelcastInstance instance = HazelcastClient.newHazelcastClient(new ClientConfig());

            for (int j = 0; j < THREADS_PER_INSTANCE; j++) {

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
        setKillThread(new KillMemberThread());
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

        System.out.println( "==>> procesed tnx "+processed.size()+" failed tnx "+failed.size()+" total value = "+total);

        assertEquals("concurrent transfers caused system total value gain/loss", TOTAL_VALUE, total);
    }



    public class StressThread extends TestThread {

        private HazelcastInstance instance;
        private ILock lock;
        IMap<Integer, Account> accounts = null;
        IMap<Long, TransferRecord> transactions = null;
        IMap failed = null;

        public StressThread( HazelcastInstance node ){

            this.instance = node;

            //with this "global Lock only one transfer at a time is allowed"
            lock = instance.getLock("L");

            accounts = instance.getMap(ACCOUNTS_MAP);
            transactions = instance.getMap(PROCESED_TRANS_MAP);
            failed = instance.getMap(FAILED_TRANS_MAP);
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

                transfer(to, from, amount);
            }
        }

        private void transfer(int fromAccountNumber, int toAccountNumber, long amount) throws Exception {


            if(lock.tryLock(10, TimeUnit.MILLISECONDS)){
                try{
                    Account fromAccount = accounts.get(fromAccountNumber);
                    Account toAccount = accounts.get(toAccountNumber);

                    TransferRecord trace = fromAccount.transferTo(toAccount, amount);
                    transactions.put(trace.getId(), trace);

                    accounts.put(fromAccount.getAcountNumber(), fromAccount);
                    accounts.put(toAccount.getAcountNumber(), toAccount);

                }finally {
                    lock.unlock();
                }
            } else {

                FailedTransferRecord trace = new FailedTransferRecord(fromAccountNumber, toAccountNumber, amount);
                trace.setReason("Time Out");
                failed.put(trace.getId(), trace);
            }
        }

    }

}
