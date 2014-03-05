package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;

/**
 * This tests verifies that atomicLong updates via getandInc are not lost.
 * a number of threads are all going for the same AtomicLong to do a get and add.
 * we verify that the actual updates to the atomic long, are the same as the total number of updates from all threads.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AtomicLongAddStressTest extends StressTestSupport {

    public static int TOTAL_HZ_CLIENT_INSTANCES = 3;
    public static int THREADS_PER_INSTANCE = 5;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_CLIENT_INSTANCES * THREADS_PER_INSTANCE];

    private String atomicKey = "atomicL";

    @Before
    public void setUp() {
        super.setUp();

        int index=0;
        for (int i = 0; i < TOTAL_HZ_CLIENT_INSTANCES; i++) {

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getNetworkConfig().setRedoOperation(true);

            HazelcastInstance instance = HazelcastClient.newHazelcastClient(clientConfig);

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

    @Test
    public void testChangingCluster() {

        setKillThread( new KillMemberOwningKeyThread(atomicKey) );
        runTest(true, stressThreads);
    }

    @Test
    public void testFixedCluster() {
        runTest(false, stressThreads);
    }


    @Override
    public void assertResult() {

        long expetedTotal=0;
        for ( StressThread s : stressThreads ) {
            expetedTotal += s.count.get();
        }

        HazelcastInstance hz = cluster.getRandomNode();
        IAtomicLong acutal = hz.getAtomicLong(atomicKey);

        assertEquals(acutal+" has value "+acutal.get(), expetedTotal, acutal.get());
    }


    public class StressThread extends TestThread{

        private HazelcastInstance instance;
        private IAtomicLong atomicLong;
        public AtomicInteger count = new AtomicInteger(0);

        public StressThread(HazelcastInstance node){
            instance = node;
            atomicLong = instance.getAtomicLong(atomicKey);
        }

        public void doRun() throws Exception {

            while ( !isStopped() ) {

                int inc = 1;
                atomicLong.getAndAdd(inc);
                count.addAndGet(inc);
            }
        }
    }

}
