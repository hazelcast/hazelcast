package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;

/**
 * This tests verifies that atomicLong updates via getandInc are not lost.
 * a number of threads are all going for the same AtomicLong to do a get and add.
 * we verify that the actual updates to the atomic long, are the same as the total number of updates from all thread.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AtomicLongAddStressTest extends StressTestSupport {

    public static int TOTAL_HZ_INSTANCES = 5;
    public static int THREADS_PER_INSTANCE = 2;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_INSTANCES * THREADS_PER_INSTANCE];

    @Before
    public void setUp() {
        super.setUp();

        int index=0;
        for (int i = 0; i < TOTAL_HZ_INSTANCES; i++) {

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

        long total=0;
        for ( StressThread s : stressThreads ) {
            total += s.count;
        }

        IAtomicLong expeted = instances.get(0).getAtomicLong("atomicL");

        assertEquals(expeted+" has failed writes ", total, expeted.get());
    }


    public class StressThread extends TestThread{

        private HazelcastInstance instance;
        private IAtomicLong atomicLong;
        public long count=0;

        public StressThread(HazelcastInstance node){
            super();

            instance = node;
            atomicLong = instance.getAtomicLong("atomicL");

        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                int inc = random.nextInt(5);
                atomicLong.getAndAdd(inc);
                count+=inc;

            }
        }
    }

}
